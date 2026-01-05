// ============ mt4-receiver-optimized.js ============
const WebSocket = require('ws');
const http = require('http');
require('dotenv').config();
const { getTimeGMT7 } = require('../Helpers/time');

// ✅ NEW: Shared Memory thay Redis cho price/ohlc

// Giữ Redis cho các operations khác (không liên quan price/ohlc)
const Redis = require('../Redis/clientRedis');
const RequestDeduplicator = require('../Redis/RequestDeduplicator');
const SymbolDebounceQueue = require('../Redis/DebounceQueue');
const deduplicator = new RequestDeduplicator(Redis.client);
const { startTradeQueue, addTradeJob } = require('../Queue/trade.queue');
const RedisH = require('../Redis/redis.helper');
const {deleteBrokerAndRelatedIndex , saveOHLC_SNAP_ArrayOnly , setBrokerStatus } = require('../Redis/redis.helper2');
const PriceFlush = require("../Redis/priceBuffer.redisFlush");
const {updateBrokerMetaFromRaw} = require("../Redis/redis.broker.meta");

const { getAllBrokers , getBrokerMeta,getPrice ,getAllPricesByBroker ,getSymbolOfMinIndexBroker  , getBestSymbolByIndex} = require("../Redis/redis.price.query");

// ✅ Track session per broker_ (fix reconnect / tud gate)
const brokerSession = new Map(); 
// broker_ -> { wsId, lastSeen, lastTud, indexBroker }

const BROKER_TIMEOUT_MS = 12_000;      // 12s không thấy PRICE_SNAP/OHLC_SNAP => coi là mất kết nối
const TUD_RESET_ALLOW_GAP = 60_000;    // nếu tud tụt mạnh => coi là restart => cho phép nhận lại

function shouldAcceptTud(broker_, tud) {
  const b = String(broker_ || '').toLowerCase();
  const cur = Number(tud);
  if (!Number.isFinite(cur)) return true;

  const sess = brokerSession.get(b);
  if (!sess) return true;

  const prev = Number(sess.lastTud);
  if (!Number.isFinite(prev)) return true;

  // bình thường: tud tăng
  if (cur > prev) return true;

  // tud giảm => MT4 restart/reconnect
  if ((prev - cur) > TUD_RESET_ALLOW_GAP) {
    sess.lastTud = cur; // reset
    return true;
  }

  // duplicate/out-of-order => drop
  return false;
}

function touchBrokerSession(broker_, ws, indexBrokerMaybe) {
  const b = String(broker_ || '').toLowerCase();
  const sess = brokerSession.get(b) || { wsId: ws?.id, lastSeen: 0, lastTud: -1, indexBroker: '' };
  sess.wsId = ws?.id || sess.wsId;
  sess.lastSeen = Date.now();
  if (indexBrokerMaybe != null && indexBrokerMaybe !== '') sess.indexBroker = String(indexBrokerMaybe);
  brokerSession.set(b, sess);
  return sess;
}

// Constants
let Time_Send;
const requestCounts = new Map();
const MAX_REQUESTS = 10;
const TIME_WINDOW = 1000;
var { broker_Actived, symbolSetting } = require('../../../models/index');
let VersionCurrent = process.env.VERSION || 1.0;

// Colors
var Color_Log_Success = "\x1b[32m%s\x1b[0m";
var Color_Log_Error = "\x1b[31m%s\x1b[0m";
const Client_Connected = new Map();

const { log, colors } = require('../Helpers/Log');
const { formatString, normSym, calculatePercentage } = require('../Helpers/text.format');

// ✅ Stats tracking
// const stats = {
//     priceSnapCount: 0,
//     ohlcSnapCount: 0,
//     totalSymbols: 0,
//     avgWriteTime: 0,
//     totalMessages: 0,
//     startTime: Date.now()
// };

// ✅ Stats tracking
const stats = {
    priceSnapCount: 0,
    ohlcSnapCount: 0,
    totalSymbols: 0,
    totalMessages: 0,
    startTime: Date.now(),
    lastLogTime: 0  // ← THÊM ĐÂY
};

// Debounce queue (giữ lại nếu cần)
const queue = new SymbolDebounceQueue({ 
    debounceTime: 3000,
    maxWaitTime: 10000,
    maxPayloads: 5000,
    delayBetweenTasks: 100,
    cooldownTime: 5000
});

// Redis init (chỉ dùng cho META, ORDER, SYNC_PRICE)
RedisH.initRedis({
    host: '127.0.0.1',
    port: 6379,
    db: 0,
    compress: true
});

async function onBrokerStatusChange(brokerName, statusString) {
    const percentage = calculatePercentage(statusString);
    await Redis.updateResetProgress(brokerName, percentage);
}

function setupWebSocketServer(port) {
    SaveAll_Info();
    PriceFlush.startRedisFlushInterval(30);

    
    // ✅ Giữ lại Redis subscriptions cho commands
    Redis.subscribe(String(port), async (channel, message) => {
        const Broker = channel.Broker;
        if(channel.Type === "Test_price") {
            for (const [id, element] of Client_Connected.entries()) {
                if(element.Broker == Broker) {
                    if (element.ws.readyState === WebSocket.OPEN) {
                        const Mess = JSON.stringify({type: "Test_price", Success: 1, message: channel.Symbol , data: channel.Points});
                        console.log(Color_Log_Success, "Received Test_price message:", Mess , " - Send to Broker:", Broker);
                        element.ws.send(Mess);
                    }
                }
            } 
        } else {
            for (const [id, element] of Client_Connected.entries()) {
                if(element.Broker == Broker) {
                    if (element.ws.readyState === WebSocket.OPEN) {
                        if(channel.Symbol === "all") {
                            const Mess = JSON.stringify({type: "Reset_All", Success: 1});
                            element.ws.send(Mess);
                        } else if(channel.type === "destroy_broker") {
                            const Mess = JSON.stringify({type: "Destroy_Broker", Success: 1, message: channel.Symbol});
                            element.ws.send(Mess);
                        } else {
                            const Mess = JSON.stringify({type: "Reset_Only", Success: 1, message: channel.Symbol});
                            element.ws.send(Mess);
                        }
                    }
                }
            }
        }
    });

    Redis.subscribe("RESET_ALL", async (channel, message) => {
        const Broker = channel.Broker;
        if(channel.Type === "Test_Time_Open") {
            console.log(Color_Log_Success, "Received Test_Time_Open message");
            for (const [id, element] of Client_Connected.entries()) {
                if (element.ws.readyState === WebSocket.OPEN) {
                    const Mess = JSON.stringify({type: "Test_Time_Open", Success: 1});
                    element.ws.send(Mess);
                }
            }
        } else {
            for (const [id, element] of Client_Connected.entries()) {
                if (element.ws.readyState === WebSocket.OPEN) {
                    if(element.Broker == Broker) {
                        if(channel.Symbol === "ALL-BROKERS") {
                            console.log(Color_Log_Success, `Publish to Broker: ${Broker}`);
                            const Mess = JSON.stringify({type: "Reset_All", Success: 1});
                            element.ws.send(Mess);
                        } else {
                            console.log(Color_Log_Success, `Publish to Symbol: ${channel.Symbol}`);
                            const Mess = JSON.stringify({type: "Reset_Only", Success: 1, message: channel.Symbol});
                            element.ws.send(Mess);
                        }
                    } else if(Broker === "ALL-BROKERS") {
                        const Mess = JSON.stringify({type: "Reset_Only", Success: 1, message: channel.Symbol});
                        element.ws.send(Mess);
                    } else if(Broker === "ALL-BROKERS-SYMBOL") {
                        const Mess = JSON.stringify({type: "Reset_Only_Auto", Success: 1, message: channel.Symbol});
                        element.ws.send(Mess);
                    }
                }
            }
        }
    });

    Redis.subscribe("ORDER", async (channel, message) => {
        const Broker = channel.Broker;
        for (const [id, element] of Client_Connected.entries()) {
            if (element.ws.readyState === WebSocket.OPEN) {
                if(element.Broker == Broker && element.Key_SECRET == channel.Key_SECRET) {
                    console.log(Color_Log_Success, `Order Send: ${Broker} - ${channel.Type_Order} - ${channel.Symbol} - ${channel.Key_SECRET}`);
                    
                    const Mess = JSON.stringify({type: "ORDER", Success: 1, message: channel.Symbol, data: `${channel.Key_SECRET}-${channel.Type_Order}-${channel.Price_Bid}`});
                    console.log (Mess);
                    element.ws.send(Mess);
                }
            }
        }
    });

    // HTTP server
    const server = http.createServer((req, res) => {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('WebSocket server đang chạy\n');
    });

    // WebSocket server với optimizations
    const wss = new WebSocket.Server({ 
        server: server,
        maxPayload: 50 * 1024 * 1024, // 50MB (đủ cho 300 symbols)
        perMessageDeflate: false, // ✅ Tắt compression để giảm CPU
        handshakeTimeout: 10000,
    });

    setInterval(async () => {
  const now = Date.now();
  for (const [broker_, sess] of brokerSession.entries()) {
    if (!sess?.lastSeen) continue;
    if ((now - sess.lastSeen) <= BROKER_TIMEOUT_MS) continue;

    console.log(Color_Log_Error, `[TIMEOUT] broker=${broker_} -> cleanup broker+index`);

    brokerSession.delete(broker_);

    // ✅ xoá sạch broker + index liên quan (đúng đề xuất)
    await deleteBrokerAndRelatedIndex(broker_).catch(() => {});

    // (optional) cập nhật status
    await setBrokerStatus(broker_, "Disconnect").catch(() => {});
  }
}, 2000).unref?.();

    wss.on('error', function(error) {
        console.log(Color_Log_Error, "WebSocket Server Error:", error);
    });

    wss.on('close', function close() {
        clearInterval(interval);
    });

    try {   
        wss.on('connection', async function connection(ws, req) {


            // ✅ Critical TCP optimization
            if (ws._socket) {
                ws._socket.setNoDelay(true); // Disable Nagle's algorithm
            }

            ws.isAlive = true;
            ws.id = Math.random().toString(36).substring(2, 15);
            
            try {
                var BrokerName = req.rawHeaders[13];
                var formattedBrokerName = formatString(BrokerName);
                var message = `New connection on port ${port} - ${BrokerName}`;
                var Version = req.rawHeaders[12].split("-")[0].trim();
                var Index_Broker = req.rawHeaders[12].split("-")[1].trim();
                var Key_SECRET = req.rawHeaders[12]?.split("-")[2]?.trim() || "No Key";
                let VerNum = parseFloat(Version);
                
                if(VerNum < VersionCurrent) {
                    log(colors.red, `FX_CLIENT - ${port}`, colors.magenta, `${req.rawHeaders[13]} - Version is not correct`);
                    if (ws.readyState === WebSocket.OPEN) {
                        ws.send(JSON.stringify({type: String(process.env.CHECK_FIRT), Success: 0, message: `Please Update New Version, Version Current: ${VersionCurrent}`, Data: ""}));
                    }
                } else { 
                    if (ws.readyState === WebSocket.OPEN) {
                        const brokerData = await RedisH.findBrokerByIndex(Index_Broker);
                        if(brokerData == null || brokerData.broker_ === formattedBrokerName) {
                            const Broker_Check = await RedisH.getBrokerMeta(formattedBrokerName);
                            if(Broker_Check == null || Broker_Check.index === Index_Broker) {
                                log(colors.green, `FX_CLIENT - ${port}`, colors.green, message);
                                ws.send(JSON.stringify({type: String(process.env.CHECK_FIRT), Success: 1, message: `Version = ${Version}, Index = ${Index_Broker}, Broker = ${BrokerName}, Key_SECRET = ${Key_SECRET} => Success`, Data: getTimeGMT7('datetime')}));
                                Client_Connected.set(ws.id, { ws, Broker: formattedBrokerName, Key_SECRET });

                                // ✅ chỉ cleanup khi đã accept connection
                                await deleteBrokerAndRelatedIndex(formattedBrokerName).catch(() => {});
                                await RedisH.deleteBroker(formattedBrokerName).catch(() => {});

                                // ✅ init broker session (reset tud)
                                brokerSession.set(formattedBrokerName, {
                                wsId: ws.id,
                                lastSeen: Date.now(),
                                lastTud: -1,
                                indexBroker: String(Index_Broker || '')
                                });

                                ws.__broker_ = formattedBrokerName;
                                ws.__indexBroker = String(Index_Broker || '');
                                ws.tud = -1; // (không dùng nữa nhưng giữ cho khỏi undefined)

                            } else {
                                log(colors.red, `FX_CLIENT - ${port}`, colors.magenta, message);
                                ws.send(JSON.stringify({type: String(process.env.CHECK_FIRT), Success: 0, message: `Version = ${Version}, Index = ${Index_Broker} => Success, Broker = ${BrokerName} => Fail`, Data: getTimeGMT7('datetime')}));
                            }
                        } else {
                            log(colors.red, `FX_CLIENT - ${port}`, colors.magenta, `${BrokerName} - Index: ${Index_Broker} is already connected by ${brokerData.Broker}`);
                            ws.send(JSON.stringify({type: String(process.env.CHECK_FIRT), Success: 0, message: `Index: ${Index_Broker} is already connected by ${brokerData.Broker}`, Data: getTimeGMT7('datetime')}));
                        }
                    }
                }
                
            } catch (error) {
                log(colors.red, `FX_CLIENT - ${port}`, colors.reset, "Error during connection setup:", error);
            }

            ws.on('error', function(error) {
                console.log(Color_Log_Error, `WebSocket Connection Error [${ws.id}]:`, error);
                const client = Client_Connected.get(ws.id);
                if (client) {
                    console.log(Color_Log_Error, "Client error, removing:", client.Broker);
                    Client_Connected.delete(ws.id);
                }
            });
            
            // ✅ OPTIMIZED MESSAGE HANDLER
            ws.on('message', async function incoming(message) {
                try {
                    const data = JSON.parse(message)[0];
                    
                    switch (data.Type) {    
                        case process.env.SYNC_PRICE:
                            // ✅ Giữ nguyên logic SYNC_PRICE (dùng Redis)
                            const clientId = ws.id;
                            try {
                                const Symbol = data.data.symbol;
                                const Broker = data.data.broker;
                                const Index = data.data.index;
                                const reset_text = data.data.Payload.mess;
                                const Index_Broker  = data.data.Payload.mess_;
                                
                                await setBrokerStatus(formatString(Broker), reset_text);
                                await onBrokerStatusChange(formatString(Broker), reset_text);
                                
                                const Response = await getSymbolOfMinIndexBroker(Symbol);
                                // console.log(Color_Log_Success, "SYNC_PRICE - Response:", Response,Response.index ,Index_Broker);
                                let responseData;
                                let logColor;
                                if (Response && Response.index < Index_Broker) {
                                    //  console.log(Color_Log_Success, "SYNC_PRICE - Response:", Response.broker ," - ", Index_Broker);
                                    responseData = {
                                        Symbol: Response.symbol,
                                        Broker: Response.broker,
                                        Bid: Response.bid,
                                        Digit: Response.digit,
                                        Time: Response.timecurrent,
                                        Index: Index,
                                        Type: data.Type
                                    };
                                    logColor = colors.green;
                                } else {
                                    responseData = {
                                        Symbol: Symbol,
                                        Broker: Broker,
                                        Bid: 'null',
                                        Digit: 'null',
                                        Time: 'null',
                                        Index: Index,
                                        Type: data.Type
                                    };
                                    logColor = colors.yellow;
                                }
                                
                                log(
                                    logColor,
                                    `${process.env.SYNC_PRICE}`,
                                    colors.reset,
                                    `Broker ${Broker} -> Symbol: ${Symbol} <=> Broker Check: ${responseData.Broker}, Processed: ${reset_text}`
                                );
                                
                                ws.send(JSON.stringify(responseData));
                            } catch (error) {
                                console.log(Color_Log_Error, "Error -> CheckPrice:", error);
                                if (ws.readyState === WebSocket.OPEN) {
                                    try {
                                        ws.send(JSON.stringify({
                                            type: "CheckPrice",
                                            error: true,
                                            message: "Server error processing request",
                                            index_Checked: data[1]?.Index
                                        }));
                                    } catch (sendError) {
                                        console.log(Color_Log_Error, "Error sending error response:", sendError);
                                    }
                                }
                            }
                            break;
                        // ✅ NEW: PRICE_SNAP -> Shared Memory (NO REDIS)
                        case "PRICE_SNAP": {
                            try {
                                const rawData = data.data;
                                if (!rawData) return;

                                const broker_ = formatString(rawData.broker_ || rawData.broker);
                                const idx = rawData.indexBroker ?? rawData.index ?? ws.__indexBroker ?? "";

                                // ✅ heartbeat for timeout + bind session to current ws
                                const sess = touchBrokerSession(broker_, ws, idx);

                                // ✅ tud gate theo broker session (NOT per ws)
                                // if (!shouldAcceptTud(broker_, rawData.tud)) return;

                                // update lastTud
                                const curTud = Number(rawData.tud);
                                if (Number.isFinite(curTud)) sess.lastTud = curTud;

                                PriceFlush.updatePriceBufferFromMT4(rawData);

                                updateBrokerMetaFromRaw(rawData).catch(err =>
                                console.error("Error updating broker meta:", err.message)
                                );

                            } catch (err) {
                                console.error("Error in PRICE_SNAP handler:", err.message);
                            }
                            break;
                            }
                       case "OHLC_SNAP": {
                            try {
                                const rawData = data.data || {};
                                const broker_ = formatString(rawData.broker_ || rawData.broker);
                                const idx = rawData.indexBroker ?? rawData.index ?? ws.__indexBroker ?? "";

                                touchBrokerSession(broker_, ws, idx);

                                await saveOHLC_SNAP_ArrayOnly(data);
                            } catch (err) {
                                console.error("Error saving OHLC_SNAP:", err.message);
                            }
                            break;
                            }


                        case "Ping": {
                            ws.send(JSON.stringify({
                                type: "Ping", 
                                Success: 1, 
                                message: "", 
                                Data: ""
                            }));
                            break;
                        }

                        case "ORDER_SEND":
                            try {
                                const orderData = data?.data || {};
                                const isSuccess = String(orderData.succes).toLowerCase() === "true";
                                
                                const trade = {
                                    Broker: String(orderData.broker || "").toUpperCase(),
                                    Type: String(orderData.cmd || "").toUpperCase(),
                                    Symbol: String(orderData.symbol || "").toUpperCase(),
                                    Ticket: Number(orderData.ticket || 0),
                                    OpenPrice: Number(orderData.open_price || 0),
                                    TimeOpen: String(orderData.time_open || ""),
                                    Volume: Number(orderData.volume || 0),
                                    PriceSend: Number(orderData.price_order || 0),
                                    Comment: orderData.comment ? String(orderData.comment) : "",
                                    Spread: orderData.spread ? String(orderData.spread) : "",
                                    Status: isSuccess ? "SUCCESS" : "FAILED",
                                };
                                
                                let mes = "";
                                let price_check = 0;
                                
                                if(trade.Type === "BUY" && isSuccess) {
                                    mes = `BUY Thành Công ✅`;
                                    price_check = Number(trade.OpenPrice) - (Number(orderData.spread_digit) || 0);
                                    if(price_check > Number(trade.PriceSend)) {
                                        mes = `BUY trượt giá ❌`;
                                    }
                                } else if(trade.Type === "SELL" && isSuccess) {
                                    mes = `SELL Thành Công ✅`;
                                    price_check = Number(trade.OpenPrice) + (Number(orderData.spread_digit) || 0);
                                    if(price_check < Number(trade.PriceSend)) {
                                        mes = `SELL trượt giá ❌`;
                                    }
                                } else {
                                    mes = `Fail`;
                                }
                                
                                if(trade.Ticket === -1) mes = "Vào Lệnh Không Thành Công ❌";
                                trade.Message = mes;

                                addTradeJob(trade);
                                
                                if (isSuccess) {
                                    log(
                                        colors.green,
                                        "ORDER",
                                        colors.reset,
                                        `${trade.Broker} ${trade.Type} ${trade.Symbol} SUCCESS -> Ticket: ${trade.Ticket}`
                                    );
                                } else {
                                    log(
                                        colors.red,
                                        "ORDER FAIL",
                                        colors.reset,
                                        `Broker: ${orderData.broker} ${orderData.cmd} ${orderData.symbol} - Message: ${orderData.comment}`
                                    );
                                }

                            } catch (error) {
                                console.error("Error in ORDER handler:", error.message);
                            }
                            break;

                        default:
                            break;
                    }
                    
                } catch (error) {
                    console.log(Color_Log_Error, "Error processing message:", error);
                    if (ws.readyState === WebSocket.OPEN) {
                        try {
                            ws.send(JSON.stringify({
                                type: "error",
                                message: "Invalid message format or processing error"
                            }));
                        } catch (sendError) {
                            // Silent fail
                        }
                    }
                }
            });

            ws.on('close', async function close() {
  const client = Client_Connected.get(ws.id);
  if (client) {
    const broker_ = client.Broker;

    log(colors.red, `FX_CLIENT - ${port}`, colors.reset, `Client disconnected: ${broker_.toUpperCase()}`);

    brokerSession.delete(broker_);

    await deleteBrokerAndRelatedIndex(broker_).catch(err =>
      log(colors.red, `FX_CLIENT - ${port}`, colors.reset, "Error deleting broker:", err)
    );

    Client_Connected.delete(ws.id);
  }
});

        });

        server.listen(port, () => {
            log(colors.green, 'FOREX WS', colors.reset, `WebSocket server đang chạy tại port ${port}`);
            log(colors.cyan, 'SHARED MEMORY', colors.reset, 'Initialized for PRICE_SNAP and OHLC_SNAP');
        });
        
        return wss;
    } catch (error) {
        console.log(Color_Log_Error, "Error setting up WebSocket server:", error);
        throw error;
    }
}

function SaveAll_Info() {
   
    // setInterval(async () => {
    //     try {
    //         const getData = await broker_Actived.find({});
    //         brokersActived = getData;
    //         info_symbol_config = await symbolSetting.find({});
    //     } catch (error) {
    //         console.error("Lỗi khi lấy thông tin brokers:", error);
    //     }
    // }, 1000);

  
}




module.exports = setupWebSocketServer;