const WebSocket = require('ws');
const http = require('http');
require('dotenv').config();
const { getTimeGMT7 } = require('../Helpers/time');

// Import các modules hiện tại
const Redis = require('../Redis/clientRedis');
const RequestDeduplicator = require('../Redis/RequestDeduplicator');
const DebounceQueue  = require('../Redis/DebounceQueue');
const deduplicator = new RequestDeduplicator(Redis.client);
// const getData = require('../Helpers/read_Data');
// const Data = require('../Helpers/get_data');
// const e = require('express');
// const priceBatcher = require('./Connect/bactching');
// const { time } = require('console');
const requestCounts = new Map(); // Lưu số lượng request theo client ID
const MAX_REQUESTS = 10; // Số request tối đa cho mỗi client
const TIME_WINDOW = 1000;
var {broker_Actived , symbolSetting} = require('../../../models/index');
let VersionCurrent = process.env.VERSION || 1.0;
// Màu cho console.log
var Color_Log_Success = "\x1b[32m%s\x1b[0m";
var Color_Log_Error = "\x1b[31m%s\x1b[0m";
const Client_Connected = new Map();

const {log , colors} = require('../Helpers/Log');
const {formatString , normSym} = require('../Helpers/text.format');
// let brokersActived = [];
// let info_symbol_config = [];
// let WS_Broker;

// Hàm này sẽ tạo một WebSocket Server ở port được truyền vào
const debounceQueue = new DebounceQueue({ 
    debounceTime: 2000  // 2 giây
});

function setupWebSocketServer(port) {
    SaveAll_Info();
    Redis.subscribe(String(port), async (channel, message) => {
        const Broker = channel.Broker
        for (const [id, element] of Client_Connected.entries()) {
            if(element.Broker == Broker) {
                if (element.ws.readyState === WebSocket.OPEN) {
                    if(channel.Symbol === "all") {
                        const Mess = JSON.stringify({type : "Reset_All", Success: 1 });
                        element.ws.send(Mess);
                    }else if(channel.type === "destroy_broker"){
                        const Mess = JSON.stringify({type : "Destroy_Broker", Success: 1 , message: channel.Symbol});
                        element.ws.send(Mess);
                    }else{
                        const Mess = JSON.stringify({type : "Reset_Only", Success: 1 , message: channel.Symbol});
                        element.ws.send(Mess);
                    }
                }
            }
        };
    });

    Redis.subscribe("RESET_ALL", async (channel, message) => {
        const Broker = channel.Broker
        for (const [id, element] of Client_Connected.entries()) {
                if (element.ws.readyState === WebSocket.OPEN) {
                    if(element.Broker == Broker){
                        if(channel.Symbol === "ALL-BROKERS") {
                        console.log(Color_Log_Success, `Publish to Broker: ${Broker}`);
                        const Mess = JSON.stringify({type : "Reset_All", Success: 1 });
                        element.ws.send(Mess);
                    }else{
                        console.log(Color_Log_Success, `Publish to Symbol: ${channel.Symbol}`);
                        const Mess = JSON.stringify({type : "Reset_Only", Success: 1 , message: channel.Symbol});
                        element.ws.send(Mess);
                    }
                    }else if(Broker === "ALL-BROKERS"){
                        const Mess = JSON.stringify({type : "Reset_Only", Success: 1 , message: channel.Symbol});
                        element.ws.send(Mess);
                    }else if(Broker === "ALL-BROKERS-SYMBOL"){
                        const Mess = JSON.stringify({type : "Reset_Only_Auto", Success: 1 , message: channel.Symbol});
                        element.ws.send(Mess);
                    }
                }
        };
    });

    Redis.subscribe("ORDER", async (channel, message) => {
        const Broker = channel.Broker
        
        for (const [id, element] of Client_Connected.entries()) {
                if (element.ws.readyState === WebSocket.OPEN) {
                    // console.log("ORDER CHANNEL: ", element  );
                    if(element.Broker == Broker && element.Key_SECRET == channel.Key_SECRET){
                        console.log(Color_Log_Success, `Order Send: ${Broker} - ${channel.Type_Order} - ${channel.Symbol} - ${channel.Key_SECRET}`);
                        const Mess = JSON.stringify({type : "ORDER", Success: 1 ,message: channel.Symbol,data: `${channel.Key_SECRET}-${channel.Type_Order}-${channel.Price_Bid}`});
                        element.ws.send(Mess);
                }
            }
        };
    });
    // Tạo HTTP server trước
    const server = http.createServer((req, res) => {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('WebSocket server đang chạy\n');
    });

    // Tạo WebSocket server với options để xử lý các lỗi phổ biến
    const wss = new WebSocket.Server({ 
        server: server,
        // Tăng kích thước tối đa của payload
        maxPayload: 1000 * 1024 * 1024, // 1000MB
        // Tắt nén để giảm CPU overhead
        perMessageDeflate: false,
        // Timeout cho handshake
        handshakeTimeout: 10000,
    });
    // WS_Broker = wss;

    // Xử lý lỗi ở cấp độ server
    wss.on('error', function(error) {
        console.log(Color_Log_Error, "WebSocket Server Error:", error);
        // Không đóng server nếu có lỗi riêng lẻ
    });

    // Thiết lập heartbeat để phát hiện kết nối đã ngắt
    function heartbeat() {
        this.isAlive = true;
    }

    const interval = setInterval(function ping() {
        wss.clients.forEach(function each(ws) {
            if (ws.isAlive === false) {
                console.log(Color_Log_Error, "Client không phản hồi, ngắt kết nối");
                return ws.terminate();
            }
            
            ws.isAlive = false;
            ws.ping(function noop() {});
        });
    }, 120000);

    wss.on('close', function close() {
        clearInterval(interval);
    });

    try {   
        wss.on('connection', async function connection(ws, req) {
            
        //    SaveAll_Info();
            // console.log(brokersCache);
            // Thiết lập heartbeat cho kết nối mới
            ws.isAlive = true;
            ws.on('pong', heartbeat);
            
            // Gán ID duy nhất cho kết nối
            ws.id = Math.random().toString(36).substring(2, 15);
            
            try {
                // ===== LOGIC KẾT NỐI GỐC CỦA BẠN BẮT ĐẦU TỪ ĐÂY =====
                
                // Lấy thông tin client MT4-5
                var BrokerName = req.rawHeaders[13];
                var formattedBrokerName = formatString(BrokerName);
                var message = `New connection on port ${port} -  ${BrokerName}`;
                var Version = req.rawHeaders[12].split("-")[0].trim();
                var Index_Broker = req.rawHeaders[12].split("-")[1].trim();
                var Key_SECRET = req.rawHeaders[12]?.split("-")[2].trim()||"No Key";

                console.log(`Key_SECRET: ${Key_SECRET}`);
                let VerNum = parseFloat(Version);
                await Redis.deleteBroker(formattedBrokerName).catch(err => log(colors.red, `FX_CLIENT - ${port} `, colors.reset, "Error deleting broker:", err));
                
              
                // Kiểm tra version
                if(VerNum < VersionCurrent) {
                    log(colors.red, `FX_CLIENT - ${port} `, colors.magenta, `${req.rawHeaders[13]} - Version is not correct`);
                    
                    if (ws.readyState === WebSocket.OPEN) {
                        ws.send(JSON.stringify({type : String(process.env.CHECK_FIRT), Success: 0 , message: `Please Update New Version , Version Current: ${VersionCurrent}` , Data: ""}));
                    }
                } else { 
                    if (ws.readyState === WebSocket.OPEN) {
                        const brokerData = await Redis.findBrokerByIndex(Index_Broker);
                        if(brokerData == null) {
                            const Broker_Check = await Redis.getBroker(formattedBrokerName);
                            if(Broker_Check == null) {
                                log(colors.green, `FX_CLIENT - ${port} `, colors.green, message);
                                ws.send(JSON.stringify({type : String(process.env.CHECK_FIRT), Success: 1 , message: `Version = ${Version} , Index = ${Index_Broker} , Broker = ${BrokerName} , Key_SECRET = ${Key_SECRET} => Success`, Data: getTimeGMT7('datetime')}));
                                // Lưu client đã connect
                                Client_Connected.set(ws.id, {ws, Broker: formattedBrokerName ,Key_SECRET});
                                await Redis.deleteBroker(formattedBrokerName);
                            }else{
                                log(colors.red, `FX_CLIENT - ${port} `, colors.magenta, message);
                                ws.send(JSON.stringify({type : String(process.env.CHECK_FIRT), Success: 0 , message: `Version = ${Version} , Index = ${Index_Broker} => Success , Broker = ${BrokerName} => Fail`, Data: getTimeGMT7('datetime')}));
                            }
                        }else{
                            log(colors.red, `FX_CLIENT - ${port} `, colors.magenta, `${BrokerName} - Index: ${Index_Broker} is already connected by ${brokerData.Broker}`);
                            ws.send(JSON.stringify({type : String(process.env.CHECK_FIRT), Success: 0 , message: `Index: ${Index_Broker} is already connected by ${brokerData.Broker}` , Data: getTimeGMT7('datetime')}));
                        }
                    }
                }
                // ===== LOGIC KẾT NỐI GỐC CỦA BẠN KẾT THÚC Ở ĐÂY =====
                
            } catch (error) {
                log(colors.red, `FX_CLIENT - ${port} `, colors.reset, "Error during connection setup:", error);
                
            }
            ws.on('error', function(error) {
                console.log(Color_Log_Error, `WebSocket Connection Error [${ws.id}]:`, error);
                const client = Client_Connected.get(ws.id);
                if (client) {
                    console.log(Color_Log_Error, "Client error, removing:", client.Broker);
                    Client_Connected.delete(ws.id);
                }
            });
            
            // Xử lý khi có message từ client
            ws.on('message', async function incoming(message) {
                
                let Check_Index = 0;
                let Reset = false;
                
                try {
                    const data = JSON.parse(message)[0];
                    
                    // Kiểm tra tính hợp lệ của dữ liệu
                    // if (!Array.isArray(data) || !data[0] || !data[0].Type) {
                    //     throw new Error('Invalid message format' ,data);
                    // } 
                    
                    // ===== LOGIC XỬ LÝ MESSAGE GỐC CỦA BẠN BẮT ĐẦU TỪ ĐÂY =====
                    // if(data[1].Broker !== undefined || data[1].Broker !== null || data[1].Broker !== "") {
                    // console.log(Color_Log_Success, "Message from client:", data[1].Broker);
                    // }
                    switch (data.Type) {    

                        case process.env.SYNC_PRICE:
                            const clientId = ws.id; // Sử dụng ID của client
                            try {
                                const Symbol = data.data.symbol;
                                const Broker = data.data.broker;
                                const Index = data.data.index;
                                const reset_text = data.data.Payload.mess;
                                await Redis.updateBrokerStatus(formatString(Broker), reset_text); 
                                const Response = await Redis.getSymbol(Symbol);
                                // console.log("Response:", Response);
                                let responseData;
                                let logColor;
                                if (Response) {
                                    responseData = {
                                    Symbol: Response.symbol,
                                    Broker: Response.Broker,
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
                                // console.log(Color_Log_Success, "CheckPrice Request from Broker:", responseData);
                                log(
                                    logColor,
                                    `${process.env.SYNC_PRICE}`,
                                    colors.reset,
                                    `Broker ${Broker} -> Symbol: ${Symbol} <=> Broker Check: ${responseData.Broker} , Processed: ${reset_text}`
                                );

                                
                                ws.send(JSON.stringify(responseData));
                            } catch (error) {
                                console.log(Color_Log_Error, "Error -> CheckPrice: ", error);
                                
                                // Vẫn gửi phản hồi lỗi nếu WebSocket còn mở
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
                       
                        case "SET_DATA":
                            try {
                                const rawData = data.data;
                                if (!rawData.broker || !rawData.index) {
                                    throw new Error('Invalid broker data structure');
                                }
                                const save = await Redis.saveBrokerData(formatString(rawData.broker) , rawData);
                            } catch (error) {
                                console.error('Error saving broker data:', error.message);
                            }
                            break;
                        case "RESET_SYMBOL":
                            try {
                                const rawData = data.data;
                                if (!rawData.symbol && !rawData.port) {
                                    throw new Error('Invalid symbol data structure');
                                }
                                
                                 const symbol = rawData.symbol;
                                    const dedupKey = `RESET:${symbol}`;

                                    debounceQueue.receive(dedupKey, rawData, async (data, meta) => {
                                        // Callback này chạy SAU 3s debounce VÀ đợi queue
                                        console.log(`Processing ${meta.key} after ${meta.count} messages`);
                                        
                                        await Redis.publish("RESET_ALL", JSON.stringify({
                                            Symbol: symbol,
                                            Broker: "ALL-BROKERS-SYMBOL",
                                        }));
                                        
                                        // Simulate processing time
                                        await new Promise(r => setTimeout(r, 2000));
                                    });
                                
                            } catch (error) {
                                console.error('Error in RESET_SYMBOL:', error.message);
                            }
                            break;
                            
                        // case "isConnect":
                        //     if (ws.readyState === WebSocket.OPEN) {
                        //         ws.send(JSON.stringify({type: "isConnect", Success: 1, message: "Connected", Data: ""}));
                        //     }
                        //     break;
                        case "ping":
                            if (ws.readyState === WebSocket.OPEN) {
                                // console.log(Color_Log_Success, "Ping from client:", data[1]);
                                ws.send(JSON.stringify({type: "pong", Success: 1, message: "", Data: ""}));
                            }
                            break;
                            
                        default:
                            // No-op for unrecognized message types
                            break;
                    }
                    
                    // ===== LOGIC XỬ LÝ MESSAGE GỐC CỦA BẠN KẾT THÚC Ở ĐÂY =====
                    
                } catch (error) {
                    console.log(Color_Log_Error, "Error processing message:", error);
                    // Gửi thông báo lỗi nếu WebSocket còn mở
                    if (ws.readyState === WebSocket.OPEN) {
                        try {
                            ws.send(JSON.stringify({
                                type: "error",
                                message: "Invalid message format or processing error"
                            }));
                        } catch (sendError) {
                            // Không làm gì thêm nếu không thể gửi
                        }
                    }
                }
            });

            
            ws.on('close', async function close() {
                const client = Client_Connected.get(ws.id);
                if (client) {
                    log(colors.red, `FX_CLIENT - ${port} `, colors.reset, `Client disconnected: ${client.Broker}`);
                    await Redis.deleteBroker(client.Broker).catch(err => log(colors.red, `FX_CLIENT - ${port} `, colors.reset, "Error deleting broker:", err));
                    Client_Connected.delete(ws.id);
                }
            });
        });
        
        // Khởi động server HTTP trên port
        server.listen(port, () => {
            // console.log(Color_Log_Success, `WebSocket server đang chạy tại port ${port}`);
            log(colors.green, 'FOREX WS', colors.reset, `WebSocket server đang chạy tại port ${port}`);
        });
        
        return wss;
    } catch (error) {
        console.log(Color_Log_Error, "Error setting up WebSocket server:", error);
        throw error; // Re-throw để caller biết server không khởi động được
    }
}


function SaveAll_Info() {
    setInterval(async () => {
        try {
            const getData = await broker_Actived.find({});
            brokersActived = getData;
            info_symbol_config = await symbolSetting.find({});
            // console.log(Color_Log_Success, "Lưu thông tin: ", info_symbol_config);
        } catch (error) {
            console.error("Lỗi khi lấy thông tin brokers:", error);
        }
        // console.log("Lưu thông tin brokers vào cache thành công:", brokersActived);
    }, 1000); // Lặp lại mỗi 5 phút
}






module.exports = setupWebSocketServer;