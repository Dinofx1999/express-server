const WebSocket = require('ws');
const http = require('http');
require('dotenv').config();
const { getTimeGMT7 } = require('../Helpers/time');

const Redis = require('../Redis/clientRedis');

const requestCounts = new Map();
const MAX_REQUESTS = 10;
const TIME_WINDOW = 1000;
var { broker_Actived, symbolSetting } = require('../../../models/index');

var Color_Log_Success = "\x1b[32m%s\x1b[0m";
var Color_Log_Error = "\x1b[31m%s\x1b[0m";
const Client_Connected = new Map();

const { log, colors } = require('../Helpers/Log');
const { formatString, normSym } = require('../Helpers/text.format');

function setupWebSocketServer(port) {

    Redis.subscribe(String(port), async (channel, message) => {
        const Broker = channel.Broker;
        for (const [id, element] of Client_Connected.entries()) {
            if (element.Broker == Broker) {
                if (element.ws.readyState === WebSocket.OPEN) {
                    console.log(Color_Log_Success, `Publish to Broker: ${channel}`);
                    if (channel.Symbol === "all") {
                        const Mess = JSON.stringify({ type: "Reset_All", Success: 1 });
                        element.ws.send(Mess);
                    } else if (channel.type === "destroy_broker") {
                        const Mess = JSON.stringify({ type: "Destroy_Broker", Success: 1, message: channel.Symbol });
                        element.ws.send(Mess);
                    } else {
                        const Mess = JSON.stringify({ type: "Reset_Only", Success: 1, message: channel.Symbol });
                        element.ws.send(Mess);
                    }
                }
            }
        }
    });

    const server = http.createServer((req, res) => {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('WebSocket server đang chạy\n');
    });

    const wss = new WebSocket.Server({
        server: server,
        maxPayload: 1000 * 1024 * 1024,
        perMessageDeflate: false,
        handshakeTimeout: 10000,
    });

    wss.on('error', function (error) {
        console.log(Color_Log_Error, "WebSocket Server Error:", error);
    });

    // ✅ Broadcast loop tối ưu
    function startBroadcastLoop() {
        const BROADCAST_INTERVAL = 100;

        async function broadcast() {
            const startTime = Date.now();

            try {
                // 1️⃣ Thu thập tất cả symbols unique từ clients
                const symbolSet = new Set();
                const clientsBySymbol = new Map();

                wss.clients.forEach((ws) => {
                    if (ws.readyState === WebSocket.OPEN && ws.symbol) {
                        const sym = ws.symbol.toUpperCase();
                        symbolSet.add(sym);

                        if (!clientsBySymbol.has(sym)) {
                            clientsBySymbol.set(sym, []);
                        }
                        clientsBySymbol.get(sym).push(ws);
                    }
                });

                const symbols = Array.from(symbolSet);

                if (symbols.length === 0) {
                    setTimeout(broadcast, BROADCAST_INTERVAL);
                    return;
                }

                // 2️⃣ Lấy TẤT CẢ data 1 lần duy nhất
                const dataMap = await Redis.getMultipleSymbolDetails(symbols);
                const time = getTimeGMT7();

                // 3️⃣ Gửi data cho từng group clients
                for (const [sym, clients] of clientsBySymbol) {
                    const data = dataMap.get(sym) || [];
                    const message = JSON.stringify({ time, data });

                    // Gửi cùng 1 message cho tất cả clients xem symbol này
                    for (const ws of clients) {
                        try {
                            if (ws.readyState === WebSocket.OPEN) {
                                ws.send(message);
                            }
                        } catch (err) {
                            // Ignore send errors
                        }
                    }
                }

            } catch (error) {
                console.error('Broadcast error:', error.message);
            }

            // Schedule next broadcast
            const elapsed = Date.now() - startTime;
            const nextDelay = Math.max(0, BROADCAST_INTERVAL - elapsed);
            setTimeout(broadcast, nextDelay);
        }

        broadcast();
    }

    // Bắt đầu broadcast loop
    startBroadcastLoop();

    try {
        wss.on('connection', async function connection(ws, req) {
            const url = req.url;
            const symbol = url.slice(1).toUpperCase();
            ws.symbol = symbol;

            log(colors.green, `WEB SYMBOL - ${symbol}`, colors.reset, "New client connected");

            ws.on('error', function (error) {
                console.error('WebSocket error:', error.message);
            });

            ws.on('message', async function incoming(message) {
                // Handle messages if needed
            });

            ws.on('close', function close() {
                const client = Client_Connected.get(ws.id);
                if (client) {
                    log(colors.red, `FX_CLIENT - ${port} `, colors.reset, "Client disconnected:", client.Broker);
                    Redis.deleteBroker(client.Broker).catch(err =>
                        log(colors.red, `FX_CLIENT - ${port} `, colors.reset, "Error deleting broker:", err)
                    );
                    Client_Connected.delete(ws.id);
                }
            });
        });

        server.listen(port, () => {
            log(colors.green, 'WS WEB SYMBOL', colors.reset, `url = ws://${process.env.DOMAIN}:${port}/:symbol`);
        });

        return wss;
    } catch (error) {
        console.log(Color_Log_Error, "Error setting up WebSocket server:", error);
        throw error;
    }
}

module.exports = setupWebSocketServer;