const WebSocket = require('ws');
const http = require('http');
require('dotenv').config({ quiet: true });
const { getTimeGMT7 } = require('../Helpers/time');

// Import các modules
const Redis = require('../Redis/clientRedis');

const RedisH = require('../Redis/redis.helper');
const {getBrokerResetting} = require('../Redis/redis.helper2');
RedisH.initRedis({
  host: '127.0.0.1',
  port: 6379,
  db: 0,          // ⚠️ PHẢI giống worker ghi
  compress: true
});
const { getAllUniqueSymbols} = require("../Redis/redis.price.query");

var Color_Log_Success = "\x1b[32m%s\x1b[0m";
var Color_Log_Error = "\x1b[31m%s\x1b[0m";

const Client_Connected = new Map();
const clientIntervals = new Map();  // ← Khai báo ở ngoài, KHÔNG dùng this

const { log, colors } = require('../Helpers/Log');
const { formatString, normSym } = require('../Helpers/text.format');

function setupWebSocketServer(port) {

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

    wss.on('error', function(error) {
        console.log(Color_Log_Error, "WebSocket Server Error:", error);
    });

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
            // ⚠️ GÁN ID TRƯỚC KHI SỬ DỤNG
            ws.id = `client_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
            ws.isAlive = true;
            ws.on('pong', heartbeat);

            try {
                await startJob(ws, ws.id);
            } catch (error) {
                log(colors.red, `FX_CLIENT - ${port} `, colors.reset, "Error during connection setup:", error.message);
            }

            ws.on('error', function(error) {
                log(colors.red, `FX_CLIENT - ${port} `, colors.reset, "WS Error:", error.message);
            });
            
            ws.on('message', async function incoming(message) {
                // Handle messages
            });

            ws.on('close', function close() {
                // const client = Client_Connected.get(ws.id);
                // if (client) {
                //     log(colors.red, `FX_CLIENT - ${port} `, colors.reset, "Client disconnected:", client.Broker);
                //     Redis.deleteBroker(client.Broker).catch(err => 
                //         log(colors.red, `FX_CLIENT - ${port} `, colors.reset, "Error deleting broker:", err)
                //     );
                //     Client_Connected.delete(ws.id);
                // }
                // Stop job khi disconnect
                stopJob(ws.id);
            });
        });
        
        server.listen(port, () => {
            log(colors.green, 'WS WEB SYMBOL', colors.reset, `WebSocket server đang chạy tại port ${port}`);
        });
        
        return wss;
    } catch (error) {
        console.log(Color_Log_Error, "Error setting up WebSocket server:", error);
        throw error;
    }
}

// ✅ START JOB - BỎ this
async function startJob(client, clientId) {
    await stopJob(clientId);

    const interval = Number(process.env.CRON_INTERVAL_BROKER_INFO || 500);
    
    const jobInterval = setInterval(async () => {
        if (client.readyState !== WebSocket.OPEN) {
            stopJob(clientId);  // ← BỎ this
            return;
        }

        try {
                const now = new Date().toISOString();
                
                // Lấy data từ Redis
                const prices = await Redis.getAnalysis();
                
                const resetting = await getBrokerResetting();
                const symbols = await getAllUniqueSymbols();
                
                // ✅ DEFENSIVE CHECK
                const analysis = prices || { Type_1: [], Type_2: [], time_analysis: null };
                
                // Build payload
                const payload = {
                    analysis: {
                        Type_1: analysis.Type_1 || [],
                        Type_2: analysis.Type_2 || []
                    },
                    timeAnalysis: analysis.time_analysis || null,
                    timestamp: now,
                    symbols: symbols || [],
                    resetting: resetting || false
                };
                
                client.send(JSON.stringify(payload));
            } catch (error) {
            console.error(`❌ Job error for ${clientId}:`, error.message);
            
            if (client.readyState === WebSocket.OPEN) {
                client.send(JSON.stringify({
                    type: 'error',
                    message: 'Failed to fetch price data',
                    timestamp: new Date().toISOString(),
                }));
            }
        }
    }, interval);

    // ← BỎ this
    clientIntervals.set(clientId, jobInterval);
}

// ✅ STOP JOB - BỎ this
async function stopJob(clientId) {
    const interval = clientIntervals.get(clientId);  // ← BỎ this
    
    if (interval) {
        clearInterval(interval);
        clientIntervals.delete(clientId);  // ← BỎ this
    }
}

module.exports = setupWebSocketServer;