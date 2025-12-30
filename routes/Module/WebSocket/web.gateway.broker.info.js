const WebSocket = require('ws');
const http = require('http');
require('dotenv').config();
const { getTimeGMT7 } = require('../Helpers/time');

// Import các modules hiện tại
const Redis = require('../Redis/clientRedis');

const RedisH = require('../Redis/redis.helper');
RedisH.initRedis({
  host: '127.0.0.1',
  port: 6379,
  db: 0,          // ⚠️ PHẢI giống worker ghi
  compress: true
});

const { getAllPricesByBroker} = require("../Redis/redis.helper2");
// const getData = require('../Helpers/read_Data');
// const Data = require('../Helpers/get_data');
// const e = require('express');
// const priceBatcher = require('./Connect/bactching');
// const { time } = require('console');
const requestCounts = new Map(); // Lưu số lượng request theo client ID
const MAX_REQUESTS = 10; // Số request tối đa cho mỗi client
const TIME_WINDOW = 1000;
var {broker_Actived , symbolSetting} = require('../../../models/index');

var Color_Log_Success = "\x1b[32m%s\x1b[0m";
var Color_Log_Error = "\x1b[31m%s\x1b[0m";
const Client_Connected = new Map();
const Symbols_Tracked = "";

const {log , colors} = require('../Helpers/Log');
const {formatString , normSym} = require('../Helpers/text.format');

function setupWebSocketServer(port) {

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

    const interval = setInterval(function ping() {
        wss.clients.forEach(async function each(ws) {
            //Lấy Thông Tin của 1 Broker
            const list = await getAllPricesByBroker(ws.Broker);
            ws.send(JSON.stringify({time: getTimeGMT7() , data: list }));
        });
    }, 200);

    wss.on('close', function close() {
        clearInterval(interval);
    });

    try {   
        wss.on('connection', async function connection(ws, req) {
            const url = req.url;             // "/GBPUSD"
            const Broker = url.slice(1);     // "GBPUSD"
            ws.Broker = Broker;
            log(colors.green, `WEB INFO BROKERS - ${Broker}`, colors.reset, "New client connected");
            
            try {
                // ===== LOGIC KẾT NỐI GỐC CỦA BẠN BẮT ĐẦU TỪ ĐÂY =====
                interval;
            } catch (error) {
                log(colors.red, `WEB INFO BROKERS - ${Broker}`, colors.reset, "Error during connection setup:", error);
                
            }
            ws.on('error', function(error) {

            });
            
            // Xử lý khi có message từ client
            ws.on('message', async function incoming(message) {

            });

            
            ws.on('close', function close() {
                const client = Client_Connected.get(ws.id);
                if (client) {
                    log(colors.red, `WEB BROKERS - ${port}`, colors.reset   , "Client disconnected:", client.Broker);
                }
            });
        });
        
        // Khởi động server HTTP trên port
        server.listen(port, () => {
            // console.log(Color_Log_Success, `WebSocket server đang chạy tại port ${port}`);
            log(colors.green, 'WS WEB INFO BROKER', colors.reset, `url = ws://${process.env.DOMAIN}:${port}/:broker`);
        });
        
        return wss;
    } catch (error) {
        console.log(Color_Log_Error, "Error setting up WebSocket server:", error);
        throw error; // Re-throw để caller biết server không khởi động được
    }
}


module.exports = setupWebSocketServer;