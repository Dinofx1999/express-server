const WebSocket = require('ws');
const http = require('http');
require('dotenv').config();
const { getTimeGMT7 } = require('../Helpers/time');


// Import các modules hiện tại
const Redis = require('../Redis/clientRedis');
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

    Redis.subscribe(String(port), async (channel, message) => {
        const Broker = channel.Broker
        for (const [id, element] of Client_Connected.entries()) {
            if(element.Broker == Broker) {
                if (element.ws.readyState === WebSocket.OPEN) {
                    console.log(Color_Log_Success, `Publish to Broker: ${channel}`);
                    if(channel.Symbol === "all") {
                        const Mess = JSON.stringify({type : "Reset_All", Success: 1 });
                        console.log(element);
                        element.ws.send(Mess);
                    }else if(channel.type === "destroy_broker"){
                        const Mess = JSON.stringify({type : "Destroy_Broker", Success: 1 , message: channel.Symbol});
                        element.ws.send(Mess);
                        console.log(Mess);
                    }else{
                        const Mess = JSON.stringify({type : "Reset_Only", Success: 1 , message: channel.Symbol});
                        element.ws.send(Mess);
                        console.log(Mess);
                    }
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

    const interval = setInterval(function ping() {
        wss.clients.forEach(async function each(ws) {
            //Lấy giá của 1 Symbol của tất cả Broker
            
            const data = await Redis.Broker_names();
            ws.send(JSON.stringify({time: getTimeGMT7() , data: data }));
        });
    }, 500);

    wss.on('close', function close() {
        clearInterval(interval);
    });

    try {   
        wss.on('connection', async function connection(ws, req) {

            log(colors.green, `WEB BROKERS - ${port}`, colors.reset, "New client connected");
            
            try {
                // ===== LOGIC KẾT NỐI GỐC CỦA BẠN BẮT ĐẦU TỪ ĐÂY =====
                interval;
            } catch (error) {
                log(colors.red, `WEB BROKERS - ${port}`, colors.reset, "Error during connection setup:", error);
                
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
                    
                    Redis.deleteBroker(client.Broker).catch(err => log(colors.red, `WEB BROKERS - ${port}`, colors.reset, "Error deleting broker:", err));
                    Client_Connected.delete(ws.id);
                }
            });
        });
        
        // Khởi động server HTTP trên port
        server.listen(port, () => {
            // console.log(Color_Log_Success, `WebSocket server đang chạy tại port ${port}`);
            log(colors.green, 'WS WEB BROKER', colors.reset, `url = ws://${process.env.DOMAIN}:${port}`);
        });
        
        return wss;
    } catch (error) {
        console.log(Color_Log_Error, "Error setting up WebSocket server:", error);
        throw error; // Re-throw để caller biết server không khởi động được
    }
}


module.exports = setupWebSocketServer;