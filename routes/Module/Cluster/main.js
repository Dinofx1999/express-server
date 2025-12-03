const cluster = require('cluster');
const os = require('os');
const mongoose = require("mongoose");
const {log , colors} = require('../Helpers/Log');
// Import các module
const Redis = require('../Redis/clientRedis');
//WebSocket
const WebSocket = require('../WebSocket/forex.gateway');
const WS_Symbol = require('../WebSocket/web.gateway.symbol');
const WS_Broker = require('../WebSocket/web.gateway.brokers');
const WS_Info_Broker = require('../WebSocket/web.gateway.broker.info');
const WS_Analysis = require('../WebSocket/web.gateway.analys.mongo');
require('dotenv').config({ quiet: true });
//JOBs
const JOB_Analysis = require('../Jobs/AnalysisMain');  // Phân Tích Dữ liệu Giá Forex và Lưu vào MongoDB
const JOB_Cron_DatAnalyses_MongoDB = require('../Jobs/Cron.Mongo.Analyses'); // Cron Lưu Dữ liệu Phân Tích từ MongoDB vào Redis

// //CopyTrade
// const WS_Mater_CopyTrade = require('./CopyTrade/WS_Mater_CopyTrade');
// const WS_Customer_CopyTrade = require('./CopyTrade/WS_Customer_CopyTrade');

// Cấu hình màu log
const Color_Log_Master = "\x1b[36m%s\x1b[0m";
const Color_Log_Worker = "\x1b[33m%s\x1b[0m";
const Color_Log_Error = "\x1b[31m%s\x1b[0m";
const Color_Log_Success = "\x1b[32m%s\x1b[0m";

// Danh sách cổng WebSocket
// const WS_PORTS = [
//     1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009,
//     1010, 1011, 1012, 1013, 1014, 1015, 1016, 1017,
//     1018, 1019, 1020, 1021, 1022, 1023, 1024, 1025,
//     1026, 1027, 1028, 1029, 1030, 1031, 1032, 1033,
// ];

const WS_PORTS = [
    1002, 1003, 1004
];

const WS_PORTS_COPYTRADE_MASTER = [
    7000, 7001, 7002
];
const WS_PORTS_COPYTRADE_CUSTOMER = [
    6000, 6001, 6002
];

async function connectToMongoDB() {
    // Kiểm tra nếu đã kết nối rồi thì bỏ qua
    if (mongoose.connection.readyState === 1) {
        log(colors.green, 'MONGODB', colors.reset, 'Already connected to MongoDB');
        return true;
    }
    
    const url_ = String(process.env.MONGO_URI || 'mongodb://localhost:27017/forex_db');
    
    try {
        await mongoose.connect(url_, {
            serverSelectionTimeoutMS: 30000,
            connectTimeoutMS: 30000,
            socketTimeoutMS: 60000,
        });
        log(colors.green, 'MONGODB', colors.reset, `Connected to MongoDB , URL: ${url_}`);
        return true;
    } catch (err) {
        console.error('MongoDB Error:', err.message);
        return false;
    }
}
async function MainStream() {
    try {
        const mongoConnected = await connectToMongoDB();
        if (!mongoConnected) {
            log(colors.red, 'MASTER', colors.reset, 'Failed to connect to MongoDB. Exiting...');
            process.exit(1);
        }

        if (cluster.isPrimary) {
            try {
                await Redis.clearData();
                log(colors.cyan, 'MASTER', colors.reset, 'Redis data cleared successfully');
            } catch (redisError) {
                log(colors.red, 'MASTER', colors.reset, `Error clearing Redis data:`, redisError);
            }

            process.on('uncaughtException', (err) => {
                log(colors.red, 'MASTER', colors.reset, `Uncaught Exception in primary process:`, err);
            });

            // Fork workers for each WebSocket port
            for (let i = 0; i < WS_PORTS.length; i++) {
                cluster.fork({ WORKER_TYPE: 'WS_FOREX', PORT: WS_PORTS[i] });
                log(colors.cyan, 'MASTER', colors.reset, `Forking worker for WS on port ${WS_PORTS[i]}`);
                
            }
            
            cluster.fork({ WORKER_TYPE: 'WS_WEB_SYMBOL', PORT: process.env.WS_PORT_SYMBOL || 2000 });
            cluster.fork({ WORKER_TYPE: 'WS_WEB_BROKER', PORT: process.env.WS_PORT_BROKER || 2001 });
            cluster.fork({ WORKER_TYPE: 'WS_INFO_BROKER', PORT: process.env.WS_PORT_INFO_BROKER || 2002 });
            cluster.fork({ WORKER_TYPE: 'WS_ANALYSIS', PORT: process.env.WS_PORT_ANALYSIS || 2003 });
            // Fork JOB worker
            cluster.fork({ WORKER_TYPE: 'JOBS', PORT: process.env.WS_PORT_ANALYSIS_JOB || 4000 });
            cluster.fork({ WORKER_TYPE: 'JOBS_CRON_MONGO', PORT: process.env.WS_PORT_CRON_MONGO || 4001 });

            // Restart worker with delay to avoid memory leak
            cluster.on('exit', (worker, code, signal) => {
                log(colors.red, 'WORKER', colors.reset, `Worker ${worker.id} died with code ${code} and signal ${signal}`);
                log(colors.cyan, 'WORKER', colors.reset, `Restarting worker in 3 seconds...`);
                const workerEnv = worker.process.env;
                setTimeout(() => {
                    // Chỉ restart nếu WORKER_TYPE tồn tại
                    if (workerEnv && workerEnv.WORKER_TYPE) {
                        cluster.fork(workerEnv);
                    } else {
                        console.error(Color_Log_Error, 'Không thể restart worker: WORKER_TYPE không xác định!', workerEnv);
                    }
                }, 3000);
            });
        } else {
            const workerType = process.env.WORKER_TYPE;
            const port = parseInt(process.env.PORT, 10);

            if (!workerType) {
                console.error(Color_Log_Error, 'WORKER_TYPE is not defined', workerType);
                process.exit(1);
            }

            if (isNaN(port)) {
                console.error(Color_Log_Error, `Invalid port: ${process.env.PORT}`);
                process.exit(1);
            }

            process.on('uncaughtException', (err) => {
                console.error(Color_Log_Error, `Uncaught Exception in worker ${workerType}:`, err);
                process.exit(1);
            });

            switch (workerType) {
                case 'WS_FOREX':
                    // log(colors.green, 'WORKER', colors.reset, `Starting WebSocket FOREX on port ${port} PID: ${process.pid}`);
                    WebSocket(port);
                    break;
                case 'WS_WEB_SYMBOL':
                    // log(colors.green, 'WORKER', colors.reset, `Starting WebSocket WEB on port ${port} PID: ${process.pid}`);
                    WS_Symbol(port);
                    break;
                case 'WS_WEB_BROKER':
                    // log(colors.green, 'WORKER', colors.reset, `Starting WebSocket WEB BROKER on port ${port} PID: ${process.pid}`);
                    WS_Broker(port);
                    break;
                case 'WS_INFO_BROKER':
                    // log(colors.green, 'WORKER', colors.reset, `Starting WebSocket INFO BROKER on port ${port} PID: ${process.pid}`);
                    WS_Info_Broker(port);
                    break;
                case 'WS_ANALYSIS':
                    log(colors.green, 'WORKER', colors.reset, `Starting WS_ANALYSIS on port ${port} PID: ${process.pid}`);
                    WS_Analysis(port);
                    break;
                
                case 'JOBS':
                    log(colors.green, 'WORKER', colors.reset, `Starting JOB ANALYSIS PID: ${process.pid}`);
                    try {
                        await JOB_Analysis();
                    } catch (err) {
                        console.error('JOB_Analysis Error:', err);
                    }
                    break;
                case 'JOBS_CRON_MONGO':
                    log(colors.green, 'WORKER', colors.reset, `Starting JOB CRON MONGO PID: ${process.pid}`);
                    try {
                        await JOB_Cron_DatAnalyses_MongoDB();
                    } catch (err) {
                        console.error('JOB_Cron_DatAnalyses_MongoDB Error:', err);
                    }
                    break;
                default:
                    log(colors.red, 'WORKER', colors.reset, `Unknown worker type: ${workerType}`);
                    
                    process.exit(1);
            }
        }
    } catch (error) {
        console.error(Color_Log_Error, `Critical error in MainStream:`, error);
        process.exit(1);
    }
}

MainStream();
module.exports = MainStream;