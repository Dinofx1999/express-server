const cluster = require('cluster');
const os = require('os');
const mongoose = require("mongoose");
const {log , colors} = require('../Helpers/Log');
// Import các module
const Redis = require('../Redis/clientRedis');
const WebSocket = require('../WebSocket/forex.gateway');
// const Run_Lech_Gia = require('./PhanTich/Lech_Gia');
// const Web_Frontend = require('./WS_Web_FE');
// const WS_Symbol = require('../Controller/WS_Symbol');
// const WS_Symbol_Broker = require('../Controller/WS_Symbol_Broker');
// const WS_Brokers = require('../Controller/WS_Brokers');

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
//     5002, 5003, 5004, 5005, 5006, 5007, 5008, 5009,
//     5010, 5011, 5012, 5013, 5014, 5015, 5016, 5017,
//     5018, 5019, 5020, 5021, 5022, 5023, 5024, 5025,
//     5026, 5027, 5028, 5029, 5030, 5031, 5032, 5033,
// ];

const WS_PORTS = [
    5002, 5003, 5004
];

const WS_PORTS_COPYTRADE_MASTER = [
    7000, 7001, 7002
];
const WS_PORTS_COPYTRADE_CUSTOMER = [
    6000, 6001, 6002
];

async function connectToMongoDB() {
    const url_ = 'mongodb://localhost:27017/tickdata';
    try {
        await mongoose.connect(url_, {
            serverSelectionTimeoutMS: 30000,
            connectTimeoutMS: 30000,
            socketTimeoutMS: 60000,
        });
        log(colors.green, 'MONGODB', colors.reset, 'Connected to MongoDB');
        
        return true;
    } catch (err) {
        if (err instanceof mongoose.Error) {
            log(colors.red, 'MONGODB', colors.reset, 'Mongoose Error:', err);
        } else {
            log(colors.red, 'MONGODB', colors.reset, 'Unknown Error:', err);
        }
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
                cluster.fork({ WORKER_TYPE: 'WS', PORT: WS_PORTS[i] });
                log(colors.cyan, 'MASTER', colors.reset, `Forking worker for WS on port ${WS_PORTS[i]}`);
                
            }

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
                case 'WS':
                    log(colors.green, 'WORKER', colors.reset, `Starting WebSocket on port ${port} PID: ${process.pid}`);
                    WebSocket(port);
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