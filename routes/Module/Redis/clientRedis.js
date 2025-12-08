const redisClient = require('./redisManager');
const Redis = require('ioredis');
const { log, colors } = require('../Helpers/Log');

class RedisManager {
    constructor() {
        this.client = redisClient;
        this.messageHandlers = new Map();  // ✅ Quản lý handlers
        this.isSubscriberSetup = false;    // ✅ Flag để tránh duplicate listeners

        // ✅ Thêm retry strategy
        const redisConfig = {
            host: 'localhost',
            port: 6379,
            retryStrategy: (times) => {
                const delay = Math.min(times * 50, 2000);
                return delay;
            },
            maxRetriesPerRequest: 3,
        };

        this.publisherClient = new Redis(redisConfig);
        this.subscriberClient = new Redis(redisConfig);

        this.setupEventHandlers();
    }

    setupEventHandlers() {
        this.publisherClient.on('connect', () => {
            log(colors.green, 'REDIS', colors.reset, 'Publisher connected');
        });
        this.publisherClient.on('error', (err) => {
            console.error('Redis Publisher Error:', err);
        });

        this.subscriberClient.on('connect', () => {
            log(colors.green, 'REDIS', colors.reset, 'Subscriber connected');
        });
        this.subscriberClient.on('error', (err) => {
            console.error('Redis Subscriber Error:', err);
        });

        // ✅ Setup message handler MỘT LẦN DUY NHẤT
        this.subscriberClient.on('message', (channel, message) => {
            const handler = this.messageHandlers.get(channel);
            if (handler) {
                try {
                    const parsedMessage = this.tryParseJSON(message);
                    handler(parsedMessage);
                } catch (error) {
                    log(colors.red, 'REDIS', colors.reset, `Message processing error: ${error.message}`);
                }
            }
        });
        this.isSubscriberSetup = true;
    }

    // ✅ FIX: Subscribe không còn memory leak
    subscribe(channel, callback) {
        try {
            log(colors.yellow, 'REDIS', colors.reset, `Subscribing to ${channel}`);
            
            // Lưu handler vào Map thay vì tạo listener mới
            this.messageHandlers.set(channel, callback);
            this.subscriberClient.subscribe(channel);
            
        } catch (error) {
            console.error('Subscribe error:', error);
        }
    }

    // ✅ Thêm hàm unsubscribe
    unsubscribe(channel) {
        try {
            this.messageHandlers.delete(channel);
            this.subscriberClient.unsubscribe(channel);
            log(colors.yellow, 'REDIS', colors.reset, `Unsubscribed from ${channel}`);
        } catch (error) {
            console.error('Unsubscribe error:', error);
        }
    }

    async publish(channel, message) {
        try {
            const payload = typeof message === 'object' ? JSON.stringify(message) : message;
            const result = await this.publisherClient.publish(channel, payload);
            return result;
        } catch (error) {
            console.error('Publish error:', error);
            throw error;
        }
    }

    tryParseJSON(message) {
        try {
            return JSON.parse(message);
        } catch {
            return message;
        }
    }

    // ✅ FIX: Dùng SCAN thay vì KEYS
    async scanKeys(pattern) {
        const keys = [];
        let cursor = '0';
        
        do {
            const [newCursor, foundKeys] = await this.client.scan(
                cursor, 
                'MATCH', pattern, 
                'COUNT', 100  // Scan từng batch 100 keys
            );
            cursor = newCursor;
            keys.push(...foundKeys);
        } while (cursor !== '0');
        
        return keys;
    }

    async saveBrokerData(broker, data) {
        const key = `BROKER:${broker}`;
        await this.client.set(key, JSON.stringify(data));
    }

    // ✅ FIX: Dùng SCAN
    async getAllBrokers_2() {
        const keys = await this.scanKeys('BROKER:*');
        const result = {};
        
        if (keys.length === 0) return result;
        
        // ✅ Dùng MGET thay vì nhiều GET
        const values = await this.client.mget(keys);
        
        keys.forEach((key, index) => {
            const raw = values[index];
            if (raw) {
                try {
                    result[key.replace('BROKER:', '')] = JSON.parse(raw);
                } catch {
                    result[key.replace('BROKER:', '')] = raw;
                }
            }
        });
        
        return result;
    }

    async saveBrokerData_(data, port) {
        try {
            if (!data.Broker) {
                throw new Error('Invalid broker data: Missing Broker name');
            }

            const brokerKey = `BROKER:${data.Broker}`;
            const now = new Date();
            const formattedDate = `${now.getFullYear()}.${String(now.getMonth() + 1).padStart(2, '0')}.${String(now.getDate()).padStart(2, '0')} ${String(now.getHours()).padStart(2, '0')}:${String(now.getMinutes()).padStart(2, '0')}:${String(now.getSeconds()).padStart(2, '0')}`;

            const brokerData = {
                ...data,
                port: port,
                lastUpdated: formattedDate,
            };

            const pipeline = this.publisherClient.pipeline();
            pipeline.set(brokerKey, JSON.stringify(brokerData));

            if (data.Infosymbol && Array.isArray(data.Infosymbol)) {
                for (const symbol of data.Infosymbol) {
                    const symbolKey = `symbol:${data.Broker}:${symbol.Symbol}`;
                    pipeline.set(symbolKey, JSON.stringify({
                        ...symbol,
                        broker: data.Broker,
                        lastUpdated: brokerData.lastUpdated,
                    }));
                }
            }

            await pipeline.exec();
            return true;
        } catch (error) {
            console.error('Error saving broker data:', error);
            return false;
        }
    }

    // ✅ FIX: Dùng SCAN + MGET
    async getAllBrokers() {
        try {
            const keys = await this.scanKeys('BROKER:*');
            if (keys.length === 0) return [];
            
            // ✅ MGET lấy tất cả cùng lúc - nhanh hơn nhiều
            const values = await this.client.mget(keys);
            
            const validBrokers = values
                .map(broker => {
                    if (!broker) return null;
                    try {
                        return JSON.parse(broker);
                    } catch (parseError) {
                        console.error('Error parsing broker data:', parseError);
                        return null;
                    }
                })
                .filter(broker => broker !== null);

            return validBrokers.sort((a, b) => {
                const indexA = parseInt(a.index, 10) || 0;
                const indexB = parseInt(b.index, 10) || 0;
                return indexA - indexB;
            });
        } catch (error) {
            console.error('Error getting brokers from Redis:', error);
            return [];
        }
    }

    async getAllUniqueSymbols() {
        try {
            const brokers = await this.getAllBrokers();
            const uniqueSymbols = new Set();

            brokers.forEach(broker => {
                if (broker.OHLC_Symbols && Array.isArray(broker.OHLC_Symbols)) {
                    broker.OHLC_Symbols.forEach(symbolData => {
                        if (symbolData.symbol) {
                            uniqueSymbols.add(symbolData.symbol);
                        }
                    });
                }
            });

            return Array.from(uniqueSymbols);
        } catch (error) {
            console.error('Error getting unique symbols from Redis:', error);
            return [];
        }
    }

    // ✅ FIX: Dùng SCAN
    async getSymbolDetails(symbolName) {
        try {
            const brokerKeys = await this.scanKeys('BROKER:*');
            if (brokerKeys.length === 0) return [];

            const values = await this.client.mget(brokerKeys);
            const symbolDetails = [];

            values.forEach(data => {
                if (!data) return;
                
                try {
                    const broker = JSON.parse(data);
                    if (broker.OHLC_Symbols && Array.isArray(broker.OHLC_Symbols)) {
                        const symbolInfo = broker.OHLC_Symbols.find(
                            (sym) => sym.symbol === symbolName && 
                                     sym.trade === "TRUE" && 
                                     broker.status === "True"
                        );
                        if (symbolInfo) {
                            symbolDetails.push({
                                Broker: broker.broker,
                                Broker_: broker.broker_,
                                Status: broker.status,
                                Index: broker.index,
                                ...symbolInfo,
                            });
                        }
                    }
                } catch (e) {
                    // Skip invalid JSON
                }
            });

            return symbolDetails.sort((a, b) => 
                parseFloat(a.Index || 0) - parseFloat(b.Index || 0)
            );
        } catch (error) {
            console.error(`Error getting symbol details for ${symbolName}:`, error);
            return [];
        }
    }

    async deleteBroker(brokerName) {
        try {
            if (!brokerName) throw new Error('Broker name is required');

            const brokerKey = `BROKER:${brokerName}`;
            const brokerExists = await this.client.exists(brokerKey);
            if (!brokerExists) {
                return { success: false, message: `Broker "${brokerName}" does not exist` };
            }

            // ✅ Dùng SCAN
            const symbolKeys = await this.scanKeys(`symbol:${brokerName}:*`);
            const pipeline = this.client.pipeline();
            symbolKeys.forEach((key) => pipeline.del(key));
            pipeline.del(brokerKey);
            await pipeline.exec();

            return { success: true, message: `Deleted broker "${brokerName}" and related symbols` };
        } catch (error) {
            console.error(`Error deleting broker ${brokerName}:`, error);
            return { success: false, message: `Error deleting broker: ${error.message}` };
        }
    }

    async clearData() {
        try {
            await this.client.flushall();
            log(colors.green, 'REDIS', colors.reset, 'Redis data cleared successfully.');
        } catch (error) {
            log(colors.red, 'REDIS', colors.reset, 'Error clearing Redis data:', error);
        }
    }

    // ✅ Xóa chỉ các key của app
    async clearAllAppData() {
        try {
            const patterns = ['BROKER:*', 'symbol:*', 'Analysis:*'];
            let totalDeleted = 0;

            for (const pattern of patterns) {
                const keys = await this.scanKeys(pattern);

                if (keys.length > 0) {
                    // ✅ Xóa theo batch để tránh block
                    const batchSize = 100;
                    for (let i = 0; i < keys.length; i += batchSize) {
                        const batch = keys.slice(i, i + batchSize);
                        await this.client.del(...batch);
                    }

                    log(colors.yellow, 'REDIS', colors.reset, 
                        `Deleted ${keys.length} keys matching "${pattern}"`);
                    totalDeleted += keys.length;
                }
            }

            log(colors.green, 'REDIS', colors.reset, 
                `✅ Cleared ${totalDeleted} keys total. Ready for fresh data.`);

            return { success: true, totalDeleted };
        } catch (error) {
            log(colors.red, 'REDIS', colors.reset, `Error clearing app data: ${error.message}`);
            return { success: false, error: error.message };
        }
    }

    async getBroker(brokerName) {
        try {
            if (!brokerName) {
                throw new Error('Broker name is required');
            }

            const brokerKey = `BROKER:${brokerName}`;
            const brokerData = await this.client.get(brokerKey);

            if (!brokerData) {
                return null;
            }

            return JSON.parse(brokerData);
        } catch (error) {
            console.error(`Error getting broker '${brokerName}' from Redis:`, error);
            return null;
        }
    }

    async getMultipleSymbolDetails(symbols) {
        if (!symbols || symbols.length === 0) return new Map();

        try {
            const brokerKeys = await this.scanKeys('BROKER:*');
            if (brokerKeys.length === 0) return new Map();

            const values = await this.client.mget(brokerKeys);
            const brokers = [];
            
            values.forEach(data => {
                if (!data) return;
                try {
                    brokers.push(JSON.parse(data));
                } catch (e) {
                    // Skip invalid JSON
                }
            });

            const symbolSet = new Set(symbols);
            const resultMap = new Map();

            for (const sym of symbols) {
                resultMap.set(sym, []);
            }

            for (const broker of brokers) {
                if (!broker.OHLC_Symbols || !Array.isArray(broker.OHLC_Symbols)) continue;
                if (broker.status !== "True") continue;

                for (const symbolInfo of broker.OHLC_Symbols) {
                    const sym = symbolInfo.symbol;

                    if (!symbolSet.has(sym)) continue;
                    if (symbolInfo.trade !== "TRUE") continue;

                    const details = {
                        Broker: broker.broker,
                        Broker_: broker.broker_,
                        Status: broker.status,
                        Index: broker.index,
                        Auto_Trade: broker.auto_trade,
                        Typeaccount: broker.typeaccount,
                        ...symbolInfo,
                    };

                    resultMap.get(sym).push(details);
                }
            }

            for (const [sym, details] of resultMap) {
                details.sort((a, b) => parseFloat(a.Index || 0) - parseFloat(b.Index || 0));
            }

            return resultMap;
        } catch (error) {
            console.error('Error in getMultipleSymbolDetails:', error);
            return new Map();
        }
    }

    // ✅ FIX: Không dùng Lua với KEYS nữa
    async findBrokerByIndex(index) {
        try {
            if (index === undefined || index === null) {
                throw new Error('index is required');
            }

            const brokers = await this.getAllBrokers();
            const targetIndex = String(index);
            
            return brokers.find(broker => String(broker.index) === targetIndex) || null;
        } catch (error) {
            console.error(`Error finding broker with index '${index}':`, error);
            return null;
        }
    }

    async updateBrokerStatus(broker, newStatus) {
        try {
            // console.log(`Updating status for broker: ${broker} to ${newStatus}`);
            const key = `BROKER:${broker}`;

            const raw = await this.client.get(key);
            if (raw) {
                const data = JSON.parse(raw);
                data.status = newStatus;
                data.timeUpdated = new Date().toISOString().slice(0, 19).replace('T', ' ');
                await this.client.set(key, JSON.stringify(data));
                return data;
            }
            return null;
        } catch (error) {
            console.error('Lỗi update status:', error.message);
            throw error;
        }
    }

    async getSymbol(symbol) {
        try {
            if (!symbol) {
                throw new Error(symbol + ': Symbol is required');
            }

            const brokers = await this.getAllBrokers();

            let result = null;
            let minIndex = Number.MAX_SAFE_INTEGER;

            for (const broker of brokers) {
                const brokerIndex = parseInt(broker.index, 10);

                if (!broker.OHLC_Symbols || isNaN(brokerIndex)) {
                    continue;
                }

                const symbolInfo = broker.OHLC_Symbols.find(
                    info => info.symbol === symbol && 
                            info.trade === "TRUE" && 
                            broker.status !== "Disconnect"
                );

                if (symbolInfo && brokerIndex < minIndex) {
                    minIndex = brokerIndex;
                    result = {
                        ...symbolInfo,
                        Broker: broker.broker,
                        BrokerIndex: broker.index
                    };
                }
            }

            return result;
        } catch (error) {
            console.error('Lỗi hàm getSymbol trong clientRedis.js', error);
            return null;
        }
    }

    async Broker_names() {
        try {
            const data = await this.getAllBrokers();
            const brokers = data.map(broker => {
                const { OHLC_Symbols, ...info } = broker;
                return info;
            });
            return brokers;
        } catch (error) {
            console.error('Lỗi hàm Broker_names trong clientRedis.js', error);
            return [];
        }
    }

    async saveAnalysis(Analysis, data) {
        const key = `Analysis:${Analysis}`;
        await this.client.set(key, JSON.stringify(data));
    }

    async getAnalysis() {
        const keys = await this.scanKeys('Analysis:*');
        const result = {};
        
        if (keys.length === 0) return result;
        
        const values = await this.client.mget(keys);
        
        keys.forEach((key, index) => {
            const raw = values[index];
            if (raw) {
                try {
                    result[key.replace('Analysis:', '')] = JSON.parse(raw);
                } catch {
                    result[key.replace('Analysis:', '')] = raw;
                }
            }
        });
        
        return result;
    }

    async getBrokerResetting() {
        const brokers = await this.getAllBrokers();
        
        return brokers
            .filter(broker => broker.status !== "True")
            .sort((a, b) => Number(a.index) - Number(b.index));
    }

    // ✅ Thêm: Graceful shutdown
    async disconnect() {
        try {
            this.messageHandlers.clear();
            await this.publisherClient.quit();
            await this.subscriberClient.quit();
            log(colors.yellow, 'REDIS', colors.reset, 'Disconnected gracefully');
        } catch (error) {
            console.error('Error disconnecting:', error);
        }
    }

    // ==================== RESET PROGRESS TRACKING (SIMPLE) ====================
    
    /**
     * Bắt đầu theo dõi reset
     */
    async startResetTracking(brokers) {
        try {
            const data = {
                brokers: brokers.map(b => ({
                    name: b.broker_ || b.broker,
                    percentage: 0,
                    completed: false
                })),
                currentIndex: 0,
                startedAt: Date.now()
            };
            
            await this.client.setex('reset_progress', 3600, JSON.stringify(data));
            log(colors.green, 'REDIS', colors.reset, `✅ Started tracking ${brokers.length} brokers`);
            return true;
        } catch (error) {
            console.error('Error starting reset tracking:', error);
            return false;
        }
    }

    /**
     * Update phần trăm của broker (AUTO CALL từ WebSocket)
     */
    async updateResetProgress(brokerName, percentage) {
        try {
            const data = await this.client.get('reset_progress');
            if (!data) return false; // Không có reset đang chạy
            
            const progress = JSON.parse(data);
            const broker = progress.brokers.find(b => b.name === brokerName);
            
            if (broker) {
                broker.percentage = percentage;
                if (percentage >= 30) {
                    broker.completed = true;
                    log(colors.green, 'RESET', colors.reset, 
                        `✅ ${brokerName} completed: ${percentage}%`);
                }
                await this.client.setex('reset_progress', 3600, JSON.stringify(progress));
                return true;
            }
            
            return false;
        } catch (error) {
            console.error('Error updating reset progress:', error);
            return false;
        }
    }

    /**
     * Kiểm tra broker đã hoàn thành chưa
     */
    async isResetCompleted(brokerName) {
        try {
            const data = await this.client.get('reset_progress');
            if (!data) return false;
            
            const progress = JSON.parse(data);
            const broker = progress.brokers.find(b => b.name === brokerName);
            
            return broker ? broker.completed : false;
        } catch (error) {
            return false;
        }
    }

    /**
     * Check có đang reset không
     */
    async isResetting() {
        try {
            const exists = await this.client.exists('reset_progress');
            return exists === 1;
        } catch (error) {
            return false;
        }
    }

    /**
     * Lấy status reset
     */
    async getResetStatus() {
        try {
            const data = await this.client.get('reset_progress');
            if (!data) return null;
            
            const progress = JSON.parse(data);
            const completed = progress.brokers.filter(b => b.completed).length;
            const total = progress.brokers.length;
            
            return {
                isRunning: true,
                progress: `${completed}/${total}`,
                percentage: Math.round((completed / total) * 100),
                startedAt: progress.startedAt,
                brokers: progress.brokers
            };
        } catch (error) {
            return null;
        }
    }

    /**
     * Xóa tracking khi xong
     */
    async clearResetTracking() {
        try {
            await this.client.del('reset_progress');
            log(colors.green, 'REDIS', colors.reset, '✅ Cleared reset tracking');
        } catch (error) {
            console.error('Error clearing reset tracking:', error);
        }
    }

    // ==================== END RESET PROGRESS TRACKING ====================
}

module.exports = new RedisManager();