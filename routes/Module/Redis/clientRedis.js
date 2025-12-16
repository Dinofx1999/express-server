const redisClient = require('./redisManager');
const Redis = require('ioredis');
const { log, colors } = require('../Helpers/Log');
const zlib = require('zlib');
const { promisify } = require('util');

const compress = promisify(zlib.gzip);
const decompress = promisify(zlib.gunzip);

class RedisManager {
    constructor() {
        this.client = redisClient;
        this.messageHandlers = new Map();
        this.isSubscriberSetup = false;

        // ✅ IN-MEMORY CACHE - Giảm 80% Redis calls
        this.cache = {
            brokers: null,           // Cache toàn bộ brokers
            brokerMap: new Map(),    // Cache từng broker theo name
            symbols: null,           // Cache unique symbols
            lastUpdate: 0,           // Timestamp cache
            ttl: 5000,              // Cache 5 giây
        };

        // ✅ COMPRESSION CONFIG
        this.useCompression = true;
        this.compressionThreshold = 1024; // Chỉ nén data > 1KB

        const redisConfig = {
            host: 'localhost',
            port: 6379,
            retryStrategy: (times) => {
                const delay = Math.min(times * 50, 2000);
                return delay;
            },
            maxRetriesPerRequest: 3,
            enableOfflineQueue: false, // ✅ Tắt queue khi offline
            lazyConnect: false,
        };

        this.publisherClient = new Redis(redisConfig);
        this.subscriberClient = new Redis(redisConfig);

        this.setupEventHandlers();
        this.startCacheInvalidation();
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

    // ✅ AUTO INVALIDATE CACHE
    startCacheInvalidation() {
        setInterval(() => {
            const now = Date.now();
            if (now - this.cache.lastUpdate > this.cache.ttl) {
                this.invalidateCache();
            }
        }, 1000);
    }

    invalidateCache() {
        this.cache.brokers = null;
        this.cache.brokerMap.clear();
        this.cache.symbols = null;
    }

    // ✅ COMPRESSION HELPERS
    async compressData(data) {
        if (!this.useCompression) return data;
        
        const str = typeof data === 'string' ? data : JSON.stringify(data);
        if (str.length < this.compressionThreshold) return str;
        
        try {
            const compressed = await compress(Buffer.from(str));
            return 'GZIP:' + compressed.toString('base64');
        } catch (error) {
            return str; // Fallback nếu lỗi
        }
    }

    async decompressData(data) {
        if (!data || !data.startsWith('GZIP:')) {
            return data;
        }
        
        try {
            const compressed = Buffer.from(data.slice(5), 'base64');
            const decompressed = await decompress(compressed);
            return decompressed.toString();
        } catch (error) {
            return data;
        }
    }

    subscribe(channel, callback) {
        try {
            log(colors.yellow, 'REDIS', colors.reset, `Subscribing to ${channel}`);
            this.messageHandlers.set(channel, callback);
            this.subscriberClient.subscribe(channel);
        } catch (error) {
            console.error('Subscribe error:', error);
        }
    }

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

    async scanKeys(pattern) {
        const keys = [];
        let cursor = '0';
        
        do {
            const [newCursor, foundKeys] = await this.client.scan(
                cursor, 
                'MATCH', pattern, 
                'COUNT', 200  // ✅ Tăng từ 100 -> 200
            );
            cursor = newCursor;
            keys.push(...foundKeys);
        } while (cursor !== '0');
        
        return keys;
    }

    // ✅ OPTIMIZED: Lazy parsing + compression
    async saveBrokerData(broker, data) {
        const key = `BROKER:${broker}`;
        
        try {
            const expectedTotal = parseInt(data.totalsymbol || 0);
            const existingData = await this.client.get(key);
            
            if (!existingData) {
                const compressed = await this.compressData(data);
                await this.client.set(key, compressed);
                this.invalidateCache(); // ✅ Xóa cache khi update
                return { success: true, action: 'created' };
            }
            
            // ✅ Lazy parsing - chỉ parse khi cần
            const decompressed = await this.decompressData(existingData);
            const totalMatch = decompressed.match(/"totalsymbol"\s*:\s*"(\d+)"/);
            const currentTotal = totalMatch ? parseInt(totalMatch[1]) : 0;
            
            if (currentTotal > expectedTotal) {
                const compressed = await this.compressData(data);
                await this.client.set(key, compressed);
                this.invalidateCache();
                return { success: true, action: 'reset', total: expectedTotal };
            }
            
            const parsedExisting = JSON.parse(decompressed);
            
            // ✅ Fast update metadata
            Object.assign(parsedExisting, {
                port: data.port,
                index: data.index,
                version: data.version,
                typeaccount: data.typeaccount,
                timecurent: data.timecurent,
                auto_trade: data.auto_trade,
                status: data.status,
                timeUpdated: data.timeUpdated
            });
            
            // ✅ Optimized merge với Map
            const symbolMap = new Map(
                parsedExisting.OHLC_Symbols.map(s => [s.symbol, s])
            );
            
            let stats = { added: 0, updated: 0 };
            
            data.OHLC_Symbols?.forEach(newSymbol => {
                if (symbolMap.has(newSymbol.symbol)) {
                    stats.updated++;
                } else {
                    stats.added++;
                }
                symbolMap.set(newSymbol.symbol, newSymbol);
            });
            
            parsedExisting.OHLC_Symbols = Array.from(symbolMap.values());
            parsedExisting.totalsymbol = symbolMap.size.toString();
            
            const compressed = await this.compressData(parsedExisting);
            await this.client.set(key, compressed);
            this.invalidateCache();
            
            return { success: true, action: 'merged', ...stats, total: symbolMap.size };
            
        } catch (error) {
            throw error;
        }
    }

    async saveConfigAdmin(data) {
        const key = `CONFIG`;
        const compressed = await this.compressData(data);
        await this.client.set(key, compressed);
    }

    async getConfigAdmin() {
        const key = `CONFIG`;
        const raw = await this.client.get(key);
        if (raw) {
            try {
                const decompressed = await this.decompressData(raw);
                return JSON.parse(decompressed);
            } catch (error) {
                console.error('Error parsing config admin data:', error);
            }
        }
        return null;
    }

    // ✅ OPTIMIZED với cache
    async getAllBrokers() {
        try {
            // ✅ Return từ cache nếu còn fresh
            if (this.cache.brokers && Date.now() - this.cache.lastUpdate < this.cache.ttl) {
                return this.cache.brokers;
            }

            const keys = await this.scanKeys('BROKER:*');
            if (keys.length === 0) return [];
            
            // ✅ MGET tất cả
            const values = await this.client.mget(keys);
            
            // ✅ Parallel decompression + parsing
            const validBrokers = await Promise.all(
                values.map(async (broker, idx) => {
                    if (!broker) return null;
                    try {
                        const decompressed = await this.decompressData(broker);
                        const parsed = JSON.parse(decompressed);
                        // ✅ Cache từng broker
                        const brokerName = keys[idx].replace('BROKER:', '');
                        this.cache.brokerMap.set(brokerName, parsed);
                        return parsed;
                    } catch (parseError) {
                        return null;
                    }
                })
            );

            const result = validBrokers
                .filter(broker => broker !== null)
                .sort((a, b) => {
                    const indexA = parseInt(a.index, 10) || 0;
                    const indexB = parseInt(b.index, 10) || 0;
                    return indexA - indexB;
                });

            // ✅ Update cache
            this.cache.brokers = result;
            this.cache.lastUpdate = Date.now();

            return result;
        } catch (error) {
            console.error('Error getting brokers from Redis:', error);
            return [];
        }
    }

    // ✅ OPTIMIZED với cache
    async getAllUniqueSymbols() {
        try {
            if (this.cache.symbols && Date.now() - this.cache.lastUpdate < this.cache.ttl) {
                return this.cache.symbols;
            }

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

            const result = Array.from(uniqueSymbols);
            this.cache.symbols = result;
            return result;
        } catch (error) {
            console.error('Error getting unique symbols from Redis:', error);
            return [];
        }
    }

    // ✅ OPTIMIZED: Dùng cached brokers
    async getSymbolDetails(symbolName) {
        try {
            const brokers = await this.getAllBrokers(); // Đã có cache
            const symbolDetails = [];

            for (const broker of brokers) {
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
            }

            return symbolDetails.sort((a, b) => 
                parseFloat(a.Index || 0) - parseFloat(b.Index || 0)
            );
        } catch (error) {
            console.error(`Error getting symbol details for ${symbolName}:`, error);
            return [];
        }
    }

    // ✅ OPTIMIZED: Pipeline delete
    async deleteBroker(brokerName) {
        try {
            if (!brokerName) throw new Error('Broker name is required');

            const brokerKey = `BROKER:${brokerName}`;
            const brokerExists = await this.client.exists(brokerKey);
            if (!brokerExists) {
                return { success: false, message: `Broker "${brokerName}" does not exist` };
            }

            const symbolKeys = await this.scanKeys(`symbol:${brokerName}:*`);
            
            // ✅ Pipeline delete theo batch
            const batchSize = 100;
            for (let i = 0; i < symbolKeys.length; i += batchSize) {
                const batch = symbolKeys.slice(i, i + batchSize);
                const pipeline = this.client.pipeline();
                batch.forEach(key => pipeline.del(key));
                await pipeline.exec();
            }
            
            await this.client.del(brokerKey);
            this.invalidateCache();

            return { success: true, message: `Deleted broker "${brokerName}" and related symbols` };
        } catch (error) {
            console.error(`Error deleting broker ${brokerName}:`, error);
            return { success: false, message: `Error deleting broker: ${error.message}` };
        }
    }

    async clearData() {
        try {
            await this.client.flushall();
            this.invalidateCache();
            log(colors.green, 'REDIS', colors.reset, 'Redis data cleared successfully.');
        } catch (error) {
            log(colors.red, 'REDIS', colors.reset, 'Error clearing Redis data:', error);
        }
    }

    async clearAllBroker() {
        try {
            const patterns = ['BROKER:*', "Analysis:*", "symbol:*"];
            let totalDeleted = 0;

            for (const pattern of patterns) {
                const keys = await this.scanKeys(pattern);

                if (keys.length > 0) {
                    const batchSize = 200; // ✅ Tăng batch size
                    for (let i = 0; i < keys.length; i += batchSize) {
                        const batch = keys.slice(i, i + batchSize);
                        await this.client.del(...batch);
                    }

                    log(colors.yellow, 'REDIS', colors.reset, 
                        `Deleted ${keys.length} keys matching "${pattern}"`);
                    totalDeleted += keys.length;
                }
            }

            this.invalidateCache();
            log(colors.green, 'REDIS', colors.reset, 
                `✅ Cleared ${totalDeleted} keys total. Ready for fresh data.`);

            return { success: true, totalDeleted };
        } catch (error) {
            log(colors.red, 'REDIS', colors.reset, `Error clearing app data: ${error.message}`);
            return { success: false, error: error.message };
        }
    }

    async clearAllAppData() {
        return this.clearAllBroker();
    }

    // ✅ OPTIMIZED: Check cache trước
    async getBroker(brokerName) {
        try {
            if (!brokerName) {
                throw new Error('Broker name is required');
            }

            // ✅ Check cache
            if (this.cache.brokerMap.has(brokerName)) {
                return this.cache.brokerMap.get(brokerName);
            }

            const brokerKey = `BROKER:${brokerName}`;
            const brokerData = await this.client.get(brokerKey);

            if (!brokerData) {
                return null;
            }

            const decompressed = await this.decompressData(brokerData);
            const parsed = JSON.parse(decompressed);
            
            // ✅ Cache result
            this.cache.brokerMap.set(brokerName, parsed);
            
            return parsed;
        } catch (error) {
            console.error(`Error getting broker '${brokerName}' from Redis:`, error);
            return null;
        }
    }

    // ✅ OPTIMIZED: Dùng cached brokers
    async getMultipleSymbolDetails(symbols) {
        if (!symbols || symbols.length === 0) return new Map();

        try {
            const brokers = await this.getAllBrokers(); // Đã có cache
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
            const key = `BROKER:${broker}`;

            const raw = await this.client.get(key);
            if (raw) {
                const decompressed = await this.decompressData(raw);
                const data = JSON.parse(decompressed);
                data.status = newStatus;
                data.timeUpdated = new Date().toISOString().slice(0, 19).replace('T', ' ');
                
                const compressed = await this.compressData(data);
                await this.client.set(key, compressed);
                this.invalidateCache();
                
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

    async saveAnalysis(data) {
        const key = `Analysis`;
        const compressed = await this.compressData(data);
        await this.client.set(key, compressed);
    }

    async getAnalysis() {
        const raw = await this.client.get('Analysis');
        if (!raw) {
            return {
                Type_1: [],
                Type_2: [],
                time_analysis: null
            };
        }

        try {
            const decompressed = await this.decompressData(raw);
            return JSON.parse(decompressed);
        } catch {
            return {
                Type_1: [],
                Type_2: [],
                time_analysis: null
            };
        }
    }

    async getBrokerResetting() {
        const brokers = await this.getAllBrokers();
        
        return brokers
            .filter(broker => broker.status !== "True")
            .sort((a, b) => Number(a.index) - Number(b.index));
    }

    async disconnect() {
        try {
            this.messageHandlers.clear();
            this.invalidateCache();
            await this.publisherClient.quit();
            await this.subscriberClient.quit();
            log(colors.yellow, 'REDIS', colors.reset, 'Disconnected gracefully');
        } catch (error) {
            console.error('Error disconnecting:', error);
        }
    }

    // ==================== RESET PROGRESS TRACKING ====================
    
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

    async updateResetProgress(brokerName, percentage) {
        try {
            const data = await this.client.get('reset_progress');
            if (!data) return false;
            
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

    async isResetting() {
        try {
            const exists = await this.client.exists('reset_progress');
            return exists === 1;
        } catch (error) {
            return false;
        }
    }

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

    async clearResetTracking() {
        try {
            await this.client.del('reset_progress');
            log(colors.green, 'REDIS', colors.reset, '✅ Cleared reset tracking');
        } catch (error) {
            console.error('Error clearing reset tracking:', error);
        }
    }
}

module.exports = new RedisManager();