const redisClient = require('./redisManager');
const Redis = require('ioredis');
const { log, colors } = require('../Helpers/Log');

class RedisManager {
    constructor() {
        this.client = redisClient;
        this.messageHandlers = new Map();  // ✅ Quản lý handlers
        this.isSubscriberSetup = false;    // ✅ Flag để tránh duplicate listeners

        // =========================
        // ✅ Performance caches
        // =========================
        this.BROKER_ZSET = 'BROKER:Z';

        // Cache brokers (parsed)
        this._brokersCache = null;
        this._brokersCacheAt = 0;
        this._brokersCacheTtlMs = 300; // ✅ realtime: 200-500ms là đẹp

        // Cache broker keys (sorted)
        this._brokerKeysCache = null;
        this._brokerKeysCacheAt = 0;
        this._brokerKeysCacheTtlMs = 1000;

        // Cache symbol index: symbol -> best broker(symbolInfo) by min index
        this._bestBySymbol = new Map();
        this._bestBySymbolAt = 0;
        this._bestBySymbolTtlMs = 300;

        // =========================
        // ✅ Redis connections
        // =========================
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

    // =========================
    // ✅ Event handlers
    // =========================
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

    // =========================
    // ✅ Pub/Sub
    // =========================
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

    // =========================
    // ✅ SCAN helper (giữ lại để dùng cho symbol:* / cleanup)
    // =========================
    async scanKeys(pattern) {
        const keys = [];
        let cursor = '0';

        do {
            const [newCursor, foundKeys] = await this.client.scan(
                cursor,
                'MATCH', pattern,
                'COUNT', 200
            );
            cursor = newCursor;
            keys.push(...foundKeys);
        } while (cursor !== '0');

        return keys;
    }

    // =========================
    // ✅ Internal performance helpers
    // =========================
    _now() { return Date.now(); }

    _normalizeIndex(index) {
        const n = parseInt(index, 10);
        return Number.isFinite(n) ? n : 0;
    }

    _invalidateCaches() {
        this._brokersCache = null;
        this._brokersCacheAt = 0;

        this._brokerKeysCache = null;
        this._brokerKeysCacheAt = 0;

        this._bestBySymbol.clear();
        this._bestBySymbolAt = 0;
    }

    async _ensureBrokerZsetBuilt() {
        try {
            const exists = await this.client.exists(this.BROKER_ZSET);
            if (exists === 1) return true;

            const keys = await this.scanKeys('BROKER:*');
            if (!keys.length) return true;

            const values = await this.client.mget(keys);
            const pipeline = this.client.pipeline();

            for (let i = 0; i < keys.length; i++) {
                const raw = values[i];
                if (!raw) continue;

                // Parse index nhanh bằng regex
                const match = raw.match(/"index"\s*:\s*"?(\d+)"?/);
                const idx = match ? parseInt(match[1], 10) : 0;
                pipeline.zadd(this.BROKER_ZSET, Number.isFinite(idx) ? idx : 0, keys[i]);
            }

            await pipeline.exec();
            return true;
        } catch (e) {
            console.error('Error ensure broker zset built:', e?.message || e);
            return false;
        }
    }

    async _zaddBrokerKey(brokerKey, index) {
        try {
            await this.client.zadd(this.BROKER_ZSET, this._normalizeIndex(index), brokerKey);
        } catch (e) {
            console.error('Error zadd broker key:', e?.message || e);
        }
    }

    async _zremBrokerKey(brokerKey) {
        try {
            await this.client.zrem(this.BROKER_ZSET, brokerKey);
        } catch (e) {
            console.error('Error zrem broker key:', e?.message || e);
        }
    }

    async _getBrokerKeysSorted() {
        const now = this._now();
        if (this._brokerKeysCache && (now - this._brokerKeysCacheAt) < this._brokerKeysCacheTtlMs) {
            return this._brokerKeysCache;
        }

        await this._ensureBrokerZsetBuilt();

        let keys = await this.client.zrange(this.BROKER_ZSET, 0, -1);
        if (!keys || keys.length === 0) {
            keys = await this.scanKeys('BROKER:*'); // fallback
        }

        this._brokerKeysCache = keys || [];
        this._brokerKeysCacheAt = now;
        return this._brokerKeysCache;
    }

    _rebuildBestBySymbol(brokers) {
        const map = new Map();

        for (const broker of brokers) {
            if (!broker) continue;
            const brokerIndexNum = this._normalizeIndex(broker.index);
            if (!broker.OHLC_Symbols || !Array.isArray(broker.OHLC_Symbols)) continue;
            if (broker.status === "Disconnect") continue;

            for (const info of broker.OHLC_Symbols) {
                if (!info || !info.symbol) continue;
                if (info.trade !== "TRUE") continue;

                const sym = info.symbol;
                const cur = map.get(sym);
                if (!cur || brokerIndexNum < cur.BrokerIndexNum) {
                    map.set(sym, {
                        ...info,
                        Broker: broker.broker,
                        BrokerIndex: broker.index,
                        BrokerIndexNum: brokerIndexNum
                    });
                }
            }
        }

        this._bestBySymbol = map;
        this._bestBySymbolAt = this._now();
    }

    // =========================
    // ✅ SAVE / UPDATE
    // =========================
    async saveBrokerData(broker, data) {
        const key = `BROKER:${broker}`;

        try {
            const expectedTotal = parseInt(data.totalsymbol || 0);

            const existingData = await this.client.get(key);

            if (!existingData) {
                await this.client.set(key, JSON.stringify(data));
                await this._zaddBrokerKey(key, data.index);
                this._invalidateCaches();
                return { success: true, action: 'created' };
            }

            const totalMatch = existingData.match(/"totalsymbol"\s*:\s*"(\d+)"/);
            const currentTotal = totalMatch ? parseInt(totalMatch[1]) : 0;

            if (currentTotal > expectedTotal) {
                await this.client.set(key, JSON.stringify(data));
                await this._zaddBrokerKey(key, data.index);
                this._invalidateCaches();
                return { success: true, action: 'reset', total: expectedTotal };
            }

            const parsedExisting = JSON.parse(existingData);

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

            const symbolMap = new Map(
                (parsedExisting.OHLC_Symbols || []).map(s => [s.symbol, s])
            );

            let stats = { added: 0, updated: 0 };

            data.OHLC_Symbols?.forEach(newSymbol => {
                if (!newSymbol || !newSymbol.symbol) return;
                if (symbolMap.has(newSymbol.symbol)) stats.updated++;
                else stats.added++;
                symbolMap.set(newSymbol.symbol, newSymbol);
            });

            parsedExisting.OHLC_Symbols = Array.from(symbolMap.values());
            parsedExisting.totalsymbol = symbolMap.size.toString();

            await this.client.set(key, JSON.stringify(parsedExisting));
            await this._zaddBrokerKey(key, parsedExisting.index);

            this._invalidateCaches();

            return { success: true, action: 'merged', ...stats, total: symbolMap.size };
        } catch (error) {
            throw error;
        }
    }

    async saveConfigAdmin(data) {
        const key = `CONFIG`;
        await this.client.set(key, JSON.stringify(data));
    }

    async getConfigAdmin() {
        const key = `CONFIG`;
        const raw = await this.client.get(key);
        if (raw) {
            try {
                return JSON.parse(raw);
            } catch (error) {
                console.error('Error parsing config admin data:', error);
            }
        }
        return null;
    }

    async getAllBrokers_2() {
        const keys = await this.scanKeys('BROKER:*');
        const result = {};

        if (keys.length === 0) return result;

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

            // ✅ update index
            pipeline.zadd(this.BROKER_ZSET, this._normalizeIndex(brokerData.index), brokerKey);

            await pipeline.exec();
            this._invalidateCaches();
            return true;
        } catch (error) {
            console.error('Error saving broker data:', error);
            return false;
        }
    }

    // =========================
    // ✅ FAST GET ALL BROKERS (tối ưu nhất)
    // =========================
    async getAllBrokers() {
        try {
            const now = this._now();
            if (this._brokersCache && (now - this._brokersCacheAt) < this._brokersCacheTtlMs) {
                return this._brokersCache;
            }

            const keys = await this._getBrokerKeysSorted();
            if (!keys.length) {
                this._brokersCache = [];
                this._brokersCacheAt = now;
                return [];
            }

            const values = await this.client.mget(keys);

            const brokers = [];
            for (const raw of values) {
                if (!raw) continue;
                try {
                    brokers.push(JSON.parse(raw));
                } catch (e) {
                    // skip invalid
                }
            }

            // ✅ Safety sort (tránh index thay đổi)
            brokers.sort((a, b) => (this._normalizeIndex(a.index) - this._normalizeIndex(b.index)));

            // ✅ update caches
            this._brokersCache = brokers;
            this._brokersCacheAt = now;

            // ✅ rebuild symbol index (phục vụ getSymbol cực nhanh)
            this._rebuildBestBySymbol(brokers);

            return brokers;
        } catch (error) {
            console.error('Error getting brokers from Redis:', error);
            return [];
        }
    }

    async getAllUniqueSymbols() {
        try {
            // ✅ nhanh nhất: dùng index đã build
            const now = this._now();
            if ((now - this._bestBySymbolAt) > this._bestBySymbolTtlMs) {
                await this.getAllBrokers(); // sẽ rebuild bestBySymbol
            }
            return Array.from(this._bestBySymbol.keys());
        } catch (error) {
            console.error('Error getting unique symbols from Redis:', error);
            return [];
        }
    }

    async getSymbolDetails(symbolName) {
        try {
            if (!symbolName) return [];

            // ✅ dùng cache brokers thay vì SCAN+MGET lại
            const brokers = await this.getAllBrokers();
            const symbolDetails = [];

            for (const broker of brokers) {
                try {
                    if (!broker?.OHLC_Symbols || !Array.isArray(broker.OHLC_Symbols)) continue;

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
                } catch { }
            }

            return symbolDetails.sort((a, b) => parseFloat(a.Index || 0) - parseFloat(b.Index || 0));
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

            const symbolKeys = await this.scanKeys(`symbol:${brokerName}:*`);
            const pipeline = this.client.pipeline();
            symbolKeys.forEach((key) => pipeline.del(key));
            pipeline.del(brokerKey);
            pipeline.zrem(this.BROKER_ZSET, brokerKey);

            await pipeline.exec();
            this._invalidateCaches();

            return { success: true, message: `Deleted broker "${brokerName}" and related symbols` };
        } catch (error) {
            console.error(`Error deleting broker ${brokerName}:`, error);
            return { success: false, message: `Error deleting broker: ${error.message}` };
        }
    }

    async clearData() {
        try {
            await this.client.flushall();
            this._invalidateCaches();
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
                    const batchSize = 200;
                    for (let i = 0; i < keys.length; i += batchSize) {
                        const batch = keys.slice(i, i + batchSize);
                        await this.client.del(...batch);
                    }

                    log(colors.yellow, 'REDIS', colors.reset, `Deleted ${keys.length} keys matching "${pattern}"`);
                    totalDeleted += keys.length;
                }
            }

            await this.client.del(this.BROKER_ZSET);
            this._invalidateCaches();

            log(colors.green, 'REDIS', colors.reset, `✅ Cleared ${totalDeleted} keys total. Ready for fresh data.`);
            return { success: true, totalDeleted };
        } catch (error) {
            log(colors.red, 'REDIS', colors.reset, `Error clearing app data: ${error.message}`);
            return { success: false, error: error.message };
        }
    }

    async clearAllAppData() {
        try {
            const patterns = ['BROKER:*', 'symbol:*', 'Analysis:*'];
            let totalDeleted = 0;

            for (const pattern of patterns) {
                const keys = await this.scanKeys(pattern);

                if (keys.length > 0) {
                    const batchSize = 200;
                    for (let i = 0; i < keys.length; i += batchSize) {
                        const batch = keys.slice(i, i + batchSize);
                        await this.client.del(...batch);
                    }

                    log(colors.yellow, 'REDIS', colors.reset, `Deleted ${keys.length} keys matching "${pattern}"`);
                    totalDeleted += keys.length;
                }
            }

            await this.client.del(this.BROKER_ZSET);
            this._invalidateCaches();

            log(colors.green, 'REDIS', colors.reset, `✅ Cleared ${totalDeleted} keys total. Ready for fresh data.`);
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
            // ✅ dùng cache brokers
            const brokers = await this.getAllBrokers();

            const symbolSet = new Set(symbols);
            const resultMap = new Map();
            for (const sym of symbols) resultMap.set(sym, []);

            for (const broker of brokers) {
                if (!broker?.OHLC_Symbols || !Array.isArray(broker.OHLC_Symbols)) continue;
                if (broker.status !== "True") continue;

                for (const symbolInfo of broker.OHLC_Symbols) {
                    const sym = symbolInfo?.symbol;
                    if (!sym) continue;

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

            // ✅ O(1) theo index: ZRANGEBYSCORE
            await this._ensureBrokerZsetBuilt();

            const target = this._normalizeIndex(index);
            const keys = await this.client.zrangebyscore(this.BROKER_ZSET, target, target, 'LIMIT', 0, 1);
            if (keys && keys[0]) {
                const raw = await this.client.get(keys[0]);
                return raw ? JSON.parse(raw) : null;
            }

            // fallback
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
                const data = JSON.parse(raw);
                data.status = newStatus;
                data.timeUpdated = new Date().toISOString().slice(0, 19).replace('T', ' ');
                await this.client.set(key, JSON.stringify(data));

                await this._zaddBrokerKey(key, data.index);
                this._invalidateCaches();

                return data;
            }
            return null;
        } catch (error) {
            console.error('Lỗi update status:', error.message);
            throw error;
        }
    }

    // =========================
    // ✅ FAST getSymbol (O(1) gần như tuyệt đối)
    // =========================
    async getSymbol(symbol) {
        try {
            if (!symbol) {
                throw new Error(symbol + ': Symbol is required');
            }

            const now = this._now();
            if ((now - this._bestBySymbolAt) > this._bestBySymbolTtlMs || this._bestBySymbol.size === 0) {
                await this.getAllBrokers(); // rebuild index
            }

            const r = this._bestBySymbol.get(symbol);
            if (!r) return null;

            // strip internal field
            const { BrokerIndexNum, ...clean } = r;
            return clean;
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
        await this.client.set(key, JSON.stringify(data));
    }

    async getAnalysis() {
        const raw = await this.client.get('Analysis');
        if (!raw) {
            return { Type_1: [], Type_2: [], time_analysis: null };
        }

        try {
            return JSON.parse(raw);
        } catch {
            return { Type_1: [], Type_2: [], time_analysis: null };
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
            await this.publisherClient.quit();
            await this.subscriberClient.quit();
            log(colors.yellow, 'REDIS', colors.reset, 'Disconnected gracefully');
        } catch (error) {
            console.error('Error disconnecting:', error);
        }
    }

    // ==================== RESET PROGRESS TRACKING (SIMPLE) ====================
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
                    log(colors.green, 'RESET', colors.reset, `✅ ${brokerName} completed: ${percentage}%`);
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

    // ==================== END RESET PROGRESS TRACKING ====================
}

module.exports = new RedisManager();
