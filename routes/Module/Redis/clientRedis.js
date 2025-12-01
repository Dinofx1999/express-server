const redisClient = require('./redisManager');
const Redis = require('ioredis');
const {log , colors} = require('../Helpers/Log');

class RedisManager {
    constructor() {
        this.client = redisClient;

        this.publisherClient = new Redis({
            host: 'localhost',
            port: 6379,
        });

        this.subscriberClient = new Redis({
            host: 'localhost',
            port: 6379,
        });

        this.setupEventHandlers();
    }

    setupEventHandlers() {
        this.publisherClient.on('connect', () => {});
        this.publisherClient.on('error', (err) => {
            console.error('Redis Publisher Error:', err);
        });
        this.subscriberClient.on('connect', () => {});
        this.subscriberClient.on('error', (err) => {
            console.error('Redis Subscriber Error:', err);
        });
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

    subscribe(channel, callback) {
        try {
            log(colors.yellow, 'REDIS', colors.reset, `Subscribing to ${channel}`);
            this.subscriberClient.subscribe(channel);
            this.subscriberClient.on('message', (receivedChannel, message) => {
                if (receivedChannel === channel) {
                    try {
                        const parsedMessage = this.tryParseJSON(message);
                        callback(parsedMessage);
                    } catch (error) {
                        log(colors.red, 'REDIS', colors.reset, `Message processing error: ${error.message}`);
                    }
                }
            });
        } catch (error) {
            console.error('Subscribe error:', error);
        }
    }

    tryParseJSON(message) {
        try {
            return JSON.parse(message);
        } catch {
            return message;
        }
    }

    async saveBrokerData(broker, data) {
        const key = `BROKER:${broker}`;  // ✅ Changed
        await this.client.set(key, JSON.stringify(data));
    }
    
    async getAllBrokers_2() {
        const keys = await this.client.keys('BROKER:*');  // ✅ Changed
        const result = {};
        for (const key of keys) {
            const raw = await this.client.get(key);
            try {
                result[key.replace('BROKER:', '')] = JSON.parse(raw);  // ✅ Changed
            } catch {
                result[key.replace('BROKER:', '')] = raw;  // ✅ Changed
            }
        }
        return result;
    }

    async saveBrokerData_(data, port) {
        try {
            if (!data.Broker) {
                throw new Error('Invalid broker data: Missing Broker name');
            }

            const brokerKey = `BROKER:${data.Broker}`;  // ✅ Changed
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

    async getAllBrokers() {
        try {
            const keys = await this.client.keys('BROKER:*');  // ✅ Changed
            const brokers = await Promise.all(keys.map((key) => this.client.get(key)));
            
            const validBrokers = brokers
                .map(broker => {
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

    async getSymbolDetails(symbolName) {
        try {
            const brokerKeys = await this.client.keys('BROKER:*');  // ✅ Changed
            if (brokerKeys.length === 0) return [];

            const pipeline = this.client.pipeline();
            brokerKeys.forEach((key) => pipeline.get(key));
            const results = await pipeline.exec();

            const symbolDetails = [];
            for (const [err, data] of results) {
                if (!err && data) {
                    const broker = JSON.parse(data);
                    if (broker.OHLC_Symbols && Array.isArray(broker.OHLC_Symbols)) {
                        const symbolInfo = broker.OHLC_Symbols.find((sym) => sym.symbol === symbolName && sym.trade == "TRUE" && broker.status === "True");
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

            const brokerKey = `BROKER:${brokerName}`;  // ✅ Changed
            const brokerExists = await this.client.exists(brokerKey);
            if (!brokerExists) return { success: false, message: `Broker "${brokerName}" does not exist` };

            const symbolKeys = await this.client.keys(`symbol:${brokerName}:*`);
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

    async getBroker(brokerName) {
        try {
            if (!brokerName) {
                throw new Error('Broker name is required');
            }
            
            const brokerKey = `BROKER:${brokerName}`;  // ✅ Changed
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

    async findBrokerByIndex(index) {
        try {
            if (!index && index !== 0) {
                throw new Error('index is required');
            }
            
            const luaScript = `
                local keys = redis.call('KEYS', 'BROKER:*')
                local targetindex = ARGV[1]
                
                for _, key in ipairs(keys) do
                    local data = redis.call('GET', key)
                    if data then
                        local success, broker = pcall(function() return cjson.decode(data) end)
                        if success and broker and tostring(broker.index) == tostring(targetindex) then
                            return data
                        end
                    end
                end
                
                return nil
            `;  // ✅ Changed KEYS pattern
            
            const result = await this.client.eval(luaScript, 0, index.toString());
            
            if (!result) {
                return null;
            }
            
            return JSON.parse(result);
        } catch (error) {
            console.error(`Error finding broker with index '${index}':`, error);
            return null;
        }
    }

    async updateBrokerStatus(broker, newStatus) {
        try {
            console.log(`Updating status for broker: ${broker} to ${newStatus}`);
            const key = `BROKER:${broker}`;  // ✅ Changed
            
            const raw = await this.client.get(key);
            if (raw) {
                const data = JSON.parse(raw);
                data.status = newStatus;
                data.timeUpdated = new Date().toISOString().slice(0, 19).replace('T', ' ');
                await this.client.set(key, JSON.stringify(data));
                return data;
            }
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
                
                const symbolInfo = broker.OHLC_Symbols.find(info => info.symbol === symbol && info.trade === "TRUE" && broker.status !== "Disconnect");
                
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
        }
    }

    async saveAnalysis(Analysis, data) {
        const key = `Analysis:${Analysis}`;
        await this.client.set(key, JSON.stringify(data));
    }

    async getAnalysis() {
        const keys = await this.client.keys('Analysis:*');
        const result = {};
        for (const key of keys) {
            const raw = await this.client.get(key);
            try {
                result[key.replace('Analysis:', '')] = JSON.parse(raw);
            } catch {
                result[key.replace('Analysis:', '')] = raw;
            }
        }
        return result;
    }

    async getBrokerResetting() {
        const keys = await this.client.keys('BROKER:*');  // ✅ Changed
        if (!keys.length) return [];
        
        const pipeline = this.client.pipeline();
        keys.forEach(k => pipeline.get(k));
        const results = await pipeline.exec();
        
        return results
            .map(([, raw]) => {
                try {
                    return JSON.parse(raw);
                } catch {
                    return null;
                }
            })
            .filter(Boolean)
            .filter(broker => broker.status !== "True")
            .sort((a, b) => Number(a.index) - Number(b.index));
    }
}

module.exports = new RedisManager();