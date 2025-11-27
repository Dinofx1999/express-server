const redisClient = require('./redisManager'); // Sử dụng redisManager để quản lý kết nối Redis
const Redis = require('ioredis');
const {log , colors} = require('../Helpers/Log');

class RedisManager {
    constructor() {
        this.client = redisClient;

        // Tạo hai client riêng biệt cho publish và subscribe
        this.publisherClient = new Redis({
            host: 'localhost',
            port: 6379,
        });

        this.subscriberClient = new Redis({
            host: 'localhost',
            port: 6379,
        });

        // Thiết lập các event handlers
        this.setupEventHandlers();
    }

    setupEventHandlers() {
        // Publisher events
        this.publisherClient.on('connect', () => {
            // console.log('Redis Publisher connected');
        });

        this.publisherClient.on('error', (err) => {
            console.error('Redis Publisher Error:', err);
        });

        // Subscriber events
        this.subscriberClient.on('connect', () => {
            // console.log('Redis Subscriber connected');
        });

        this.subscriberClient.on('error', (err) => {
            console.error('Redis Subscriber Error:', err);
        });
    }

    // Publish tin nhắn tới một channel
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

    // Đăng ký nhận tin từ channel
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

            log(colors.yellow, 'REDIS', colors.reset, `Subscribed to ${channel}`);
            
        } catch (error) {
            console.error('Subscribe error:', error);
        }
    }

    // Tiện ích parse JSON an toàn
    tryParseJSON(message) {
        try {
            return JSON.parse(message);
        } catch {
            return message;
        }
    }


    async saveBrokerData(broker, data) {
        const key = `broker:${broker}`;
        await this.client.set(key, JSON.stringify(data));
        }
    
    async  getAllBrokers_2() {
  const keys = await this.client.keys('broker:*');
  const result = {};
  for (const key of keys) {
    const raw = await this.client.get(key);
    try {
      result[key.replace('broker:', '')] = JSON.parse(raw);
    } catch {
      result[key.replace('broker:', '')] = raw;
    }
  }
  return result;
}
    // Lưu dữ liệu broker và symbol vào Redis
    async saveBrokerData_(data, port) {
        try {
            if (!data.Broker) {
                throw new Error('Invalid broker data: Missing Broker name');
            }

            const brokerKey = `broker:${data.Broker}`;
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

    // Lấy thông tin tất cả các Broker từ Redis
    async getAllBrokers() {
        try {
            const keys = await this.client.keys('broker:*');
            const brokers = await Promise.all(keys.map((key) => this.client.get(key)));
            
            // Parse tất cả các broker và lọc bỏ các giá trị không hợp lệ
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
            
            // Sắp xếp theo Index (chuyển đổi sang số để so sánh)
            return validBrokers.sort((a, b) => {
                // Chuyển Index sang số để so sánh
                const indexA = parseInt(a.index, 10) || 0;
                const indexB = parseInt(b.index, 10) || 0;
                
                return indexA - indexB; // Sắp xếp tăng dần
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
    // Lấy thông tin chi tiết của một symbol
    async getSymbolDetails(symbolName) {
        try {
            const brokerKeys = await this.client.keys('broker:*');
            if (brokerKeys.length === 0) return [];

            const pipeline = this.client.pipeline();
            brokerKeys.forEach((key) => pipeline.get(key));
            const results = await pipeline.exec();

            const symbolDetails = [];
            for (const [err, data] of results) {
                if (!err && data) {
                    const broker = JSON.parse(data);
                    if (broker.Infosymbol && Array.isArray(broker.Infosymbol)) {
                        const symbolInfo = broker.Infosymbol.find((sym) => sym.Symbol === symbolName && sym.TimeSymbol !== 'Close Trade');
                        if (symbolInfo) {
                           
                            symbolDetails.push({
                                Broker: broker.Broker,
                                Index: broker.Index,
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

    // Xóa một broker và các symbol liên quan
    async deleteBroker(brokerName) {
        try {
            if (!brokerName) throw new Error('Broker name is required');

            const brokerKey = `broker:${brokerName}`;
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

    // Xóa toàn bộ dữ liệu Redis
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
            
            // Lấy trực tiếp broker với key chính xác
            const brokerKey = `broker:${brokerName}`;
            const brokerData = await this.client.get(brokerKey);
            
            if (!brokerData) {
                return null;  // Broker không tồn tại
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
            // Lua script để tìm broker theo index
            const luaScript = `
                local keys = redis.call('KEYS', 'broker:*')
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
            `;
            
            // Thực thi Lua script
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
    // console.log(`Updating broker: ${broker}, newStatus: ${newStatus}`);
    const key = `broker:${broker}`; // ← Key phải match với getPortBroker
    
    const raw = await this.client.get(key);
    if (raw) {
      const data = JSON.parse(raw);
    
    // Update status
    data.status = newStatus; // "True" hoặc "False"
    data.timeUpdated = new Date().toISOString().slice(0, 19).replace('T', ' ');
    
    // Lưu lại
    await this.client.set(key, JSON.stringify(data));
    
    // console.log(`✓ Đã update ${broker} status thành: ${newStatus}`);
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
                 throw new Error( symbol + ': Symbol is required');
            }
            
            // Lấy tất cả các broker
            const brokers = await this.getAllBrokers();
            
            let result = null;
            let minIndex = Number.MAX_SAFE_INTEGER;
            
           
            // Duyệt qua từng broker
            for (const broker of brokers) {
                const brokerIndex = parseInt(broker.index, 10);
                
                if (!broker.OHLC_Symbols || isNaN(brokerIndex)) {
                    continue;
                }
                
                // Tìm symbol trong danh sách
                const symbolInfo = broker.OHLC_Symbols.find(info => info.symbol === symbol);
                
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
    async updateBrokerStatus(brokerName, status, statusDetail = '') {
        try {
            if (!brokerName) {
                throw new Error('Broker name is required');
            }
            
            // Tạo key cho broker
            const brokerKey = `broker:${brokerName}`;
            
            // Lấy dữ liệu broker hiện tại
            const brokerData = await this.client.get(brokerKey);
            
            if (brokerData) {
                // Parse JSON
            const broker = JSON.parse(brokerData);
            
            // Cập nhật trạng thái
            broker.Status = status;
            
            // Cập nhật chi tiết trạng thái nếu có
            if (statusDetail) {
                broker.TimeCurrent = statusDetail;
            }
            const now = new Date();
            const formattedDate = `${now.getFullYear()}.${String(now.getMonth() + 1).padStart(2, '0')}.${String(now.getDate()).padStart(2, '0')} ${String(now.getHours()).padStart(2, '0')}:${String(now.getMinutes()).padStart(2, '0')}:${String(now.getSeconds()).padStart(2, '0')}`;
            // Cập nhật thời gian
            broker.lastUpdated = formattedDate;
            
            // Lưu lại vào Redis
            await this.client.set(brokerKey, JSON.stringify(broker));
            
            return true;
            }
            return false; // Broker không tồn tại
            
            
        } catch (error) {
            console.error(`Error updating status for broker '${brokerName}':`, error);
            throw error;
        }
    }

    async Broker_names() {
        try {
            const data = await this.getAllBrokers();
            const brokerNames = data.map(broker => broker.Broker);
            return brokerNames;
        } catch (error) {
                
        }
    }

    
}

module.exports = new RedisManager();