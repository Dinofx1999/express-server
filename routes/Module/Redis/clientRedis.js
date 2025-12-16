'use strict';

const Redis = require('ioredis');
const zlib = require('zlib');
const { log, colors } = require('../Helpers/Log');

// ===================== COMPRESSION HELPERS =====================
// Magic header để phân biệt dữ liệu nén vs JSON string cũ
const MAGIC = Buffer.from('BR1'); // 3 bytes

function packJSON(obj) {
  const jsonBuf = Buffer.from(JSON.stringify(obj));
  const compressed = zlib.brotliCompressSync(jsonBuf, {
    params: { [zlib.constants.BROTLI_PARAM_QUALITY]: 4 }, // ưu tiên tốc độ
  });
  return Buffer.concat([MAGIC, compressed]);
}

function unpackJSON(buf) {
  // 1) New format: MAGIC + brotli
  if (Buffer.isBuffer(buf) && buf.length > 3 && buf.subarray(0, 3).equals(MAGIC)) {
    const compressed = buf.subarray(3);
    const raw = zlib.brotliDecompressSync(compressed);
    return JSON.parse(raw.toString());
  }

  // 2) Old format: plain JSON string stored in Redis
  const text = Buffer.isBuffer(buf) ? buf.toString() : String(buf);
  return JSON.parse(text);
}

class RedisManager {
  constructor() {
    this.messageHandlers = new Map(); // channel -> handler
    this.isSubscriberSetup = false;

    const redisConfig = {
      host: 'localhost',
      port: 6379,
      db: 0,

      // ⚡ hiệu năng
      enableAutoPipelining: true,
      maxRetriesPerRequest: null,
      keepAlive: 10000,
      retryStrategy: (times) => Math.min(times * 50, 2000),
    };

    // ✅ 1 client chính
    this.client = new Redis(redisConfig);

    // ✅ Pub/Sub phải tách connection
    this.publisherClient = this.client.duplicate();
    this.subscriberClient = this.client.duplicate();

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

    // ✅ message handler 1 lần duy nhất
    this.subscriberClient.on('message', (channel, message) => {
      const handler = this.messageHandlers.get(channel);
      if (!handler) return;

      try {
        handler(this.tryParseJSON(message));
      } catch (error) {
        log(colors.red, 'REDIS', colors.reset, `Message processing error: ${error.message}`);
      }
    });

    this.isSubscriberSetup = true;
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
      return await this.publisherClient.publish(channel, payload);
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

  // ===================== SCAN (FAST + SAFE) =====================
  async scanKeys(pattern) {
    const keys = [];
    const stream = this.client.scanStream({ match: pattern, count: 2000 });

    return await new Promise((resolve, reject) => {
      stream.on('data', (resultKeys) => {
        for (const k of resultKeys) keys.push(k);
      });
      stream.on('end', () => resolve(keys));
      stream.on('error', reject);
    });
  }

  // ===================== INTERNAL BROKER READ/WRITE =====================
  async _getBrokerObjectByKey(key) {
    const buf = await this.client.getBuffer(key);
    if (!buf) return null;
    try {
      return unpackJSON(buf);
    } catch (e) {
      // fallback: nếu là string plain nhưng getBuffer vẫn trả Buffer
      try {
        return JSON.parse(buf.toString());
      } catch {
        return null;
      }
    }
  }

  async _setBrokerObjectByKey(key, obj) {
    const payload = packJSON(obj);
    await this.client.set(key, payload);
  }

  // ===================== FAST SAVE (FULL SNAPSHOT) =====================
  /**
   * FULL snapshot => SET thẳng (không merge)
   * Không HASH.
   * Có nén Brotli.
   */
  async saveBrokerData(broker, data) {
    const key = `BROKER:${broker}`;

    try {
      await this._setBrokerObjectByKey(key, data);

      return {
        success: true,
        action: 'set_full',
        total: data?.OHLC_Symbols?.length || parseInt(data?.totalsymbol || 0, 10) || 0,
      };
    } catch (error) {
      throw error;
    }
  }

  // (giữ như bạn – nhưng không khuyên dùng cho full snapshot)
  async saveBrokerData_(data, port) {
    try {
      if (!data.Broker) {
        throw new Error('Invalid broker data: Missing Broker name');
      }

      const brokerKey = `BROKER:${data.Broker}`;
      const now = new Date();
      const formattedDate =
        `${now.getFullYear()}.${String(now.getMonth() + 1).padStart(2, '0')}.${String(now.getDate()).padStart(2, '0')} ` +
        `${String(now.getHours()).padStart(2, '0')}:${String(now.getMinutes()).padStart(2, '0')}:${String(now.getSeconds()).padStart(2, '0')}`;

      const brokerData = {
        ...data,
        port: port,
        lastUpdated: formattedDate,
      };

      const pipeline = this.client.pipeline();
      // ở hàm này bạn đang lưu string JSON (legacy). Vẫn ok.
      pipeline.set(brokerKey, JSON.stringify(brokerData));

      if (data.Infosymbol && Array.isArray(data.Infosymbol)) {
        for (const symbol of data.Infosymbol) {
          const symbolKey = `symbol:${data.Broker}:${symbol.Symbol}`;
          pipeline.set(
            symbolKey,
            JSON.stringify({
              ...symbol,
              broker: data.Broker,
              lastUpdated: brokerData.lastUpdated,
            })
          );
        }
      }

      await pipeline.exec();
      return true;
    } catch (error) {
      console.error('Error saving broker data:', error);
      return false;
    }
  }

  // ===================== CONFIG =====================
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

  // ===================== READ BROKERS =====================
  async getAllBrokers_2() {
    const keys = await this.scanKeys('BROKER:*');
    const result = {};
    if (keys.length === 0) return result;

    const bufs = await this.client.mgetBuffer(keys);

    keys.forEach((key, index) => {
      const buf = bufs[index];
      if (!buf) return;

      let data = null;
      try {
        data = unpackJSON(buf);
      } catch {
        try {
          data = JSON.parse(buf.toString());
        } catch {
          data = null;
        }
      }

      if (data) result[key.replace('BROKER:', '')] = data;
    });

    return result;
  }

  async getAllBrokers() {
    try {
      const keys = await this.scanKeys('BROKER:*');
      if (keys.length === 0) return [];

      const bufs = await this.client.mgetBuffer(keys);

      const validBrokers = bufs
        .map((buf) => {
          if (!buf) return null;
          try {
            return unpackJSON(buf);
          } catch {
            try {
              return JSON.parse(buf.toString());
            } catch {
              return null;
            }
          }
        })
        .filter((b) => b !== null);

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

  async getBroker(brokerName) {
    try {
      if (!brokerName) throw new Error('Broker name is required');
      const brokerKey = `BROKER:${brokerName}`;
      return await this._getBrokerObjectByKey(brokerKey);
    } catch (error) {
      console.error(`Error getting broker '${brokerName}' from Redis:`, error);
      return null;
    }
  }

  async getBrokerResetting() {
    const brokers = await this.getAllBrokers();
    return brokers
      .filter((broker) => String(broker.status || '').trim() !== 'True')
      .sort((a, b) => Number(a.index) - Number(b.index));
  }

  async getAllUniqueSymbols() {
    try {
      const brokers = await this.getAllBrokers();
      const uniqueSymbols = new Set();

      brokers.forEach((broker) => {
        if (broker.OHLC_Symbols && Array.isArray(broker.OHLC_Symbols)) {
          broker.OHLC_Symbols.forEach((symbolData) => {
            if (symbolData.symbol) uniqueSymbols.add(symbolData.symbol);
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
      const brokerKeys = await this.scanKeys('BROKER:*');
      if (brokerKeys.length === 0) return [];

      const bufs = await this.client.mgetBuffer(brokerKeys);
      const symbolDetails = [];

      bufs.forEach((buf) => {
        if (!buf) return;

        let broker = null;
        try {
          broker = unpackJSON(buf);
        } catch {
          try {
            broker = JSON.parse(buf.toString());
          } catch {
            broker = null;
          }
        }
        if (!broker) return;

        if (broker.OHLC_Symbols && Array.isArray(broker.OHLC_Symbols)) {
          const symbolInfo = broker.OHLC_Symbols.find(
            (sym) => sym.symbol === symbolName && sym.trade === 'TRUE' && broker.status === 'True'
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
      });

      return symbolDetails.sort((a, b) => parseFloat(a.Index || 0) - parseFloat(b.Index || 0));
    } catch (error) {
      console.error(`Error getting symbol details for ${symbolName}:`, error);
      return [];
    }
  }

  async getMultipleSymbolDetails(symbols) {
    if (!symbols || symbols.length === 0) return new Map();

    try {
      const brokerKeys = await this.scanKeys('BROKER:*');
      if (brokerKeys.length === 0) return new Map();

      const bufs = await this.client.mgetBuffer(brokerKeys);
      const brokers = [];

      bufs.forEach((buf) => {
        if (!buf) return;
        try {
          brokers.push(unpackJSON(buf));
        } catch {
          try {
            brokers.push(JSON.parse(buf.toString()));
          } catch {}
        }
      });

      const symbolSet = new Set(symbols);
      const resultMap = new Map();
      for (const sym of symbols) resultMap.set(sym, []);

      for (const broker of brokers) {
        if (!broker?.OHLC_Symbols || !Array.isArray(broker.OHLC_Symbols)) continue;
        if (broker.status !== 'True') continue;

        for (const symbolInfo of broker.OHLC_Symbols) {
          const sym = symbolInfo.symbol;
          if (!symbolSet.has(sym)) continue;
          if (symbolInfo.trade !== 'TRUE') continue;

          resultMap.get(sym).push({
            Broker: broker.broker,
            Broker_: broker.broker_,
            Status: broker.status,
            Index: broker.index,
            Auto_Trade: broker.auto_trade,
            Typeaccount: broker.typeaccount,
            ...symbolInfo,
          });
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
      if (index === undefined || index === null) throw new Error('index is required');
      const brokers = await this.getAllBrokers();
      const targetIndex = String(index);
      return brokers.find((broker) => String(broker.index) === targetIndex) || null;
    } catch (error) {
      console.error(`Error finding broker with index '${index}':`, error);
      return null;
    }
  }

  async updateBrokerStatus(broker, newStatus) {
    try {
      const key = `BROKER:${broker}`;
      const data = await this._getBrokerObjectByKey(key);
      if (!data) return null;

      data.status = newStatus;
      data.timeUpdated = new Date().toISOString().slice(0, 19).replace('T', ' ');
      await this._setBrokerObjectByKey(key, data);

      return data;
    } catch (error) {
      console.error('Lỗi update status:', error.message);
      throw error;
    }
  }

  async getSymbol(symbol) {
    try {
      if (!symbol) throw new Error(symbol + ': Symbol is required');

      const brokers = await this.getAllBrokers();
      let result = null;
      let minIndex = Number.MAX_SAFE_INTEGER;

      for (const broker of brokers) {
        const brokerIndex = parseInt(broker.index, 10);
        if (!broker.OHLC_Symbols || isNaN(brokerIndex)) continue;

        const symbolInfo = broker.OHLC_Symbols.find(
          (info) => info.symbol === symbol && info.trade === 'TRUE' && broker.status !== 'Disconnect'
        );

        if (symbolInfo && brokerIndex < minIndex) {
          minIndex = brokerIndex;
          result = {
            ...symbolInfo,
            Broker: broker.broker,
            BrokerIndex: broker.index,
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
      const brokers = data.map((broker) => {
        const { OHLC_Symbols, ...info } = broker;
        return info;
      });
      return brokers;
    } catch (error) {
      console.error('Lỗi hàm Broker_names trong clientRedis.js', error);
      return [];
    }
  }

  async deleteBroker(brokerName) {
    try {
      if (!brokerName) throw new Error('Broker name is required');

      const brokerKey = `BROKER:${brokerName}`;
      const brokerExists = await this.client.exists(brokerKey);
      if (!brokerExists) return { success: false, message: `Broker "${brokerName}" does not exist` };

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

  async clearAllBroker() {
    return this.clearAllAppData();
  }

  async clearAllAppData() {
    try {
      const patterns = ['BROKER:*', 'symbol:*', 'Analysis:*'];
      let totalDeleted = 0;

      for (const pattern of patterns) {
        const keys = await this.scanKeys(pattern);
        if (keys.length > 0) {
          const batchSize = 1000;
          for (let i = 0; i < keys.length; i += batchSize) {
            const batch = keys.slice(i, i + batchSize);
            await this.client.del(...batch);
          }

          log(colors.yellow, 'REDIS', colors.reset, `Deleted ${keys.length} keys matching "${pattern}"`);
          totalDeleted += keys.length;
        }
      }

      log(colors.green, 'REDIS', colors.reset, `✅ Cleared ${totalDeleted} keys total. Ready for fresh data.`);
      return { success: true, totalDeleted };
    } catch (error) {
      log(colors.red, 'REDIS', colors.reset, `Error clearing app data: ${error.message}`);
      return { success: false, error: error.message };
    }
  }

  // ===================== Analysis =====================
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

  // ===================== RESET PROGRESS TRACKING =====================
  async startResetTracking(brokers) {
    try {
      const data = {
        brokers: brokers.map((b) => ({
          name: b.broker_ || b.broker,
          percentage: 0,
          completed: false,
        })),
        currentIndex: 0,
        startedAt: Date.now(),
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
      const broker = progress.brokers.find((b) => b.name === brokerName);

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
      const broker = progress.brokers.find((b) => b.name === brokerName);
      return broker ? broker.completed : false;
    } catch {
      return false;
    }
  }

  async isResetting() {
    try {
      const exists = await this.client.exists('reset_progress');
      return exists === 1;
    } catch {
      return false;
    }
  }

  async getResetStatus() {
    try {
      const data = await this.client.get('reset_progress');
      if (!data) return null;

      const progress = JSON.parse(data);
      const completed = progress.brokers.filter((b) => b.completed).length;
      const total = progress.brokers.length;

      return {
        isRunning: true,
        progress: `${completed}/${total}`,
        percentage: Math.round((completed / total) * 100),
        startedAt: progress.startedAt,
        brokers: progress.brokers,
      };
    } catch {
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

  // ===================== GRACEFUL SHUTDOWN =====================
  async disconnect() {
    try {
      this.messageHandlers.clear();
      await this.publisherClient.quit();
      await this.subscriberClient.quit();
      await this.client.quit();
      log(colors.yellow, 'REDIS', colors.reset, 'Disconnected gracefully');
    } catch (error) {
      console.error('Error disconnecting:', error);
    }
  }
}

module.exports = new RedisManager();
