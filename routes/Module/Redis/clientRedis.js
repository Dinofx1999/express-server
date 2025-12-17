const redisClient = require('./redisManager');
const Redis = require('ioredis');
const { log, colors } = require('../Helpers/Log');
const zlib = require('zlib');
const { promisify } = require('util');

const gzip = promisify(zlib.gzip);
const gunzip = promisify(zlib.gunzip);

class RedisManager {
  constructor() {
    this.client = redisClient;
    this.messageHandlers = new Map();
    this.isSubscriberSetup = false;

    // ✅ AGGRESSIVE CACHING
    this._cache = {
      brokerKeys: { at: 0, ttl: 1000, value: null },
      allBrokers: { at: 0, ttl: 50, value: null }, // ✅ 50ms cache cực ngắn
      brokerData: new Map(), // broker name -> { at, ttl, value }
    };

    // ✅ In-memory diff tracking để skip unchanged data
    this._lastHashes = new Map(); // broker -> hash of data

    const redisConfig = {
      host: 'localhost',
      port: 6379,
      lazyConnect: true,
      enableAutoPipelining: true,
      enableOfflineQueue: false, // ✅ Fail fast
      connectTimeout: 5000,
      keepAlive: 1,
      noDelay: true,
      retryStrategy: (times) => {
        if (times > 3) return null; // ✅ Fail after 3 retries
        return Math.min(times * 50, 500);
      },
      maxRetriesPerRequest: 2,
      reconnectOnError: (err) => {
        const msg = err?.message || '';
        return msg.includes('READONLY') || msg.includes('ECONNRESET');
      },
    };

    this.publisherClient = new Redis(redisConfig);
    this.subscriberClient = new Redis(redisConfig);

    this.setupEventHandlers();
  }

  _now() {
    return Date.now();
  }

  _invalidateBrokerCache() {
    this._cache.brokerKeys.value = null;
    this._cache.brokerKeys.at = 0;
    this._cache.allBrokers.value = null;
    this._cache.allBrokers.at = 0;
    this._cache.brokerData.clear();
  }

  _getCache(bucket) {
    const c = this._cache[bucket];
    if (!c) return null;
    const now = this._now();
    if (c.value && now - c.at < c.ttl) return c.value;
    return null;
  }

  _setCache(bucket, value) {
    const c = this._cache[bucket];
    if (!c) return;
    c.value = value;
    c.at = this._now();
  }

  _getBrokerCache(broker) {
    const cached = this._cache.brokerData.get(broker);
    if (!cached) return null;
    if (this._now() - cached.at < cached.ttl) return cached.value;
    return null;
  }

  _setBrokerCache(broker, value, ttl = 100) {
    this._cache.brokerData.set(broker, {
      value,
      at: this._now(),
      ttl
    });
  }

  _maybeJsonParse(raw) {
    if (raw == null) return null;
    if (typeof raw !== 'string') return raw;
    const first = raw[0];
    if (first !== '{' && first !== '[' && first !== '"') return raw;
    try {
      return JSON.parse(raw);
    } catch {
      return raw;
    }
  }

  // ✅ FAST HASH để detect changes
  _fastHash(str) {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
      const char = str.charCodeAt(i);
      hash = ((hash << 5) - hash) + char;
      hash = hash & hash;
    }
    return hash;
  }

  async _deleteKeys(keys) {
    if (!keys || keys.length === 0) return 0;
    const batchSize = 1000; // ✅ Tăng batch size
    let deleted = 0;
    for (let i = 0; i < keys.length; i += batchSize) {
      const batch = keys.slice(i, i + batchSize);
      try {
        const res = await this.client.unlink(...batch);
        deleted += Number(res || 0);
      } catch (e) {
        const res = await this.client.del(...batch);
        deleted += Number(res || 0);
      }
    }
    return deleted;
  }

  async _getBrokerKeysCached() {
    const cached = this._getCache('brokerKeys');
    if (cached) return cached;
    const keys = await this.scanKeys('BROKER:*');
    this._setCache('brokerKeys', keys);
    return keys;
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
      if (!handler) return;
      try {
        const parsedMessage = this._maybeJsonParse(message);
        handler(parsedMessage);
      } catch (error) {
        log(colors.red, 'REDIS', colors.reset, `Message error: ${error.message}`);
      }
    });

    this.isSubscriberSetup = true;
  }

  subscribe(channel, callback) {
    try {
      this.messageHandlers.set(channel, callback);
      return this.subscriberClient.subscribe(channel);
    } catch (error) {
      console.error('Subscribe error:', error);
      return Promise.resolve(0);
    }
  }

  unsubscribe(channel) {
    try {
      this.messageHandlers.delete(channel);
      return this.subscriberClient.unsubscribe(channel);
    } catch (error) {
      console.error('Unsubscribe error:', error);
      return Promise.resolve(0);
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

  async scanKeys(pattern) {
    const keys = [];
    let cursor = '0';
    do {
      const [newCursor, foundKeys] = await this.client.scan(
        cursor,
        'MATCH', pattern,
        'COUNT', 2000 // ✅ Tăng COUNT
      );
      cursor = newCursor;
      if (foundKeys && foundKeys.length) keys.push(...foundKeys);
    } while (cursor !== '0');
    return keys;
  }

  // ================================================================
  // ✅ MEGA OPTIMIZED: Hash + Compression + Smart Caching
  // ================================================================

  /**
   * MAIN ENTRY - Xử lý thông minh data từ MT4
   */
  async saveBrokerData(broker, data) {
    const startTime = Date.now();

    try {
      // ✅ QUICK HASH CHECK - Skip nếu data không đổi
      const dataStr = JSON.stringify(data);
      const dataHash = this._fastHash(dataStr);
      const lastHash = this._lastHashes.get(broker);

      if (lastHash === dataHash) {
        // Data không đổi, skip
        return { 
          success: true, 
          action: 'skipped_unchanged',
          elapsed: Date.now() - startTime 
        };
      }

      // ✅ Update hash
      this._lastHashes.set(broker, dataHash);

      // ✅ STRATEGY: Hash storage + Compression cho symbols
      const result = await this._saveBrokerOptimized(broker, data);
      
      const elapsed = Date.now() - startTime;
      
      if (elapsed > 50) {
        log(colors.yellow, 'REDIS', colors.reset, 
            `⚠️ ${broker}: Save took ${elapsed}ms`);
      }

      return { ...result, elapsed };

    } catch (error) {
      console.error('Error in saveBrokerData:', error);
      throw error;
    }
  }

  /**
   * OPTIMIZED SAVE: Hash + Compression
   */
  async _saveBrokerOptimized(broker, data) {
    const metaKey = `BROKER:${broker}`;
    const symbolsKey = `BROKER:${broker}:symbols:gz`;

    const pipeline = this.client.pipeline();

    // ✅ METADATA (small, uncompressed)
    const metadata = {
      port: data.port || '',
      index: data.index || '',
      broker: data.broker || broker,
      broker_: data.broker_ || '',
      version: data.version || '',
      typeaccount: data.typeaccount || '',
      timecurent: data.timecurent || '',
      auto_trade: data.auto_trade || '',
      status: data.status || '',
      timeUpdated: data.timeUpdated || '',
      totalsymbol: data.totalsymbol || '0',
      batch: data.batch || '',
      totalBatches: data.totalBatches || ''
    };

    pipeline.set(metaKey, JSON.stringify(metadata));

    // ✅ SYMBOLS: Compress toàn bộ array
    const symbols = data.OHLC_Symbols || [];
    if (symbols.length > 0) {
      const symbolsJson = JSON.stringify(symbols);
      const originalSize = Buffer.byteLength(symbolsJson);
      
      // Compress
      const compressed = await gzip(Buffer.from(symbolsJson), {
        level: 6 // ✅ Balance speed vs ratio
      });
      
      const compressedSize = compressed.length;
      const ratio = Math.round((1 - compressedSize / originalSize) * 100);

      pipeline.setBuffer(symbolsKey, compressed);
      pipeline.expire(symbolsKey, 3600); // ✅ TTL 1 hour

      // Log nếu compression kém
      if (ratio < 50) {
        log(colors.yellow, 'REDIS', colors.reset,
            `⚠️ ${broker}: Low compression ${ratio}%`);
      }
    }

    await pipeline.exec();
    
    // ✅ Invalidate cache
    this._invalidateBrokerCache();
    this._setBrokerCache(broker, null); // Clear broker cache

    return { 
      success: true, 
      action: 'saved_compressed', 
      symbols: symbols.length 
    };
  }

  /**
   * GET SINGLE BROKER - với aggressive caching
   */
  async getBroker(brokerName) {
    try {
      if (!brokerName) throw new Error('Broker name required');

      // ✅ Check cache
      const cached = this._getBrokerCache(brokerName);
      if (cached) return cached;

      const metaKey = `BROKER:${brokerName}`;
      const symbolsKey = `BROKER:${brokerName}:symbols:gz`;

      // ✅ Pipeline get
      const pipeline = this.client.pipeline();
      pipeline.get(metaKey);
      pipeline.getBuffer(symbolsKey);

      const results = await pipeline.exec();

      if (!results || results.length < 2) return null;

      const metadata = this._maybeJsonParse(results[0][1]);
      const compressedSymbols = results[1][1];

      if (!metadata) return null;

      let OHLC_Symbols = [];
      
      if (compressedSymbols) {
        try {
          const decompressed = await gunzip(compressedSymbols);
          OHLC_Symbols = JSON.parse(decompressed.toString());
        } catch (e) {
          console.error('Decompress error:', e);
        }
      }

      metadata.totalsymbol = OHLC_Symbols.length.toString();

      const result = {
        ...metadata,
        OHLC_Symbols
      };

      // ✅ Cache result
      this._setBrokerCache(brokerName, result, 100); // 100ms cache

      return result;

    } catch (error) {
      console.error('Error getBroker:', error);
      return null;
    }
  }

  /**
   * GET ALL BROKERS - Ultra optimized
   */
  async getAllBrokers() {
    try {
      // ✅ Check cache (50ms TTL)
      const cached = this._getCache('allBrokers');
      if (cached) return cached;

      // ✅ Get metadata keys
      const allKeys = await this._getBrokerKeysCached();
      const metaKeys = allKeys.filter(k => !k.includes(':symbols'));

      if (metaKeys.length === 0) {
        this._setCache('allBrokers', []);
        return [];
      }

      // ✅ Build mega pipeline
      const pipeline = this.client.pipeline();
      
      for (const metaKey of metaKeys) {
        const broker = metaKey.replace('BROKER:', '');
        const symbolsKey = `BROKER:${broker}:symbols:gz`;
        
        pipeline.get(metaKey);
        pipeline.getBuffer(symbolsKey);
      }

      const results = await pipeline.exec();

      // ✅ Parallel decompress
      const brokerPromises = [];

      for (let i = 0; i < results.length; i += 2) {
        const metaResult = results[i];
        const symbolsResult = results[i + 1];

        if (!metaResult || !metaResult[1]) continue;

        const metadata = this._maybeJsonParse(metaResult[1]);
        if (!metadata) continue;

        const compressedSymbols = symbolsResult ? symbolsResult[1] : null;

        // ✅ Decompress async
        const promise = (async () => {
          let OHLC_Symbols = [];
          
          if (compressedSymbols) {
            try {
              const decompressed = await gunzip(compressedSymbols);
              OHLC_Symbols = JSON.parse(decompressed.toString());
            } catch (e) {
              // Silent fail
            }
          }

          metadata.totalsymbol = OHLC_Symbols.length.toString();
          
          return {
            ...metadata,
            OHLC_Symbols
          };
        })();

        brokerPromises.push(promise);
      }

      // ✅ Wait all decompress parallel
      const validBrokers = await Promise.all(brokerPromises);

      // ✅ Sort
      validBrokers.sort((a, b) => {
        const indexA = parseInt(a.index, 10) || 0;
        const indexB = parseInt(b.index, 10) || 0;
        return indexA - indexB;
      });

      this._setCache('allBrokers', validBrokers);
      return validBrokers;

    } catch (error) {
      console.error('Error getAllBrokers:', error);
      return [];
    }
  }

  /**
   * DELETE BROKER
   */
  async deleteBroker(brokerName) {
    try {
      if (!brokerName) throw new Error('Broker name required');

      const metaKey = `BROKER:${brokerName}`;
      const symbolsKey = `BROKER:${brokerName}:symbols:gz`;

      const exists = await this.client.exists(metaKey);
      if (!exists) {
        return { success: false, message: `Broker "${brokerName}" not found` };
      }

      await this.client.unlink(metaKey, symbolsKey);

      // Clean old keys
      const oldKeys = await this.scanKeys(`symbol:${brokerName}:*`);
      if (oldKeys.length > 0) {
        await this._deleteKeys(oldKeys);
      }

      this._invalidateBrokerCache();
      this._lastHashes.delete(brokerName);

      return { success: true, message: `Deleted broker "${brokerName}"` };

    } catch (error) {
      console.error('Error deleteBroker:', error);
      return { success: false, message: error.message };
    }
  }

  // ================================================================
  // REST OF FUNCTIONS (keeping same logic)
  // ================================================================

  async saveConfigAdmin(data) {
    await this.client.set('CONFIG', JSON.stringify(data));
  }

  async getConfigAdmin() {
    const raw = await this.client.get('CONFIG');
    return raw ? this._maybeJsonParse(raw) : null;
  }

  async getAllUniqueSymbols() {
    try {
      const brokers = await this.getAllBrokers();
      const uniqueSymbols = new Set();

      for (const broker of brokers) {
        const arr = broker?.OHLC_Symbols;
        if (!Array.isArray(arr)) continue;
        for (const symbolData of arr) {
          if (symbolData?.symbol) uniqueSymbols.add(symbolData.symbol);
        }
      }

      return Array.from(uniqueSymbols);
    } catch (error) {
      console.error('Error getAllUniqueSymbols:', error);
      return [];
    }
  }

  async getSymbolDetails(symbolName) {
    try {
      const brokers = await this.getAllBrokers();
      if (!brokers || brokers.length === 0) return [];

      const symbolDetails = [];

      for (const broker of brokers) {
        if (!broker || broker.status !== "True") continue;

        const arr = broker.OHLC_Symbols;
        if (!Array.isArray(arr)) continue;

        for (const sym of arr) {
          if (!sym) continue;
          if (sym.symbol === symbolName && sym.trade === "TRUE") {
            symbolDetails.push({
              Broker: broker.broker,
              Broker_: broker.broker_,
              Status: broker.status,
              Index: broker.index,
              ...sym,
            });
            break;
          }
        }
      }

      symbolDetails.sort((a, b) => parseFloat(a.Index || 0) - parseFloat(b.Index || 0));
      return symbolDetails;
    } catch (error) {
      console.error('Error getSymbolDetails:', error);
      return [];
    }
  }

  async clearData() {
    try {
      await this.client.flushall();
      this._invalidateBrokerCache();
      this._lastHashes.clear();
      log(colors.green, 'REDIS', colors.reset, 'Redis cleared');
    } catch (error) {
      log(colors.red, 'REDIS', colors.reset, 'Error clearing:', error);
    }
  }

  async clearAllBroker() {
    try {
      const patterns = ['BROKER:*', 'Analysis:*', 'symbol:*'];
      let totalDeleted = 0;

      for (const pattern of patterns) {
        const keys = await this.scanKeys(pattern);
        if (keys.length > 0) {
          const deleted = await this._deleteKeys(keys);
          totalDeleted += (deleted || keys.length);
        }
      }

      this._invalidateBrokerCache();
      this._lastHashes.clear();
      
      log(colors.green, 'REDIS', colors.reset, `✅ Cleared ${totalDeleted} keys`);
      return { success: true, totalDeleted };
    } catch (error) {
      log(colors.red, 'REDIS', colors.reset, `Error: ${error.message}`);
      return { success: false, error: error.message };
    }
  }

  async clearAllAppData() {
    return await this.clearAllBroker();
  }

  async getMultipleSymbolDetails(symbols) {
    if (!symbols || symbols.length === 0) return new Map();

    try {
      const brokers = await this.getAllBrokers();
      if (!brokers || brokers.length === 0) return new Map();

      const symbolSet = new Set(symbols);
      const resultMap = new Map();

      for (const sym of symbols) resultMap.set(sym, []);

      for (const broker of brokers) {
        if (!broker?.OHLC_Symbols || !Array.isArray(broker.OHLC_Symbols)) continue;
        if (broker.status !== "True") continue;

        for (const symbolInfo of broker.OHLC_Symbols) {
          const sym = symbolInfo?.symbol;
          if (!sym || !symbolSet.has(sym)) continue;
          if (symbolInfo.trade !== "TRUE") continue;

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
      console.error('Error getMultipleSymbolDetails:', error);
      return new Map();
    }
  }

  async findBrokerByIndex(index) {
    try {
      if (index === undefined || index === null) throw new Error('index required');

      const brokers = await this.getAllBrokers();
      const targetIndex = String(index);

      return brokers.find(broker => String(broker.index) === targetIndex) || null;
    } catch (error) {
      console.error('Error findBrokerByIndex:', error);
      return null;
    }
  }

  async updateBrokerStatus(broker, newStatus) {
    try {
      const key = `BROKER:${broker}`;
      const raw = await this.client.get(key);
      
      if (raw) {
        const data = this._maybeJsonParse(raw);
        if (!data || typeof data !== 'object') return null;

        data.status = newStatus;
        data.timeUpdated = new Date().toISOString().slice(0, 19).replace('T', ' ');
        await this.client.set(key, JSON.stringify(data));

        this._invalidateBrokerCache();
        this._setBrokerCache(broker, null);
        
        return data;
      }
      return null;
    } catch (error) {
      console.error('Error updateBrokerStatus:', error);
      throw error;
    }
  }

  async getSymbol(symbol) {
    try {
      if (!symbol) throw new Error('Symbol required');

      const brokers = await this.getAllBrokers();

      let result = null;
      let minIndex = Number.MAX_SAFE_INTEGER;

      for (const broker of brokers) {
        const brokerIndex = parseInt(broker?.index, 10);
        if (!broker?.OHLC_Symbols || isNaN(brokerIndex)) continue;

        for (const info of broker.OHLC_Symbols) {
          if (!info) continue;
          if (
            info.symbol === symbol &&
            info.trade === "TRUE" &&
            broker.status !== "Disconnect"
          ) {
            if (brokerIndex < minIndex) {
              minIndex = brokerIndex;
              result = {
                ...info,
                Broker: broker.broker,
                BrokerIndex: broker.index
              };
            }
            break;
          }
        }
      }

      return result;
    } catch (error) {
      console.error('Error getSymbol:', error);
      return null;
    }
  }

  async Broker_names() {
    try {
      const data = await this.getAllBrokers();
      return data.map(broker => {
        const { OHLC_Symbols, ...info } = broker;
        return info;
      });
    } catch (error) {
      console.error('Error Broker_names:', error);
      return [];
    }
  }

  async saveAnalysis(data) {
    await this.client.set('Analysis', JSON.stringify(data));
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
      this._lastHashes.clear();
      await this.publisherClient.quit();
      await this.subscriberClient.quit();
      log(colors.yellow, 'REDIS', colors.reset, 'Disconnected');
    } catch (error) {
      console.error('Error disconnect:', error);
    }
  }

  // Reset tracking
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
      log(colors.green, 'REDIS', colors.reset, `✅ Tracking ${brokers.length} brokers`);
      return true;
    } catch (error) {
      console.error('Error startResetTracking:', error);
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
        }
        await this.client.setex('reset_progress', 3600, JSON.stringify(progress));
        return true;
      }

      return false;
    } catch (error) {
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
      console.error('Error clearResetTracking:', error);
    }
  }
}

module.exports = new RedisManager();