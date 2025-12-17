const redisClient = require('./redisManager'); // ✅ Import Redis client
const Redis = require('ioredis');
const { log, colors } = require('../Helpers/Log');
const zlib = require('zlib');
const { promisify } = require('util');

const gzip = promisify(zlib.gzip);
const gunzip = promisify(zlib.gunzip);

class RedisManager {
  constructor() {
    this.client = redisClient; // ✅ Sử dụng singleton client

    this.messageHandlers = new Map();
    this.isSubscriberSetup = false;

    // ✅ CONNECTION STATE
    this.isPublisherReady = false;
    this.isSubscriberReady = false;

    // ✅ CACHING
    this._cache = {
      brokerKeys: { at: 0, ttl: 1000, value: null },
      allBrokers: { at: 0, ttl: 50, value: null },
      brokerData: new Map(),
    };

    this._lastHashes = new Map();

    // ✅ TẠO RIÊNG PUBLISHER VÀ SUBSCRIBER
    const redisConfig = {
      host: process.env.REDIS_HOST || 'localhost',
      port: process.env.REDIS_PORT || 6379,
      password: process.env.REDIS_PASSWORD || undefined,
      db: process.env.REDIS_DB || 0,
      
      connectTimeout: 10000,
      keepAlive: 1,
      noDelay: true,
      enableAutoPipelining: true,
      enableOfflineQueue: true,
      
      retryStrategy: (times) => {
        if (times > 10) return null;
        return Math.min(times * 100, 2000);
      },
      
      maxRetriesPerRequest: 3,
      
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
    const batchSize = 1000;
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

  async _ensurePublisherReady() {
    if (this.isPublisherReady) return true;

    const maxWait = 5000;
    const start = Date.now();

    while (!this.isPublisherReady && Date.now() - start < maxWait) {
      await new Promise(resolve => setTimeout(resolve, 50));
    }

    return this.isPublisherReady;
  }

  async _ensureSubscriberReady() {
    if (this.isSubscriberReady) return true;

    const maxWait = 5000;
    const start = Date.now();

    while (!this.isSubscriberReady && Date.now() - start < maxWait) {
      await new Promise(resolve => setTimeout(resolve, 50));
    }

    return this.isSubscriberReady;
  }

  setupEventHandlers() {
    // PUBLISHER EVENTS
    this.publisherClient.on('connect', () => {
      log(colors.green, 'REDIS-PUB', colors.reset, 'Connecting...');
    });

    this.publisherClient.on('ready', () => {
      this.isPublisherReady = true;
      log(colors.green, 'REDIS-PUB', colors.reset, '✅ READY');
    });

    this.publisherClient.on('error', (err) => {
      console.error('Redis Publisher Error:', err.message);
      this.isPublisherReady = false;
    });

    this.publisherClient.on('close', () => {
      log(colors.yellow, 'REDIS-PUB', colors.reset, '⚠️ Closed');
      this.isPublisherReady = false;
    });

    this.publisherClient.on('reconnecting', () => {
      log(colors.yellow, 'REDIS-PUB', colors.reset, 'Reconnecting...');
      this.isPublisherReady = false;
    });

    // SUBSCRIBER EVENTS
    this.subscriberClient.on('connect', () => {
      log(colors.green, 'REDIS-SUB', colors.reset, 'Connecting...');
    });

    this.subscriberClient.on('ready', () => {
      this.isSubscriberReady = true;
      log(colors.green, 'REDIS-SUB', colors.reset, '✅ READY');
    });

    this.subscriberClient.on('error', (err) => {
      console.error('Redis Subscriber Error:', err.message);
      this.isSubscriberReady = false;
    });

    this.subscriberClient.on('close', () => {
      log(colors.yellow, 'REDIS-SUB', colors.reset, '⚠️ Closed');
      this.isSubscriberReady = false;
    });

    this.subscriberClient.on('reconnecting', () => {
      log(colors.yellow, 'REDIS-SUB', colors.reset, 'Reconnecting...');
      this.isSubscriberReady = false;
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

  async subscribe(channel, callback) {
    try {
      const ready = await this._ensureSubscriberReady();
      if (!ready) {
        throw new Error('Subscriber not ready');
      }

      this.messageHandlers.set(channel, callback);
      return await this.subscriberClient.subscribe(channel);
    } catch (error) {
      console.error('Subscribe error:', error.message);
      return 0;
    }
  }

  async unsubscribe(channel) {
    try {
      this.messageHandlers.delete(channel);
      return await this.subscriberClient.unsubscribe(channel);
    } catch (error) {
      console.error('Unsubscribe error:', error.message);
      return 0;
    }
  }

  async publish(channel, message) {
    try {
      const ready = await this._ensurePublisherReady();
      if (!ready) {
        throw new Error('Publisher not ready');
      }

      const payload = typeof message === 'object' ? JSON.stringify(message) : message;
      return await this.publisherClient.publish(channel, payload);
    } catch (error) {
      console.error('Publish error:', error.message);
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
    const ready = await this._ensurePublisherReady();
    if (!ready) {
      log(colors.red, 'REDIS', colors.reset, 'Cannot scan: not ready');
      return [];
    }

    const keys = [];
    let cursor = '0';
    do {
      try {
        const [newCursor, foundKeys] = await this.client.scan(
          cursor,
          'MATCH', pattern,
          'COUNT', 2000
        );
        cursor = newCursor;
        if (foundKeys && foundKeys.length) keys.push(...foundKeys);
      } catch (error) {
        console.error('Scan error:', error.message);
        break;
      }
    } while (cursor !== '0');
    return keys;
  }

  // ================================================================
  // MAIN FUNCTIONS
  // ================================================================

  async saveBrokerData(broker, data) {
    const startTime = Date.now();

    try {
      const ready = await this._ensurePublisherReady();
      if (!ready) {
        log(colors.red, 'REDIS', colors.reset, `Cannot save ${broker}: not ready`);
        return { 
          success: false, 
          action: 'redis_not_ready',
          elapsed: Date.now() - startTime 
        };
      }

      // Hash check
      const dataStr = JSON.stringify(data);
      const dataHash = this._fastHash(dataStr);
      const lastHash = this._lastHashes.get(broker);

      if (lastHash === dataHash) {
        return { 
          success: true, 
          action: 'skipped_unchanged',
          elapsed: Date.now() - startTime 
        };
      }

      this._lastHashes.set(broker, dataHash);

      const result = await this._saveBrokerOptimized(broker, data);
      
      const elapsed = Date.now() - startTime;
      
      if (elapsed > 50) {
        log(colors.yellow, 'REDIS', colors.reset, 
            `⚠️ ${broker}: Save took ${elapsed}ms`);
      }

      return { ...result, elapsed };

    } catch (error) {
      console.error(`Error saving ${broker}:`, error.message);
      return {
        success: false,
        action: 'error',
        error: error.message,
        elapsed: Date.now() - startTime
      };
    }
  }

  async _saveBrokerOptimized(broker, data) {
    const metaKey = `BROKER:${broker}`;
    const symbolsKey = `BROKER:${broker}:symbols:gz`;

    const pipeline = this.client.pipeline();

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

    const symbols = data.OHLC_Symbols || [];
    if (symbols.length > 0) {
      const symbolsJson = JSON.stringify(symbols);
      
      const compressed = await gzip(Buffer.from(symbolsJson), {
        level: 6
      });

      pipeline.setBuffer(symbolsKey, compressed);
      pipeline.expire(symbolsKey, 3600);
    }

    await pipeline.exec();
    
    this._invalidateBrokerCache();
    this._setBrokerCache(broker, null);

    return { 
      success: true, 
      action: 'saved_compressed', 
      symbols: symbols.length 
    };
  }

  async getBroker(brokerName) {
    try {
      if (!brokerName) throw new Error('Broker name required');

      const ready = await this._ensurePublisherReady();
      if (!ready) return null;

      const cached = this._getBrokerCache(brokerName);
      if (cached) return cached;

      const metaKey = `BROKER:${brokerName}`;
      const symbolsKey = `BROKER:${brokerName}:symbols:gz`;

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
          console.error('Decompress error:', e.message);
        }
      }

      metadata.totalsymbol = OHLC_Symbols.length.toString();

      const result = {
        ...metadata,
        OHLC_Symbols
      };

      this._setBrokerCache(brokerName, result, 100);

      return result;

    } catch (error) {
      console.error('Error getBroker:', error.message);
      return null;
    }
  }

  async getAllBrokers() {
    try {
      const ready = await this._ensurePublisherReady();
      if (!ready) return [];

      const cached = this._getCache('allBrokers');
      if (cached) return cached;

      const allKeys = await this._getBrokerKeysCached();
      const metaKeys = allKeys.filter(k => !k.includes(':symbols'));

      if (metaKeys.length === 0) {
        this._setCache('allBrokers', []);
        return [];
      }

      const pipeline = this.client.pipeline();
      
      for (const metaKey of metaKeys) {
        const broker = metaKey.replace('BROKER:', '');
        const symbolsKey = `BROKER:${broker}:symbols:gz`;
        
        pipeline.get(metaKey);
        pipeline.getBuffer(symbolsKey);
      }

      const results = await pipeline.exec();

      const brokerPromises = [];

      for (let i = 0; i < results.length; i += 2) {
        const metaResult = results[i];
        const symbolsResult = results[i + 1];

        if (!metaResult || !metaResult[1]) continue;

        const metadata = this._maybeJsonParse(metaResult[1]);
        if (!metadata) continue;

        const compressedSymbols = symbolsResult ? symbolsResult[1] : null;

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

      const validBrokers = await Promise.all(brokerPromises);

      validBrokers.sort((a, b) => {
        const indexA = parseInt(a.index, 10) || 0;
        const indexB = parseInt(b.index, 10) || 0;
        return indexA - indexB;
      });

      this._setCache('allBrokers', validBrokers);
      return validBrokers;

    } catch (error) {
      console.error('Error getAllBrokers:', error.message);
      return [];
    }
  }

  async deleteBroker(brokerName) {
    try {
      if (!brokerName) throw new Error('Broker name required');

      const ready = await this._ensurePublisherReady();
      if (!ready) {
        return { success: false, message: 'Redis not ready' };
      }

      const metaKey = `BROKER:${brokerName}`;
      const symbolsKey = `BROKER:${brokerName}:symbols:gz`;

      const exists = await this.client.exists(metaKey);
      if (!exists) {
        return { success: false, message: `Broker not found` };
      }

      await this.client.unlink(metaKey, symbolsKey);

      const oldKeys = await this.scanKeys(`symbol:${brokerName}:*`);
      if (oldKeys.length > 0) {
        await this._deleteKeys(oldKeys);
      }

      this._invalidateBrokerCache();
      this._lastHashes.delete(brokerName);

      return { success: true, message: `Deleted broker` };

    } catch (error) {
      console.error('Error deleteBroker:', error.message);
      return { success: false, message: error.message };
    }
  }

  // ✅ REST OF FUNCTIONS - giữ nguyên logic, chỉ thêm connection check
  // (Copy từ phần trước, đã có đầy đủ)

  async saveConfigAdmin(data) {
    const ready = await this._ensurePublisherReady();
    if (!ready) throw new Error('Redis not ready');
    await this.client.set('CONFIG', JSON.stringify(data));
  }

  async getConfigAdmin() {
    const ready = await this._ensurePublisherReady();
    if (!ready) return null;
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
      console.error('Error getAllUniqueSymbols:', error.message);
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
      console.error('Error getSymbolDetails:', error.message);
      return [];
    }
  }

  async clearData() {
    try {
      const ready = await this._ensurePublisherReady();
      if (!ready) throw new Error('Redis not ready');
      await this.client.flushall();
      this._invalidateBrokerCache();
      this._lastHashes.clear();
      log(colors.green, 'REDIS', colors.reset, 'Cleared');
    } catch (error) {
      log(colors.red, 'REDIS', colors.reset, 'Error:', error.message);
    }
  }

  async clearAllBroker() {
    try {
      const ready = await this._ensurePublisherReady();
      if (!ready) return { success: false, error: 'Redis not ready' };
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
      console.error('Error getMultipleSymbolDetails:', error.message);
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
      console.error('Error findBrokerByIndex:', error.message);
      return null;
    }
  }

  async updateBrokerStatus(broker, newStatus) {
    try {
      const ready = await this._ensurePublisherReady();
      if (!ready) throw new Error('Redis not ready');
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
      console.error('Error updateBrokerStatus:', error.message);
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
      console.error('Error getSymbol:', error.message);
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
      console.error('Error Broker_names:', error.message);
      return [];
    }
  }

  async saveAnalysis(data) {
    const ready = await this._ensurePublisherReady();
    if (!ready) throw new Error('Redis not ready');
    await this.client.set('Analysis', JSON.stringify(data));
  }

  async getAnalysis() {
    const ready = await this._ensurePublisherReady();
    if (!ready) return { Type_1: [], Type_2: [], time_analysis: null };
    const raw = await this.client.get('Analysis');
    if (!raw) return { Type_1: [], Type_2: [], time_analysis: null };
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
      this.isPublisherReady = false;
      this.isSubscriberReady = false;
      this.messageHandlers.clear();
      this._lastHashes.clear();
      await Promise.all([
        this.publisherClient.quit(),
        this.subscriberClient.quit()
      ]);
      log(colors.yellow, 'REDIS', colors.reset, 'Disconnected');
    } catch (error) {
      console.error('Error disconnect:', error.message);
    }
  }

  // Reset tracking
  async startResetTracking(brokers) {
    try {
      const ready = await this._ensurePublisherReady();
      if (!ready) return false;
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
      console.error('Error startResetTracking:', error.message);
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
        if (percentage >= 30) broker.completed = true;
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
      console.error('Error clearResetTracking:', error.message);
    }
  }
}

module.exports = new RedisManager();