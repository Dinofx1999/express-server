const redisClient = require('./redisManager');
const Redis = require('ioredis');
const { log, colors } = require('../Helpers/Log');

class RedisManager {
  constructor() {
    this.client = redisClient;
    this.messageHandlers = new Map();
    this.isSubscriberSetup = false;
    this.isPublisherReady = false;
    this.isSubscriberReady = false;

    // ✅ CACHE
    this._cache = {
      allBrokers: { at: 0, ttl: 100, value: null },
      brokerData: new Map(),
    };

    // ✅ BATCH BUFFER SYSTEM
    this.batchBuffer = new Map(); // broker -> { batches: [], timeout, receivedCount }
    this.BATCH_WINDOW = 150; // ms - wait time for batches

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

  _invalidateCache() {
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
    // PUBLISHER
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

    // SUBSCRIBER
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

  // ================================================================
  // PUB/SUB
  // ================================================================

  async subscribe(channel, callback) {
    try {
      const ready = await this._ensureSubscriberReady();
      if (!ready) throw new Error('Subscriber not ready');

      log(colors.yellow, 'REDIS', colors.reset, `Subscribing to ${channel}`);
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
      const result = await this.subscriberClient.unsubscribe(channel);
      log(colors.yellow, 'REDIS', colors.reset, `Unsubscribed from ${channel}`);
      return result;
    } catch (error) {
      console.error('Unsubscribe error:', error.message);
      return 0;
    }
  }

  async publish(channel, message) {
    try {
      const ready = await this._ensurePublisherReady();
      if (!ready) throw new Error('Publisher not ready');

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

  // ================================================================
  // SCAN (SAFE - NO BLOCKING)
  // ================================================================

  async scanKeys(pattern, count = 1000) {
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
          'COUNT', count
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
  // ✅ BATCH BUFFER SYSTEM
  // ================================================================

  async _processBatchBuffer(broker) {
    const buffer = this.batchBuffer.get(broker);
    if (!buffer) return;

    try {
      // Clear timeout
      if (buffer.timeout) {
        clearTimeout(buffer.timeout);
        buffer.timeout = null;
      }

      const batches = buffer.batches;
      const startTime = Date.now();

      // ✅ MERGE ALL BATCHES
      const mergedData = this._mergeAllBatches(batches);

      // ✅ SAVE MERGED DATA
      const key = `BROKER:${broker}`;
      await this.client.set(key, JSON.stringify(mergedData));

      this._invalidateCache();

      const elapsed = Date.now() - startTime;

      log(colors.green, 'REDIS', colors.reset, 
          `✅ ${broker}: Merged ${batches.length} batches (${mergedData.OHLC_Symbols.length} symbols) in ${elapsed}ms`);

      // Clean up buffer
      this.batchBuffer.delete(broker);

      return {
        success: true,
        action: 'merged_batches',
        batches: batches.length,
        symbols: mergedData.OHLC_Symbols.length,
        elapsed
      };

    } catch (error) {
      console.error(`Error processing batch buffer for ${broker}:`, error.message);
      this.batchBuffer.delete(broker);
      
      return {
        success: false,
        action: 'error',
        error: error.message
      };
    }
  }

  _mergeAllBatches(batches) {
    if (!batches || batches.length === 0) {
      throw new Error('No batches to merge');
    }

    // Use first batch as base
    const merged = {
      port: batches[0].port || '',
      index: batches[0].index || '',
      broker: batches[0].broker || '',
      broker_: batches[0].broker_ || '',
      version: batches[0].version || '',
      typeaccount: batches[0].typeaccount || '',
      timecurent: batches[0].timecurent || '',
      auto_trade: batches[0].auto_trade || '',
      status: batches[0].status || '',
      timeUpdated: batches[0].timeUpdated || new Date().toISOString().slice(0, 19).replace('T', ' '),
      totalsymbol: '0',
      OHLC_Symbols: []
    };

    // ✅ MERGE SYMBOLS from all batches
    const symbolMap = new Map();

    for (const batch of batches) {
      const symbols = batch.OHLC_Symbols || [];
      
      for (const sym of symbols) {
        if (!sym || !sym.symbol) continue;
        
        // Use symbol name as key to avoid duplicates
        symbolMap.set(sym.symbol, sym);
      }
    }

    // Convert Map to Array
    merged.OHLC_Symbols = Array.from(symbolMap.values());
    merged.totalsymbol = merged.OHLC_Symbols.length.toString();

    return merged;
  }

  // ================================================================
  // ✅ SAVE BROKER DATA - with BATCH SUPPORT
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

      // ✅ CHECK IF THIS IS BATCHED DATA
      const batchNum = parseInt(data.batch) || 0;
      const totalBatches = parseInt(data.totalBatches) || 0;

      // ✅ SINGLE BATCH or NO BATCH INFO → Save immediately
      if (totalBatches <= 1 || !batchNum) {
        return await this._saveBrokerImmediate(broker, data, startTime);
      }

      // ✅ MULTIPLE BATCHES → Use buffer system
      return await this._saveBrokerBuffered(broker, data, batchNum, totalBatches, startTime);

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

  async _saveBrokerImmediate(broker, data, startTime) {
    const key = `BROKER:${broker}`;
    
    const brokerData = {
      port: data.port || '',
      index: data.index || '',
      broker: data.broker || broker,
      broker_: data.broker_ || '',
      version: data.version || '',
      typeaccount: data.typeaccount || '',
      timecurent: data.timecurent || '',
      auto_trade: data.auto_trade || '',
      status: data.status || '',
      timeUpdated: data.timeUpdated || new Date().toISOString().slice(0, 19).replace('T', ' '),
      totalsymbol: data.totalsymbol || '0',
      OHLC_Symbols: data.OHLC_Symbols || []
    };

    await this.client.set(key, JSON.stringify(brokerData));
    
    this._invalidateCache();

    const elapsed = Date.now() - startTime;

    return { 
      success: true, 
      action: 'saved_immediate', 
      symbols: brokerData.OHLC_Symbols.length,
      elapsed 
    };
  }

  async _saveBrokerBuffered(broker, data, batchNum, totalBatches, startTime) {
    // ✅ GET or CREATE BUFFER
    if (!this.batchBuffer.has(broker)) {
      this.batchBuffer.set(broker, {
        batches: [],
        receivedBatches: new Set(),
        timeout: null,
        totalExpected: totalBatches
      });
    }

    const buffer = this.batchBuffer.get(broker);

    // ✅ ADD BATCH to buffer
    buffer.batches.push(data);
    buffer.receivedBatches.add(batchNum);

    // ✅ CHECK if ALL BATCHES RECEIVED
    if (buffer.receivedBatches.size === totalBatches) {
      // All batches received → Process immediately
      return await this._processBatchBuffer(broker);
    }

    // ✅ NOT ALL BATCHES YET → Set timeout
    if (buffer.timeout) {
      clearTimeout(buffer.timeout);
    }

    buffer.timeout = setTimeout(() => {
      log(colors.yellow, 'REDIS', colors.reset, 
          `⚠️ ${broker}: Timeout - Processing ${buffer.receivedBatches.size}/${totalBatches} batches`);
      this._processBatchBuffer(broker);
    }, this.BATCH_WINDOW);

    const elapsed = Date.now() - startTime;

    return {
      success: true,
      action: 'buffered',
      batch: batchNum,
      totalBatches: totalBatches,
      received: buffer.receivedBatches.size,
      elapsed
    };
  }

  // ================================================================
  // GET BROKER
  // ================================================================

  async getBroker(brokerName) {
    try {
      if (!brokerName) throw new Error('Broker name required');

      const ready = await this._ensurePublisherReady();
      if (!ready) return null;

      // ✅ CHECK CACHE
      const cached = this._cache.brokerData.get(brokerName);
      if (cached && this._now() - cached.at < 100) {
        return cached.value;
      }

      const key = `BROKER:${brokerName}`;
      const raw = await this.client.get(key);

      if (!raw) return null;

      const data = this._maybeJsonParse(raw);
      
      // ✅ CACHE RESULT
      this._cache.brokerData.set(brokerName, {
        value: data,
        at: this._now()
      });

      return data;

    } catch (error) {
      console.error('Error getBroker:', error.message);
      return null;
    }
  }

  async getAllBrokers() {
    try {
      const ready = await this._ensurePublisherReady();
      if (!ready) {
        log(colors.red, 'REDIS', colors.reset, 'Cannot get brokers: not ready');
        return [];
      }

      // ✅ CHECK CACHE
      const cached = this._getCache('allBrokers');
      if (cached) return cached;

      // ✅ SCAN (SAFE - NON-BLOCKING)
      const keys = await this.scanKeys('BROKER:*', 100);

      if (keys.length === 0) {
        this._setCache('allBrokers', []);
        return [];
      }

      // ✅ MGET - Get multiple keys in 1 round trip
      const values = await this.client.mget(keys);

      const validBrokers = values
        .map(broker => {
          try {
            return JSON.parse(broker);
          } catch (parseError) {
            console.error('Error parsing broker data:', parseError);
            return null;
          }
        })
        .filter(broker => broker !== null);

      // Sort by index
      validBrokers.sort((a, b) => {
        const indexA = parseInt(a.index, 10) || 0;
        const indexB = parseInt(b.index, 10) || 0;
        return indexA - indexB;
      });

      // ✅ CACHE RESULT
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

      const key = `BROKER:${brokerName}`;
      
      const exists = await this.client.exists(key);
      if (!exists) {
        return { success: false, message: `Broker "${brokerName}" not found` };
      }

      await this.client.del(key);

      // ✅ Clear batch buffer if exists
      if (this.batchBuffer.has(brokerName)) {
        const buffer = this.batchBuffer.get(brokerName);
        if (buffer.timeout) clearTimeout(buffer.timeout);
        this.batchBuffer.delete(brokerName);
      }

      // ✅ Delete old symbol keys if exist
      const oldSymbolKeys = await this.scanKeys(`symbol:${brokerName}:*`);
      if (oldSymbolKeys.length > 0) {
        await this.client.del(...oldSymbolKeys);
      }

      this._invalidateCache();

      log(colors.green, 'REDIS', colors.reset, `Deleted broker "${brokerName}"`);
      return { success: true, message: `Deleted broker "${brokerName}"` };

    } catch (error) {
      console.error('Error deleteBroker:', error.message);
      return { success: false, message: error.message };
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

        this._invalidateCache();
        
        return data;
      }
      return null;
    } catch (error) {
      console.error('Error updateBrokerStatus:', error.message);
      throw error;
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

  async getBrokerResetting() {
    const brokers = await this.getAllBrokers();
    return brokers
      .filter(broker => broker.status !== "True")
      .sort((a, b) => Number(a.index) - Number(b.index));
  }

  // ================================================================
  // SYMBOL OPERATIONS
  // ================================================================

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

  // ================================================================
  // CONFIG
  // ================================================================

  async saveConfigAdmin(data) {
    try {
      const ready = await this._ensurePublisherReady();
      if (!ready) throw new Error('Redis not ready');
      
      await this.client.set('CONFIG', JSON.stringify(data));
      return true;
    } catch (error) {
      console.error('Error saveConfigAdmin:', error.message);
      return false;
    }
  }

  async getConfigAdmin() {
    try {
      const ready = await this._ensurePublisherReady();
      if (!ready) return null;
      
      const raw = await this.client.get('CONFIG');
      return raw ? this._maybeJsonParse(raw) : null;
    } catch (error) {
      console.error('Error getConfigAdmin:', error.message);
      return null;
    }
  }

  // ================================================================
  // ANALYSIS
  // ================================================================

  async saveAnalysis(data) {
    try {
      const ready = await this._ensurePublisherReady();
      if (!ready) throw new Error('Redis not ready');
      
      await this.client.set('Analysis', JSON.stringify(data));
      return true;
    } catch (error) {
      console.error('Error saveAnalysis:', error.message);
      return false;
    }
  }

  async getAnalysis() {
    try {
      const ready = await this._ensurePublisherReady();
      if (!ready) {
        return { Type_1: [], Type_2: [], time_analysis: null };
      }

      const raw = await this.client.get('Analysis');
      if (!raw) {
        return { Type_1: [], Type_2: [], time_analysis: null };
      }
      
      try {
        return JSON.parse(raw);
      } catch {
        return { Type_1: [], Type_2: [], time_analysis: null };
      }
    } catch (error) {
      console.error('Error getAnalysis:', error.message);
      return { Type_1: [], Type_2: [], time_analysis: null };
    }
  }

  // ================================================================
  // CLEAR / DELETE
  // ================================================================

  async clearData() {
    try {
      const ready = await this._ensurePublisherReady();
      if (!ready) throw new Error('Redis not ready');

      await this.client.flushall();
      this._invalidateCache();
      
      // Clear batch buffers
      for (const [broker, buffer] of this.batchBuffer.entries()) {
        if (buffer.timeout) clearTimeout(buffer.timeout);
      }
      this.batchBuffer.clear();
      
      log(colors.green, 'REDIS', colors.reset, 'Redis cleared successfully');
      return true;
    } catch (error) {
      log(colors.red, 'REDIS', colors.reset, 'Error clearing:', error.message);
      return false;
    }
  }

  async clearAllBroker() {
    try {
      const ready = await this._ensurePublisherReady();
      if (!ready) {
        return { success: false, error: 'Redis not ready' };
      }

      // ✅ SCAN + DELETE (SAFE)
      const patterns = ['BROKER:*', 'Analysis:*', 'symbol:*'];
      let totalDeleted = 0;

      for (const pattern of patterns) {
        let cursor = '0';
        
        do {
          const [newCursor, foundKeys] = await this.client.scan(
            cursor,
            'MATCH', pattern,
            'COUNT', 1000
          );
          cursor = newCursor;
          
          if (foundKeys && foundKeys.length > 0) {
            await this.client.del(...foundKeys);
            totalDeleted += foundKeys.length;
            log(colors.yellow, 'REDIS', colors.reset, 
                `Deleted ${foundKeys.length} keys matching "${pattern}"`);
          }
        } while (cursor !== '0');
      }

      this._invalidateCache();
      
      // Clear batch buffers
      for (const [broker, buffer] of this.batchBuffer.entries()) {
        if (buffer.timeout) clearTimeout(buffer.timeout);
      }
      this.batchBuffer.clear();
      
      log(colors.green, 'REDIS', colors.reset, `✅ Cleared ${totalDeleted} keys total`);
      return { success: true, totalDeleted };
      
    } catch (error) {
      log(colors.red, 'REDIS', colors.reset, `Error: ${error.message}`);
      return { success: false, error: error.message };
    }
  }

  async clearAllAppData() {
    return await this.clearAllBroker();
  }

  // ================================================================
  // RESET PROGRESS TRACKING
  // ================================================================

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
      log(colors.green, 'REDIS', colors.reset, `✅ Started tracking ${brokers.length} brokers`);
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
        if (percentage >= 30) {
          broker.completed = true;
          log(colors.green, 'RESET', colors.reset, `✅ ${brokerName} completed: ${percentage}%`);
        }
        await this.client.setex('reset_progress', 3600, JSON.stringify(progress));
        return true;
      }

      return false;
    } catch (error) {
      console.error('Error updateResetProgress:', error.message);
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

  // ================================================================
  // DISCONNECT
  // ================================================================

  async disconnect() {
    try {
      this.isPublisherReady = false;
      this.isSubscriberReady = false;
      this.messageHandlers.clear();
      this._invalidateCache();
      
      // Clear all batch buffers
      for (const [broker, buffer] of this.batchBuffer.entries()) {
        if (buffer.timeout) clearTimeout(buffer.timeout);
      }
      this.batchBuffer.clear();
      
      await Promise.all([
        this.publisherClient.quit(),
        this.subscriberClient.quit()
      ]);
      
      log(colors.yellow, 'REDIS', colors.reset, 'Disconnected gracefully');
    } catch (error) {
      console.error('Error disconnect:', error.message);
    }
  }
}

module.exports = new RedisManager();