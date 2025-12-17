const redisClient = require('./redisManager');
const Redis = require('ioredis');
const { log, colors } = require('../Helpers/Log');

class RedisManager {
  constructor() {
    this.client = redisClient;

    this.messageHandlers = new Map();
    this.isSubscriberSetup = false;

    // ✅ THÊM BATCH BUFFER
    this.batchBuffer = new Map(); // broker -> { timeout, data[] }
    this.BATCH_WINDOW = 150; // ms - chờ tất cả batches về trong 150ms

    // ================== PERF CACHE ==================
    this._cache = {
      brokerKeys: { at: 0, ttl: 800, value: null },
      allBrokers: { at: 0, ttl: 200, value: null }, // ✅ GIẢM TỪ 600 → 200ms
    };

    const redisConfig = {
      host: 'localhost',
      port: 6379,

      lazyConnect: true,
      enableAutoPipelining: true,

      connectTimeout: 10000,
      keepAlive: 1,
      noDelay: true,

      retryStrategy: (times) => Math.min(times * 50, 2000),
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

  // ================== INTERNAL PERF HELPERS ==================
  _now() {
    return Date.now();
  }

  _invalidateBrokerCache() {
    this._cache.brokerKeys.value = null;
    this._cache.brokerKeys.at = 0;
    this._cache.allBrokers.value = null;
    this._cache.allBrokers.at = 0;
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

  async _deleteKeys(keys) {
    if (!keys || keys.length === 0) return 0;

    const batchSize = 500;
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

  async _scanIter(pattern, count = 1000, onKeys) {
    let cursor = '0';
    do {
      const [newCursor, foundKeys] = await this.client.scan(
        cursor,
        'MATCH', pattern,
        'COUNT', count
      );
      cursor = newCursor;
      if (foundKeys && foundKeys.length) {
        await onKeys(foundKeys);
      }
    } while (cursor !== '0');
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
        log(colors.red, 'REDIS', colors.reset, `Message processing error: ${error.message}`);
      }
    });

    this.isSubscriberSetup = true;
  }

  subscribe(channel, callback) {
    try {
      log(colors.yellow, 'REDIS', colors.reset, `Subscribing to ${channel}`);
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
      return this.subscriberClient.unsubscribe(channel).then((res) => {
        log(colors.yellow, 'REDIS', colors.reset, `Unsubscribed from ${channel}`);
        return res;
      });
    } catch (error) {
      console.error('Unsubscribe error:', error);
      return Promise.resolve(0);
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
        'COUNT', 1000
      );
      cursor = newCursor;
      if (foundKeys && foundKeys.length) keys.push(...foundKeys);
    } while (cursor !== '0');

    return keys;
  }

  // ================================================================
  // ✅ MEGA OPTIMIZATION: BATCH BUFFER + INCREMENTAL UPDATE
  // ================================================================
  
  /**
   * Entry point - buffer batches trong 150ms rồi process 1 lần
   */
  async saveBrokerData(broker, data) {
    const key = `BROKER:${broker}`;

    try {
      // ✅ CHECK BATCH INFO
      const batchNum = parseInt(data.batch || 1);
      const totalBatches = parseInt(data.totalBatches || 1);

      // ✅ NẾU CHỈ 1 BATCH → XỬ LÝ NGAY
      if (totalBatches === 1) {
        return await this._saveBrokerDataImmediate(broker, data);
      }

      // ✅ BUFFER BATCH
      if (!this.batchBuffer.has(broker)) {
        this.batchBuffer.set(broker, {
          data: [],
          receivedBatches: new Set(),
          timeout: null,
          startTime: Date.now()
        });
      }

      const buffer = this.batchBuffer.get(broker);
      buffer.data.push(data);
      buffer.receivedBatches.add(batchNum);

      // ✅ CLEAR OLD TIMEOUT
      if (buffer.timeout) {
        clearTimeout(buffer.timeout);
      }

      // ✅ NẾU ĐỦ BATCHES → XỬ LÝ NGAY
      if (buffer.receivedBatches.size === totalBatches) {
        clearTimeout(buffer.timeout);
        return await this._processBatchBuffer(broker);
      }

      // ✅ SET TIMEOUT ĐỂ XỬ LÝ SAU BATCH_WINDOW
      buffer.timeout = setTimeout(async () => {
        await this._processBatchBuffer(broker);
      }, this.BATCH_WINDOW);

      return { success: true, action: 'buffered', batch: batchNum };

    } catch (error) {
      console.error('Error in saveBrokerData:', error);
      throw error;
    }
  }

  /**
   * Process tất cả batches đã buffer
   */
  async _processBatchBuffer(broker) {
    const buffer = this.batchBuffer.get(broker);
    if (!buffer || buffer.data.length === 0) return;

    const startTime = Date.now();

    try {
      // ✅ MERGE TẤT CẢ BATCHES
      const mergedData = this._mergeAllBatches(buffer.data);
      
      // ✅ SAVE VÀO REDIS
      const result = await this._saveBrokerDataOptimized(broker, mergedData);

      const elapsed = Date.now() - startTime;
      log(colors.green, 'REDIS', colors.reset, 
          `✅ ${broker}: Processed ${buffer.data.length} batches in ${elapsed}ms`);

      // ✅ CLEANUP BUFFER
      this.batchBuffer.delete(broker);

      return result;

    } catch (error) {
      console.error('Error processing batch buffer:', error);
      this.batchBuffer.delete(broker);
      throw error;
    }
  }

  /**
   * Merge tất cả batches thành 1 data object
   */
  _mergeAllBatches(batches) {
    if (batches.length === 1) return batches[0];

    // ✅ LẤY METADATA TỪ BATCH CUỐI (MỚI NHẤT)
    const latest = batches[batches.length - 1];
    const merged = {
      port: latest.port,
      index: latest.index,
      broker: latest.broker,
      broker_: latest.broker_,
      version: latest.version,
      typeaccount: latest.typeaccount,
      timecurent: latest.timecurent,
      auto_trade: latest.auto_trade,
      status: latest.status,
      timeUpdated: latest.timeUpdated,
      OHLC_Symbols: []
    };

    // ✅ MERGE SYMBOLS BẰNG MAP
    const symbolMap = new Map();

    for (const batch of batches) {
      const symbols = batch.OHLC_Symbols;
      if (!Array.isArray(symbols)) continue;

      for (const sym of symbols) {
        if (!sym || !sym.symbol) continue;
        symbolMap.set(sym.symbol, sym);
      }
    }

    merged.OHLC_Symbols = Array.from(symbolMap.values());
    merged.totalsymbol = symbolMap.size.toString();

    return merged;
  }

  /**
   * Save optimized - chỉ update khi cần
   */
  async _saveBrokerDataOptimized(broker, data) {
    const key = `BROKER:${broker}`;

    try {
      const expectedTotal = parseInt(data.totalsymbol || 0);

      // ✅ GET EXISTING (có thể dùng pipeline để giảm RTT)
      const existingData = await this.client.get(key);

      if (!existingData) {
        await this.client.set(key, JSON.stringify(data));
        this._invalidateBrokerCache();
        return { success: true, action: 'created', total: expectedTotal };
      }

      // ✅ QUICK CHECK: Parse chỉ để lấy totalsymbol
      const totalMatch = existingData.match(/"totalsymbol"\s*:\s*"(\d+)"/);
      const currentTotal = totalMatch ? parseInt(totalMatch[1]) : 0;

      // ✅ RESET NẾU SERVER > CLIENT
      if (currentTotal > expectedTotal) {
        await this.client.set(key, JSON.stringify(data));
        this._invalidateBrokerCache();
        return { success: true, action: 'reset', total: expectedTotal };
      }

      // ✅ PARSE ĐẦY ĐỦ ĐỂ MERGE
      let parsedExisting;
      try {
        parsedExisting = JSON.parse(existingData);
      } catch {
        await this.client.set(key, JSON.stringify(data));
        this._invalidateBrokerCache();
        return { success: true, action: 'reset', total: expectedTotal };
      }

      // ✅ UPDATE METADATA
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

      // ✅ MERGE SYMBOLS
      const oldArr = Array.isArray(parsedExisting.OHLC_Symbols) ? parsedExisting.OHLC_Symbols : [];
      const symbolMap = new Map(oldArr.map(s => [s.symbol, s]));

      let stats = { added: 0, updated: 0 };

      const newArr = Array.isArray(data.OHLC_Symbols) ? data.OHLC_Symbols : [];
      for (const newSymbol of newArr) {
        if (!newSymbol || !newSymbol.symbol) continue;

        if (symbolMap.has(newSymbol.symbol)) stats.updated++;
        else stats.added++;

        symbolMap.set(newSymbol.symbol, newSymbol);
      }

      parsedExisting.OHLC_Symbols = Array.from(symbolMap.values());
      parsedExisting.totalsymbol = symbolMap.size.toString();

      // ✅ SAVE
      await this.client.set(key, JSON.stringify(parsedExisting));
      this._invalidateBrokerCache();

      return { success: true, action: 'merged', ...stats, total: symbolMap.size };

    } catch (error) {
      throw error;
    }
  }

  /**
   * Immediate save (fallback cho single batch)
   */
  async _saveBrokerDataImmediate(broker, data) {
    return await this._saveBrokerDataOptimized(broker, data);
  }

  // ================================================================
  // ✅ ALTERNATIVE: HASH-BASED STORAGE (SIÊU NHANH)
  // ================================================================
  
  /**
   * Lưu từng symbol riêng bằng HASH - nhanh hơn 10x
   * Thay vì 1 JSON lớn, mỗi symbol là 1 hash field
   */
  async saveBrokerDataHash(broker, data) {
    const key = `BROKER:${broker}`;
    const symbolsKey = `BROKER:${broker}:symbols`;

    try {
      const pipeline = this.client.pipeline();

      // ✅ SAVE METADATA (nhỏ gọn)
      const metadata = {
        port: data.port,
        index: data.index,
        broker: data.broker,
        broker_: data.broker_,
        version: data.version,
        typeaccount: data.typeaccount,
        timecurent: data.timecurent,
        auto_trade: data.auto_trade,
        status: data.status,
        timeUpdated: data.timeUpdated,
        totalsymbol: data.totalsymbol
      };

      pipeline.set(key, JSON.stringify(metadata));

      // ✅ SAVE MỖI SYMBOL VÀO HASH
      const symbols = data.OHLC_Symbols || [];
      for (const sym of symbols) {
        if (!sym || !sym.symbol) continue;
        
        // HSET broker:symbols symbol_name json_data
        pipeline.hset(symbolsKey, sym.symbol, JSON.stringify(sym));
      }

      await pipeline.exec();
      this._invalidateBrokerCache();

      return { 
        success: true, 
        action: 'saved_hash', 
        symbols: symbols.length 
      };

    } catch (error) {
      console.error('Error saving hash-based data:', error);
      throw error;
    }
  }

  /**
   * Get broker với hash storage
   */
  async getBrokerHash(brokerName) {
    try {
      const key = `BROKER:${brokerName}`;
      const symbolsKey = `BROKER:${brokerName}:symbols`;

      // ✅ GET METADATA + ALL SYMBOLS trong 1 pipeline
      const pipeline = this.client.pipeline();
      pipeline.get(key);
      pipeline.hgetall(symbolsKey);

      const results = await pipeline.exec();

      const metadata = this._maybeJsonParse(results[0][1]);
      const symbolsHash = results[1][1];

      if (!metadata) return null;

      // ✅ PARSE SYMBOLS
      const OHLC_Symbols = [];
      if (symbolsHash) {
        for (const [symbol, jsonData] of Object.entries(symbolsHash)) {
          const sym = this._maybeJsonParse(jsonData);
          if (sym) OHLC_Symbols.push(sym);
        }
      }

      return {
        ...metadata,
        OHLC_Symbols
      };

    } catch (error) {
      console.error('Error getting hash-based broker:', error);
      return null;
    }
  }

  // ================================================================
  // REST OF THE CODE UNCHANGED
  // ================================================================

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
        const parsed = this._maybeJsonParse(raw);
        result[key.replace('BROKER:', '')] = parsed;
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
      this._invalidateBrokerCache();
      return true;
    } catch (error) {
      console.error('Error saving broker data:', error);
      return false;
    }
  }

  async getAllBrokers() {
    try {
      const cached = this._getCache('allBrokers');
      if (cached) return cached;

      const keys = await this._getBrokerKeysCached();
      if (keys.length === 0) {
        this._setCache('allBrokers', []);
        return [];
      }

      const values = await this.client.mget(keys);

      const validBrokers = [];
      for (const raw of values) {
        if (!raw) continue;
        const parsed = this._maybeJsonParse(raw);
        if (parsed && typeof parsed === 'object') validBrokers.push(parsed);
      }

      validBrokers.sort((a, b) => {
        const indexA = parseInt(a.index, 10) || 0;
        const indexB = parseInt(b.index, 10) || 0;
        return indexA - indexB;
      });

      this._setCache('allBrokers', validBrokers);
      return validBrokers;
    } catch (error) {
      console.error('Error getting brokers from Redis:', error);
      return [];
    }
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
      console.error('Error getting unique symbols from Redis:', error);
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

      await this._deleteKeys(symbolKeys);

      try {
        await this.client.unlink(brokerKey);
      } catch {
        await this.client.del(brokerKey);
      }

      this._invalidateBrokerCache();
      return { success: true, message: `Deleted broker "${brokerName}" and related symbols` };
    } catch (error) {
      console.error(`Error deleting broker ${brokerName}:`, error);
      return { success: false, message: `Error deleting broker: ${error.message}` };
    }
  }

  async clearData() {
    try {
      await this.client.flushall();
      this._invalidateBrokerCache();
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
          const deleted = await this._deleteKeys(keys);
          log(colors.yellow, 'REDIS', colors.reset, `Deleted ${keys.length} keys matching "${pattern}"`);
          totalDeleted += (deleted || keys.length);
        }
      }

      this._invalidateBrokerCache();
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
          const deleted = await this._deleteKeys(keys);
          log(colors.yellow, 'REDIS', colors.reset, `Deleted ${keys.length} keys matching "${pattern}"`);
          totalDeleted += (deleted || keys.length);
        }
      }

      this._invalidateBrokerCache();
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

      const parsed = this._maybeJsonParse(brokerData);
      return (parsed && typeof parsed === 'object') ? parsed : null;
    } catch (error) {
      console.error(`Error getting broker '${brokerName}' from Redis:`, error);
      return null;
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
        const data = this._maybeJsonParse(raw);
        if (!data || typeof data !== 'object') return null;

        data.status = newStatus;
        data.timeUpdated = new Date().toISOString().slice(0, 19).replace('T', ' ');
        await this.client.set(key, JSON.stringify(data));

        this._invalidateBrokerCache();
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
        const brokerIndex = parseInt(broker?.index, 10);

        if (!broker?.OHLC_Symbols || isNaN(brokerIndex)) continue;

        let found = null;
        for (const info of broker.OHLC_Symbols) {
          if (!info) continue;
          if (
            info.symbol === symbol &&
            info.trade === "TRUE" &&
            broker.status !== "Disconnect"
          ) {
            found = info;
            break;
          }
        }

        if (found && brokerIndex < minIndex) {
          minIndex = brokerIndex;
          result = {
            ...found,
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
    await this.client.set(key, JSON.stringify(data));
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
      return JSON.parse(raw);
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
      
      // ✅ CLEAR BATCH BUFFERS
      for (const [broker, buffer] of this.batchBuffer) {
        if (buffer.timeout) clearTimeout(buffer.timeout);
      }
      this.batchBuffer.clear();

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
}

module.exports = new RedisManager();