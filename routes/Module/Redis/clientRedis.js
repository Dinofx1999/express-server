'use strict';

const Redis = require('ioredis');
const { log, colors } = require('../Helpers/Log');

/**
 * ==========================
 *  Redis Key Convention
 * ==========================
 * META: BROKER:<broker>:META   (HASH)
 * SYMS: BROKER:<broker>:SYMS   (HASH) field = symbol, value = JSON string
 * CONFIG: CONFIG              (STRING JSON)
 * Analysis: Analysis          (STRING JSON)
 * reset progress: reset_progress (STRING JSON)
 */

class RedisManager {
  constructor() {
    this.messageHandlers = new Map(); // channel -> callback
    this.isSubscriberSetup = false;

    // ✅ ioredis tối ưu realtime:
    // - enableAutoPipelining: gom lệnh tự động giảm roundtrip
    // - maxRetriesPerRequest: null => tránh “kẹt request” khi lag
    // - keepAlive: giữ TCP ổn định
    const redisConfig = {
      host: 'localhost',
      port: 6379,

      // ⚡ tăng throughput (rất quan trọng)
      enableAutoPipelining: true,
      autoPipeliningIgnoredCommands: ['subscribe', 'psubscribe', 'unsubscribe', 'punsubscribe'],

      // ổn định khi có spike
      maxRetriesPerRequest: null,
      enableReadyCheck: true,
      keepAlive: 10000,

      retryStrategy: (times) => Math.min(times * 50, 2000),
      reconnectOnError: (err) => {
        // reconnect khi gặp lỗi mạng/READONLY…
        const msg = err?.message || '';
        if (msg.includes('READONLY') || msg.includes('ECONNRESET') || msg.includes('ETIMEDOUT')) return true;
        return false;
      },
    };

    // ✅ 1 client data chính
    this.client = new Redis(redisConfig);

    // ✅ pub/sub tách connection (Redis yêu cầu subscribe riêng)
    this.publisherClient = this.client.duplicate();
    this.subscriberClient = this.client.duplicate();

    this.setupEventHandlers();
  }

  // ==========================
  // Helpers
  // ==========================
  metaKey(broker) {
    return `BROKER:${broker}:META`;
  }
  symsKey(broker) {
    return `BROKER:${broker}:SYMS`;
  }
  legacyKey(broker) {
    // key cũ bạn từng dùng: BROKER:<broker> (STRING JSON)
    return `BROKER:${broker}`;
  }

  tryParseJSON(message) {
    try {
      return JSON.parse(message);
    } catch {
      return message;
    }
  }

  // ✅ scan nhanh + ít memory hơn (dùng scanStream)
  async scanKeys(pattern) {
    const keys = [];
    const stream = this.client.scanStream({
      match: pattern,
      count: 2000, // tăng count giảm vòng lặp
    });

    return await new Promise((resolve, reject) => {
      stream.on('data', (resultKeys) => {
        for (const k of resultKeys) keys.push(k);
      });
      stream.on('end', () => resolve(keys));
      stream.on('error', reject);
    });
  }

  // ==========================
  // Connection / PubSub
  // ==========================
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
    const payload = typeof message === 'object' ? JSON.stringify(message) : message;
    return this.publisherClient.publish(channel, payload);
  }

  // ==========================
  // FAST WRITE PATH (x10)
  // ==========================
  /**
   * saveBrokerData(broker, data)
   * - Lưu META (HASH) + SYMS (HASH)
   * - Không GET/parse JSON khổng lồ
   * - Update từng symbol bằng HSET => cực nhanh
   */
  async saveBrokerData(broker, data) {
    const metaKey = this.metaKey(broker);
    const symsKey = this.symsKey(broker);

    try {
      const expectedTotal = parseInt(data?.totalsymbol || 0, 10) || 0;

      // ✅ đọc totalsymbol hiện tại từ META (nhanh)
      const currentTotalRaw = await this.client.hget(metaKey, 'totalsymbol');
      const currentTotal = parseInt(currentTotalRaw || '0', 10) || 0;

      // ✅ nếu server > client => reset nhanh (DEL hash)
      // (giữ logic tương tự code cũ của bạn)
      if (currentTotal > expectedTotal && expectedTotal > 0) {
        const p = this.client.pipeline();
        p.del(metaKey);
        p.del(symsKey);
        // xóa key cũ nếu còn
        p.del(this.legacyKey(broker));
        await p.exec();
      }

      const p = this.client.pipeline();

      // 1) META: chỉ lưu field cần thiết
      // (giữ tên field giống data của bạn để UI không đổi)
      const meta = {
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
      };

      // lọc undefined để tránh “rác”
      const metaFlat = [];
      for (const [k, v] of Object.entries(meta)) {
        if (v !== undefined) metaFlat.push(k, String(v));
      }

      if (metaFlat.length) p.hset(metaKey, ...metaFlat);

      // 2) SYMS: update theo symbol
      const symbols = Array.isArray(data?.OHLC_Symbols) ? data.OHLC_Symbols : [];

      // để trả stats gần đúng (không tốn hexists từng symbol)
      p.hlen(symsKey); // before

      if (symbols.length) {
        // chunk field để tránh packet quá lớn
        const CHUNK = 500; // 500 fields/lần là ổn
        for (let i = 0; i < symbols.length; i += CHUNK) {
          const slice = symbols.slice(i, i + CHUNK);

          const flat = [];
          for (const s of slice) {
            if (!s || !s.symbol) continue;
            flat.push(String(s.symbol), JSON.stringify(s));
          }
          if (flat.length) p.hset(symsKey, ...flat);
        }
      }

      p.hlen(symsKey); // after

      const res = await p.exec();

      const beforeLen = parseInt(res?.[res.length - 2]?.[1] || 0, 10) || 0;
      const afterLen = parseInt(res?.[res.length - 1]?.[1] || 0, 10) || 0;

      // update totalsymbol (META) 1 lệnh
      await this.client.hset(metaKey, 'totalsymbol', String(afterLen));

      const added = Math.max(0, afterLen - beforeLen);
      const updated = Math.max(0, symbols.length - added);

      return { success: true, action: 'merged', added, updated, total: afterLen };
    } catch (error) {
      throw error;
    }
  }

  // ==========================
  // CONFIG / Analysis (giữ như cũ)
  // ==========================
  async saveConfigAdmin(data) {
    await this.client.set('CONFIG', JSON.stringify(data));
  }

  async getConfigAdmin() {
    const raw = await this.client.get('CONFIG');
    if (!raw) return null;
    try {
      return JSON.parse(raw);
    } catch (error) {
      console.error('Error parsing config admin data:', error);
      return null;
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

  // ==========================
  // READ PATH
  // ==========================
  /**
   * getAllBrokers()
   * - scan META keys
   * - lấy META + (tuỳ) load symbols
   */
  async getAllBrokers() {
    try {
      const metaKeys = await this.scanKeys('BROKER:*:META');
      if (metaKeys.length === 0) return [];

      // lấy meta hàng loạt
      const p = this.client.pipeline();
      for (const mk of metaKeys) p.hgetall(mk);
      const metaRes = await p.exec();

      const brokers = [];
      for (let i = 0; i < metaKeys.length; i++) {
        const meta = metaRes[i]?.[1];
        if (!meta || !meta.broker) continue;

        const brokerName = meta.broker;
        const symsKey = this.symsKey(brokerName);

        // ⚠️ load full symbols (nặng) - nhưng vẫn nhanh hơn parse JSON cục lớn khi write
        const symbolValues = await this.client.hvals(symsKey);

        const OHLC_Symbols = [];
        for (const v of symbolValues) {
          if (!v) continue;
          try {
            OHLC_Symbols.push(JSON.parse(v));
          } catch {}
        }

        brokers.push({
          ...meta,
          OHLC_Symbols,
          totalsymbol: meta.totalsymbol || String(OHLC_Symbols.length),
        });
      }

      // sort theo index như cũ
      return brokers.sort((a, b) => {
        const ia = parseInt(a.index, 10) || 0;
        const ib = parseInt(b.index, 10) || 0;
        return ia - ib;
      });
    } catch (error) {
      console.error('Error getting brokers from Redis:', error);
      return [];
    }
  }

  /**
   * getAllBrokers_2() - giữ lại cho bạn (object map)
   */
  async getAllBrokers_2() {
    const brokers = await this.getAllBrokers();
    const result = {};
    for (const b of brokers) {
      result[b.broker] = b;
    }
    return result;
  }

  /**
   * Broker_names(): chỉ meta (siêu nhanh)
   */
  async Broker_names() {
    try {
      const metaKeys = await this.scanKeys('BROKER:*:META');
      if (metaKeys.length === 0) return [];

      const p = this.client.pipeline();
      for (const mk of metaKeys) p.hgetall(mk);
      const metaRes = await p.exec();

      const brokers = [];
      for (const r of metaRes) {
        const meta = r?.[1];
        if (meta && meta.broker) brokers.push(meta);
      }

      return brokers.sort((a, b) => (parseInt(a.index, 10) || 0) - (parseInt(b.index, 10) || 0));
    } catch (error) {
      console.error('Lỗi hàm Broker_names trong clientRedis.js', error);
      return [];
    }
  }

  async getAllUniqueSymbols() {
    try {
      const brokers = await this.getAllBrokers();
      const uniqueSymbols = new Set();

      for (const broker of brokers) {
        if (Array.isArray(broker.OHLC_Symbols)) {
          for (const s of broker.OHLC_Symbols) {
            if (s?.symbol) uniqueSymbols.add(s.symbol);
          }
        }
      }
      return Array.from(uniqueSymbols);
    } catch (error) {
      console.error('Error getting unique symbols from Redis:', error);
      return [];
    }
  }

  /**
   * getBroker(brokerName): reconstruct from META+SYMS
   */
  async getBroker(brokerName) {
    try {
      if (!brokerName) throw new Error('Broker name is required');

      const metaKey = this.metaKey(brokerName);
      const meta = await this.client.hgetall(metaKey);
      if (!meta || !meta.broker) return null;

      const values = await this.client.hvals(this.symsKey(brokerName));
      const OHLC_Symbols = [];
      for (const v of values) {
        if (!v) continue;
        try {
          OHLC_Symbols.push(JSON.parse(v));
        } catch {}
      }

      return {
        ...meta,
        OHLC_Symbols,
        totalsymbol: meta.totalsymbol || String(OHLC_Symbols.length),
      };
    } catch (error) {
      console.error(`Error getting broker '${brokerName}' from Redis:`, error);
      return null;
    }
  }

  /**
   * updateBrokerStatus: update META (không set lại JSON lớn)
   */
  async updateBrokerStatus(broker, newStatus) {
    try {
      const metaKey = this.metaKey(broker);
      const exists = await this.client.exists(metaKey);
      if (!exists) return null;

      const timeUpdated = new Date().toISOString().slice(0, 19).replace('T', ' ');
      await this.client.hset(metaKey, 'status', String(newStatus), 'timeUpdated', timeUpdated);

      const meta = await this.client.hgetall(metaKey);
      return meta;
    } catch (error) {
      console.error('Lỗi update status:', error.message);
      throw error;
    }
  }

  // ==========================
  // FAST SYMBOL LOOKUPS (x10+)
  // ==========================
  /**
   * getSymbolDetails(symbolName)
   * - Không load toàn bộ OHLC_Symbols
   * - HGET từng broker theo field symbolName
   */
  async getSymbolDetails(symbolName) {
    try {
      const metaKeys = await this.scanKeys('BROKER:*:META');
      if (metaKeys.length === 0) return [];

      // lấy meta + symbol field song song
      const p = this.client.pipeline();
      for (const mk of metaKeys) {
        p.hgetall(mk);
      }
      const metaRes = await p.exec();

      const p2 = this.client.pipeline();
      const metas = [];
      for (const r of metaRes) {
        const meta = r?.[1];
        if (!meta || !meta.broker) {
          metas.push(null);
          continue;
        }
        metas.push(meta);
        p2.hget(this.symsKey(meta.broker), symbolName);
      }
      const symRes = await p2.exec();

      const symbolDetails = [];
      let symIdx = 0;

      for (const meta of metas) {
        if (!meta) continue;

        const rawSym = symRes[symIdx++]?.[1];
        if (!rawSym) continue;

        let symbolInfo = null;
        try {
          symbolInfo = JSON.parse(rawSym);
        } catch {
          continue;
        }

        // giữ logic lọc như bạn
        if (symbolInfo.trade !== 'TRUE') continue;
        if (meta.status !== 'True') continue;

        symbolDetails.push({
          Broker: meta.broker,
          Broker_: meta.broker_,
          Status: meta.status,
          Index: meta.index,
          ...symbolInfo,
        });
      }

      return symbolDetails.sort((a, b) => parseFloat(a.Index || 0) - parseFloat(b.Index || 0));
    } catch (error) {
      console.error(`Error getting symbol details for ${symbolName}:`, error);
      return [];
    }
  }

  /**
   * getMultipleSymbolDetails(symbols)
   * - HMGET theo danh sách symbols cho mỗi broker (cực nhanh)
   */
  async getMultipleSymbolDetails(symbols) {
    if (!symbols || symbols.length === 0) return new Map();

    try {
      const metaKeys = await this.scanKeys('BROKER:*:META');
      if (metaKeys.length === 0) return new Map();

      const p = this.client.pipeline();
      for (const mk of metaKeys) p.hgetall(mk);
      const metaRes = await p.exec();

      const metas = metaRes.map(r => r?.[1]).filter(m => m && m.broker);

      const resultMap = new Map();
      for (const s of symbols) resultMap.set(s, []);

      // pipeline HMGET theo broker
      const p2 = this.client.pipeline();
      for (const meta of metas) {
        if (meta.status !== 'True') continue;
        p2.hmget(this.symsKey(meta.broker), ...symbols);
      }
      const hmgetRes = await p2.exec();

      let idx = 0;
      for (const meta of metas) {
        if (meta.status !== 'True') continue;

        const arr = hmgetRes[idx++]?.[1];
        if (!Array.isArray(arr)) continue;

        for (let i = 0; i < symbols.length; i++) {
          const sym = symbols[i];
          const raw = arr[i];
          if (!raw) continue;

          let symbolInfo = null;
          try {
            symbolInfo = JSON.parse(raw);
          } catch {
            continue;
          }

          if (symbolInfo.trade !== 'TRUE') continue;

          resultMap.get(sym).push({
            Broker: meta.broker,
            Broker_: meta.broker_,
            Status: meta.status,
            Index: meta.index,
            Auto_Trade: meta.auto_trade,
            Typeaccount: meta.typeaccount,
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

  /**
   * getSymbol(symbol): tìm broker tốt nhất (index nhỏ nhất)
   * - HGET symbol theo broker (không load hết)
   */
  async getSymbol(symbol) {
    try {
      if (!symbol) throw new Error(symbol + ': Symbol is required');

      const metaKeys = await this.scanKeys('BROKER:*:META');
      if (metaKeys.length === 0) return null;

      const p = this.client.pipeline();
      for (const mk of metaKeys) p.hgetall(mk);
      const metaRes = await p.exec();

      const metas = metaRes.map(r => r?.[1]).filter(m => m && m.broker);

      // HGET symbol theo broker
      const p2 = this.client.pipeline();
      for (const meta of metas) {
        if (meta.status === 'Disconnect') continue;
        p2.hget(this.symsKey(meta.broker), symbol);
      }
      const symRes = await p2.exec();

      let best = null;
      let bestIndex = Number.MAX_SAFE_INTEGER;

      let idx = 0;
      for (const meta of metas) {
        if (meta.status === 'Disconnect') continue;

        const raw = symRes[idx++]?.[1];
        if (!raw) continue;

        let symbolInfo = null;
        try {
          symbolInfo = JSON.parse(raw);
        } catch {
          continue;
        }

        if (symbolInfo.trade !== 'TRUE') continue;

        const brokerIndex = parseInt(meta.index, 10);
        if (!isNaN(brokerIndex) && brokerIndex < bestIndex) {
          bestIndex = brokerIndex;
          best = {
            ...symbolInfo,
            Broker: meta.broker,
            BrokerIndex: meta.index,
          };
        }
      }

      return best;
    } catch (error) {
      console.error('Lỗi hàm getSymbol trong clientRedis.js', error);
      return null;
    }
  }

  // ==========================
  // Admin / maintenance
  // ==========================
  async findBrokerByIndex(index) {
    try {
      if (index === undefined || index === null) throw new Error('index is required');
      const brokers = await this.Broker_names();
      const targetIndex = String(index);
      return brokers.find(b => String(b.index) === targetIndex) || null;
    } catch (error) {
      console.error(`Error finding broker with index '${index}':`, error);
      return null;
    }
  }

  async deleteBroker(brokerName) {
    try {
      if (!brokerName) throw new Error('Broker name is required');

      const metaKey = this.metaKey(brokerName);
      const symsKey = this.symsKey(brokerName);

      const exists = await this.client.exists(metaKey);
      if (!exists) {
        return { success: false, message: `Broker "${brokerName}" does not exist` };
      }

      const p = this.client.pipeline();
      p.del(metaKey);
      p.del(symsKey);
      p.del(this.legacyKey(brokerName)); // dọn key cũ nếu có
      // dọn symbol:* cũ nếu bạn còn dùng
      const oldSymbolKeys = await this.scanKeys(`symbol:${brokerName}:*`);
      for (const k of oldSymbolKeys) p.del(k);

      await p.exec();
      return { success: true, message: `Deleted broker "${brokerName}" and related data` };
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
      const patterns = ['BROKER:*:META', 'BROKER:*:SYMS', 'BROKER:*', 'symbol:*', 'Analysis:*'];
      let totalDeleted = 0;

      for (const pattern of patterns) {
        const keys = await this.scanKeys(pattern);
        if (keys.length === 0) continue;

        // xóa theo batch
        const batchSize = 2000;
        for (let i = 0; i < keys.length; i += batchSize) {
          const batch = keys.slice(i, i + batchSize);
          await this.client.del(...batch);
        }

        log(colors.yellow, 'REDIS', colors.reset, `Deleted ${keys.length} keys matching "${pattern}"`);
        totalDeleted += keys.length;
      }

      log(colors.green, 'REDIS', colors.reset, `✅ Cleared ${totalDeleted} keys total. Ready for fresh data.`);
      return { success: true, totalDeleted };
    } catch (error) {
      log(colors.red, 'REDIS', colors.reset, `Error clearing app data: ${error.message}`);
      return { success: false, error: error.message };
    }
  }

  // ==========================
  // Legacy function (giữ lại)
  // ==========================
  async saveBrokerData_(data, port) {
    try {
      if (!data.Broker) throw new Error('Invalid broker data: Missing Broker name');

      // bạn đang dùng key cũ symbol:<broker>:<symbol> ở hàm này
      // vẫn giữ, nhưng dùng pipeline + autoPipelining đã nhanh hơn
      const brokerKey = `BROKER:${data.Broker}`;
      const now = new Date();
      const formattedDate =
        `${now.getFullYear()}.${String(now.getMonth() + 1).padStart(2, '0')}.${String(now.getDate()).padStart(2, '0')} ` +
        `${String(now.getHours()).padStart(2, '0')}:${String(now.getMinutes()).padStart(2, '0')}:${String(now.getSeconds()).padStart(2, '0')}`;

      const brokerData = { ...data, port, lastUpdated: formattedDate };

      const pipeline = this.client.pipeline();
      pipeline.set(brokerKey, JSON.stringify(brokerData));

      if (Array.isArray(data.Infosymbol)) {
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

  // ==========================
  // Reset progress tracking (giữ nguyên logic)
  // ==========================
  async startResetTracking(brokers) {
    try {
      const data = {
        brokers: brokers.map(b => ({
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
    } catch {
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

  // ✅ graceful shutdown
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

    async getBrokerResetting() {
    // Chỉ load META => nhanh hơn rất nhiều
    const metaKeys = await this.scanKeys('BROKER:*:META');
    if (metaKeys.length === 0) return [];

    const p = this.client.pipeline();
    for (const mk of metaKeys) p.hgetall(mk);
    const res = await p.exec();

    const metas = res
      .map(r => r?.[1])
      .filter(m => m && m.broker && m.status !== "True")
      .sort((a, b) => (Number(a.index) || 0) - (Number(b.index) || 0));

    return metas;
  }
}

module.exports = new RedisManager();
