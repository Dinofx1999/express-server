'use strict';

const Redis = require('ioredis');
const { log, colors } = require('../Helpers/Log');

class RedisManager {
  constructor() {
    this.messageHandlers = new Map();

    const redisConfig = {
      host: 'localhost',
      port: 6379,
      db: 0,

      // ⚡ tối ưu throughput
      enableAutoPipelining: true,
      autoPipeliningIgnoredCommands: ['subscribe', 'psubscribe', 'unsubscribe', 'punsubscribe'],
      maxRetriesPerRequest: null,
      keepAlive: 10000,
      retryStrategy: (times) => Math.min(times * 50, 2000),
    };

    // ✅ client chính
    this.client = new Redis(redisConfig);

    // ✅ pub/sub tách connection
    this.publisherClient = this.client.duplicate();
    this.subscriberClient = this.client.duplicate();

    this.setupEventHandlers();
  }

  // ===================== KEYS =====================
  _kBroker(broker) { return `BROKER:${broker}`; }                   // legacy full snapshot (JSON string)
  _kMeta(broker) { return `BROKER:${broker}:META`; }               // meta nhỏ (JSON string)
  _kSymSet(broker) { return `BROKER:${broker}:SYMBOLS`; }          // SET of symbol names
  _kSym(broker, sym) { return `BROKER:${broker}:SYM:${sym}`; }     // per-symbol JSON string
  _kReset() { return 'reset_progress'; }

  // ===================== EVENTS =====================
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
        handler(this.tryParseJSON(message));
      } catch (error) {
        log(colors.red, 'REDIS', colors.reset, `Message processing error: ${error.message}`);
      }
    });
  }

  tryParseJSON(message) {
    try { return JSON.parse(message); } catch { return message; }
  }

  subscribe(channel, callback) {
    this.messageHandlers.set(channel, callback);
    this.subscriberClient.subscribe(channel);
  }

  unsubscribe(channel) {
    this.messageHandlers.delete(channel);
    this.subscriberClient.unsubscribe(channel);
  }

  async publish(channel, message) {
    const payload = typeof message === 'object' ? JSON.stringify(message) : message;
    return this.publisherClient.publish(channel, payload);
  }

  // ===================== SCAN (FAST) =====================
  async scanKeys(pattern) {
    const keys = [];
    const stream = this.client.scanStream({ match: pattern, count: 2000 });

    return await new Promise((resolve, reject) => {
      stream.on('data', (arr) => { for (const k of arr) keys.push(k); });
      stream.on('end', () => resolve(keys));
      stream.on('error', reject);
    });
  }

  // ===================== CORE SAVE (BATCH OPTIMIZED) =====================
  /**
   * Batch-friendly:
   * - Không merge JSON lớn mỗi batch nữa
   * - Lưu per-symbol + set symbol list + meta
   * - Khi đủ symbols => build legacy full snapshot BROKER:<broker> để tương thích code cũ
   */
  async saveBrokerData(broker, data) {
    const brokerKey = String(broker || '').trim();
    if (!brokerKey) throw new Error('broker is required');

    const expectedTotal = parseInt(data?.totalsymbol || 0, 10) || 0;
    const symbols = Array.isArray(data?.OHLC_Symbols) ? data.OHLC_Symbols : [];

    // ✅ meta nhỏ, nhớ: brokerKey chính là key để đọc/ghi (không bị lệch formatString)
    const meta = {
      broker_key: brokerKey,
      broker: data?.broker ?? brokerKey,
      broker_: data?.broker_ ?? String(data?.broker ?? brokerKey).replace(/\s+/g, '-'),
      port: data?.port,
      index: data?.index,
      version: data?.version,
      typeaccount: data?.typeaccount,
      timecurent: data?.timecurent,
      auto_trade: data?.auto_trade,
      status: (data?.status !== undefined ? String(data.status) : 'True').trim(),
      timeUpdated: data?.timeUpdated,
      totalsymbol: expectedTotal ? String(expectedTotal) : undefined,
    };

    // ✅ pipeline cực nhanh
    const p = this.client.pipeline();

    // 1) save meta
    p.set(this._kMeta(brokerKey), JSON.stringify(meta));

    // 2) save symbols in this batch
    if (symbols.length) {
      const symNames = [];
      for (const s of symbols) {
        if (!s || !s.symbol) continue;
        const sym = String(s.symbol);
        symNames.push(sym);
        p.set(this._kSym(brokerKey, sym), JSON.stringify(s)); // per-symbol
      }
      if (symNames.length) p.sadd(this._kSymSet(brokerKey), ...symNames);
    }

    await p.exec();

    // 3) Nếu đã đủ symbols => build full snapshot legacy 1 lần
    // (để getAllBrokers / UI cũ không bị thiếu)
    if (expectedTotal > 0) {
      const currentCount = await this.client.scard(this._kSymSet(brokerKey));
      if (currentCount >= expectedTotal) {
        await this._buildLegacySnapshot(brokerKey, meta);
        return { success: true, action: 'batch_saved_full_ready', currentCount, expectedTotal };
      }
      return { success: true, action: 'batch_saved_partial', currentCount, expectedTotal };
    }

    return { success: true, action: 'batch_saved_no_total', batchSymbols: symbols.length };
  }

  // Build lại BROKER:<broker> (full JSON) chỉ khi đủ dữ liệu
  async _buildLegacySnapshot(brokerKey, metaObj) {
    try {
      const syms = await this.client.smembers(this._kSymSet(brokerKey));
      if (!syms || syms.length === 0) return false;

      const OHLC_Symbols = [];
      const CHUNK = 500;

      for (let i = 0; i < syms.length; i += CHUNK) {
        const part = syms.slice(i, i + CHUNK);
        const keys = part.map((s) => this._kSym(brokerKey, s));
        const values = await this.client.mget(keys);

        for (const v of values) {
          if (!v) continue;
          try { OHLC_Symbols.push(JSON.parse(v)); } catch {}
        }
      }

      // legacy object giống bạn đang dùng
      const full = {
        ...metaObj,
        broker: metaObj.broker,
        broker_: metaObj.broker_,
        totalsymbol: metaObj.totalsymbol || String(syms.length),
        OHLC_Symbols,
      };

      await this.client.set(this._kBroker(brokerKey), JSON.stringify(full));
      return true;
    } catch {
      return false;
    }
  }

  // ===================== READ COMPAT (ƯU TIÊN LEGACY, FALLBACK NEW) =====================
  async getBroker(brokerName) {
    try {
      if (!brokerName) throw new Error('Broker name is required');

      // 1) ưu tiên legacy full (nhanh cho UI cũ)
      const legacy = await this.client.get(this._kBroker(brokerName));
      if (legacy) {
        try { return JSON.parse(legacy); } catch {}
      }

      // 2) fallback: build on-the-fly từ meta + symbols
      const metaRaw = await this.client.get(this._kMeta(brokerName));
      if (!metaRaw) return null;
      const meta = JSON.parse(metaRaw);

      const syms = await this.client.smembers(this._kSymSet(brokerName));
      if (!syms || syms.length === 0) {
        return { ...meta, OHLC_Symbols: [], totalsymbol: meta.totalsymbol || '0' };
      }

      const OHLC_Symbols = [];
      const CHUNK = 500;
      for (let i = 0; i < syms.length; i += CHUNK) {
        const part = syms.slice(i, i + CHUNK);
        const keys = part.map((s) => this._kSym(brokerName, s));
        const values = await this.client.mget(keys);
        for (const v of values) {
          if (!v) continue;
          try { OHLC_Symbols.push(JSON.parse(v)); } catch {}
        }
      }

      return { ...meta, OHLC_Symbols, totalsymbol: meta.totalsymbol || String(syms.length) };
    } catch (e) {
      console.error('getBroker error:', e);
      return null;
    }
  }

  async getAllBrokers() {
    try {
      // ✅ lấy danh sách broker từ META keys (nhanh + đúng)
      const metaKeys = await this.scanKeys('BROKER:*:META');
      if (metaKeys.length === 0) return [];

      const metasRaw = await this.client.mget(metaKeys);
      const metas = metasRaw
        .map((x) => { try { return x ? JSON.parse(x) : null; } catch { return null; } })
        .filter(Boolean);

      // ✅ ưu tiên đọc legacy full nếu có, fallback build
      const brokers = [];
      for (const meta of metas) {
        const bk = meta.broker_key || meta.broker;
        if (!bk) continue;

        const legacy = await this.client.get(this._kBroker(bk));
        if (legacy) {
          try {
            brokers.push(JSON.parse(legacy));
            continue;
          } catch {}
        }

        // fallback
        const b = await this.getBroker(bk);
        if (b) brokers.push(b);
      }

      return brokers.sort((a, b) => (parseInt(a.index, 10) || 0) - (parseInt(b.index, 10) || 0));
    } catch (e) {
      console.error('getAllBrokers error:', e);
      return [];
    }
  }

  // ✅ nhanh: chỉ meta để check broker đang lỗi/reset
  async getBrokerResetting() {
    const metaKeys = await this.scanKeys('BROKER:*:META');
    if (metaKeys.length === 0) return [];

    const metasRaw = await this.client.mget(metaKeys);
    const metas = metasRaw
      .map((x) => { try { return x ? JSON.parse(x) : null; } catch { return null; } })
      .filter(Boolean);

    return metas
      .filter((m) => String(m.status || '').trim() !== 'True')
      .sort((a, b) => (Number(a.index) || 0) - (Number(b.index) || 0));
  }

  // ===================== SYMBOL LOOKUP (FAST, NO FULL LOAD) =====================
  async getSymbolDetails(symbolName) {
    try {
      const metaKeys = await this.scanKeys('BROKER:*:META');
      if (metaKeys.length === 0) return [];

      const metasRaw = await this.client.mget(metaKeys);
      const metas = metasRaw
        .map((x) => { try { return x ? JSON.parse(x) : null; } catch { return null; } })
        .filter(Boolean)
        .filter((m) => String(m.status).trim() === 'True');

      const out = [];
      for (const meta of metas) {
        const bk = meta.broker_key || meta.broker;
        if (!bk) continue;

        const raw = await this.client.get(this._kSym(bk, symbolName));
        if (!raw) continue;

        let sym = null;
        try { sym = JSON.parse(raw); } catch { sym = null; }
        if (!sym) continue;

        if (sym.trade !== 'TRUE') continue;

        out.push({
          Broker: meta.broker,
          Broker_: meta.broker_,
          Status: meta.status,
          Index: meta.index,
          ...sym,
        });
      }

      return out.sort((a, b) => parseFloat(a.Index || 0) - parseFloat(b.Index || 0));
    } catch (e) {
      console.error('getSymbolDetails error:', e);
      return [];
    }
  }

  async getMultipleSymbolDetails(symbols) {
    if (!symbols || symbols.length === 0) return new Map();

    try {
      const metaKeys = await this.scanKeys('BROKER:*:META');
      if (metaKeys.length === 0) return new Map();

      const metasRaw = await this.client.mget(metaKeys);
      const metas = metasRaw
        .map((x) => { try { return x ? JSON.parse(x) : null; } catch { return null; } })
        .filter(Boolean)
        .filter((m) => String(m.status).trim() === 'True');

      const resultMap = new Map();
      for (const s of symbols) resultMap.set(s, []);

      for (const meta of metas) {
        const bk = meta.broker_key || meta.broker;
        if (!bk) continue;

        const keys = symbols.map((sym) => this._kSym(bk, sym));
        const values = await this.client.mget(keys);

        for (let i = 0; i < symbols.length; i++) {
          const v = values[i];
          if (!v) continue;
          let symObj = null;
          try { symObj = JSON.parse(v); } catch { symObj = null; }
          if (!symObj) continue;
          if (symObj.trade !== 'TRUE') continue;

          resultMap.get(symbols[i]).push({
            Broker: meta.broker,
            Broker_: meta.broker_,
            Status: meta.status,
            Index: meta.index,
            Auto_Trade: meta.auto_trade,
            Typeaccount: meta.typeaccount,
            ...symObj,
          });
        }
      }

      for (const [sym, details] of resultMap) {
        details.sort((a, b) => parseFloat(a.Index || 0) - parseFloat(b.Index || 0));
      }

      return resultMap;
    } catch (e) {
      console.error('getMultipleSymbolDetails error:', e);
      return new Map();
    }
  }

  async getSymbol(symbol) {
    try {
      if (!symbol) throw new Error('Symbol is required');

      const metaKeys = await this.scanKeys('BROKER:*:META');
      if (metaKeys.length === 0) return null;

      const metasRaw = await this.client.mget(metaKeys);
      const metas = metasRaw
        .map((x) => { try { return x ? JSON.parse(x) : null; } catch { return null; } })
        .filter(Boolean)
        .filter((m) => String(m.status).trim() !== 'Disconnect');

      let best = null;
      let bestIndex = Number.MAX_SAFE_INTEGER;

      for (const meta of metas) {
        const bk = meta.broker_key || meta.broker;
        if (!bk) continue;

        const brokerIndex = parseInt(meta.index, 10);
        if (isNaN(brokerIndex)) continue;

        const raw = await this.client.get(this._kSym(bk, symbol));
        if (!raw) continue;

        let symObj = null;
        try { symObj = JSON.parse(raw); } catch { symObj = null; }
        if (!symObj) continue;
        if (symObj.trade !== 'TRUE') continue;

        if (brokerIndex < bestIndex) {
          bestIndex = brokerIndex;
          best = { ...symObj, Broker: meta.broker, BrokerIndex: meta.index };
        }
      }

      return best;
    } catch (e) {
      console.error('getSymbol error:', e);
      return null;
    }
  }

  // ===================== CONFIG / ANALYSIS (GIỮ NGUYÊN) =====================
  async saveConfigAdmin(data) {
    await this.client.set('CONFIG', JSON.stringify(data));
  }

  async getConfigAdmin() {
    const raw = await this.client.get('CONFIG');
    if (!raw) return null;
    try { return JSON.parse(raw); } catch { return null; }
  }

  async saveAnalysis(data) {
    await this.client.set('Analysis', JSON.stringify(data));
  }

  async getAnalysis() {
    const raw = await this.client.get('Analysis');
    if (!raw) return { Type_1: [], Type_2: [], time_analysis: null };
    try { return JSON.parse(raw); } catch { return { Type_1: [], Type_2: [], time_analysis: null }; }
  }

  // ===================== CLEAR / DELETE =====================
  async deleteBroker(brokerName) {
    try {
      const bk = String(brokerName || '').trim();
      if (!bk) throw new Error('Broker name is required');

      const syms = await this.client.smembers(this._kSymSet(bk));
      const p = this.client.pipeline();

      if (syms && syms.length) {
        for (const s of syms) p.del(this._kSym(bk, s));
      }

      p.del(this._kSymSet(bk));
      p.del(this._kMeta(bk));
      p.del(this._kBroker(bk)); // legacy

      await p.exec();
      return { success: true, message: `Deleted broker "${bk}"` };
    } catch (e) {
      console.error('deleteBroker error:', e);
      return { success: false, message: e.message };
    }
  }

  async clearAllAppData() {
    const patterns = ['BROKER:*:META', 'BROKER:*:SYMBOLS', 'BROKER:*:SYM:*', 'BROKER:*', 'Analysis:*', 'symbol:*'];
    let totalDeleted = 0;

    for (const pattern of patterns) {
      const keys = await this.scanKeys(pattern);
      if (!keys.length) continue;

      const batchSize = 2000;
      for (let i = 0; i < keys.length; i += batchSize) {
        const batch = keys.slice(i, i + batchSize);
        await this.client.del(...batch);
      }
      totalDeleted += keys.length;
    }

    return { success: true, totalDeleted };
  }

  async clearAllBroker() {
    return this.clearAllAppData();
  }

  async clearData() {
    await this.client.flushall();
    log(colors.green, 'REDIS', colors.reset, 'Redis data cleared successfully.');
  }

  // ===================== RESET TRACKING (GIỮ NGUYÊN) =====================
  async startResetTracking(brokers) {
    const data = {
      brokers: brokers.map((b) => ({
        name: b.broker_ || b.broker,
        percentage: 0,
        completed: false,
      })),
      currentIndex: 0,
      startedAt: Date.now(),
    };
    await this.client.setex(this._kReset(), 3600, JSON.stringify(data));
    return true;
  }

  async updateResetProgress(brokerName, percentage) {
    const raw = await this.client.get(this._kReset());
    if (!raw) return false;

    const progress = JSON.parse(raw);
    const b = progress.brokers.find((x) => x.name === brokerName);
    if (!b) return false;

    b.percentage = percentage;
    if (percentage >= 30) b.completed = true;

    await this.client.setex(this._kReset(), 3600, JSON.stringify(progress));
    return true;
  }

  async isResetCompleted(brokerName) {
    const raw = await this.client.get(this._kReset());
    if (!raw) return false;
    const progress = JSON.parse(raw);
    const b = progress.brokers.find((x) => x.name === brokerName);
    return b ? b.completed : false;
  }

  async isResetting() {
    return (await this.client.exists(this._kReset())) === 1;
  }

  async getResetStatus() {
    const raw = await this.client.get(this._kReset());
    if (!raw) return null;

    const progress = JSON.parse(raw);
    const completed = progress.brokers.filter((b) => b.completed).length;
    const total = progress.brokers.length;

    return {
      isRunning: true,
      progress: `${completed}/${total}`,
      percentage: Math.round((completed / total) * 100),
      startedAt: progress.startedAt,
      brokers: progress.brokers,
    };
  }

  async clearResetTracking() {
    await this.client.del(this._kReset());
  }

  // ===================== SHUTDOWN =====================
  async disconnect() {
    try {
      this.messageHandlers.clear();
      await this.publisherClient.quit();
      await this.subscriberClient.quit();
      await this.client.quit();
    } catch (e) {
      console.error('disconnect error:', e);
    }
  }
}

module.exports = new RedisManager();
