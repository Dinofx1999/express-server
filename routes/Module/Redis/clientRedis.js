'use strict';

const Redis = require('ioredis');
const zlib = require('zlib');
const { log, colors } = require('../Helpers/Log');

// ===================== COMPRESSION HELPERS =====================
const MAGIC = Buffer.from('BR1'); // phân biệt dữ liệu nén

function packJSON(obj) {
  const jsonBuf = Buffer.from(JSON.stringify(obj));
  const compressed = zlib.brotliCompressSync(jsonBuf, {
    params: { [zlib.constants.BROTLI_PARAM_QUALITY]: 4 }, // ưu tiên tốc độ
  });
  return Buffer.concat([MAGIC, compressed]);
}

function unpackJSON(bufOrStr) {
  // New format: MAGIC + brotli
  if (Buffer.isBuffer(bufOrStr) && bufOrStr.length > 3 && bufOrStr.subarray(0, 3).equals(MAGIC)) {
    const compressed = bufOrStr.subarray(3);
    const raw = zlib.brotliDecompressSync(compressed);
    return JSON.parse(raw.toString());
  }
  // Old format: plain JSON string
  const text = Buffer.isBuffer(bufOrStr) ? bufOrStr.toString() : String(bufOrStr);
  return JSON.parse(text);
}

// ===================== KEY BUILDERS =====================
const kMeta = (broker) => `BROKER:${broker}:META`;              // STRING (nén)
const kSymSet = (broker) => `BROKER:${broker}:SYMBOLS`;         // SET
const kSym = (broker, symbol) => `BROKER:${broker}:SYM:${symbol}`; // STRING (nén)
const kLegacy = (broker) => `BROKER:${broker}`;                 // STRING (optional legacy)

class RedisManager {
  constructor() {
    this.messageHandlers = new Map();
    this.isSubscriberSetup = false;

    const redisConfig = {
      host: 'localhost',
      port: 6379,
      db: 0,

      // ⚡ throughput
      enableAutoPipelining: true,
      autoPipeliningIgnoredCommands: ['subscribe', 'psubscribe', 'unsubscribe', 'punsubscribe'],
      maxRetriesPerRequest: null,
      keepAlive: 10000,
      retryStrategy: (times) => Math.min(times * 50, 2000),
    };

    this.client = new Redis(redisConfig);
    this.publisherClient = this.client.duplicate();
    this.subscriberClient = this.client.duplicate();

    this.setupEventHandlers();
  }

  setupEventHandlers() {
    this.publisherClient.on('connect', () => log(colors.green, 'REDIS', colors.reset, 'Publisher connected'));
    this.publisherClient.on('error', (err) => console.error('Redis Publisher Error:', err));

    this.subscriberClient.on('connect', () => log(colors.green, 'REDIS', colors.reset, 'Subscriber connected'));
    this.subscriberClient.on('error', (err) => console.error('Redis Subscriber Error:', err));

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

  tryParseJSON(message) {
    try { return JSON.parse(message); } catch { return message; }
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

  // ===================== SCAN (FAST) =====================
  async scanKeys(pattern) {
    const keys = [];
    const stream = this.client.scanStream({ match: pattern, count: 2000 });
    return await new Promise((resolve, reject) => {
      stream.on('data', (resultKeys) => { for (const k of resultKeys) keys.push(k); });
      stream.on('end', () => resolve(keys));
      stream.on('error', reject);
    });
  }

  // ===================== SAVE: BATCH FRIENDLY (NO HASH) =====================
  /**
   * saveBrokerData(broker, data)
   * - data.OHLC_Symbols có thể là 1 batch (một phần symbols)
   * - Lưu từng symbol thành key riêng (STRING) + set danh sách symbols (SET)
   * - Lưu meta (STRING) nhỏ
   */
  async saveBrokerData(broker, data) {
    const brokerName = String(broker || '').trim();
    if (!brokerName) throw new Error('broker is required');

    const symbols = Array.isArray(data?.OHLC_Symbols) ? data.OHLC_Symbols : [];
    const expectedTotal = parseInt(data?.totalsymbol || 0, 10) || 0;

    // meta nhỏ (giữ field giống bạn)
    const meta = {
      port: data?.port,
      index: data?.index,
      broker: data?.broker ?? brokerName,
      broker_: data?.broker_ ?? String(data?.broker ?? brokerName).replace(/\s+/g, '-'),
      version: data?.version,
      typeaccount: data?.typeaccount,
      timecurent: data?.timecurent,
      auto_trade: data?.auto_trade,
      status: (data?.status !== undefined ? String(data.status) : 'True').trim(),
      timeUpdated: data?.timeUpdated,
      totalsymbol: expectedTotal ? String(expectedTotal) : undefined,
    };

    const p = this.client.pipeline();

    // 1) set meta
    p.set(kMeta(brokerName), packJSON(meta));

    // 2) add symbols + save per-symbol
    if (symbols.length) {
      // SADD all symbols (one command)
      const symNames = [];
      for (const s of symbols) {
        if (s && s.symbol) symNames.push(String(s.symbol));
      }
      if (symNames.length) p.sadd(kSymSet(brokerName), ...symNames);

      // SET each symbol (chunk to avoid huge packet)
      const CHUNK = 200;
      for (let i = 0; i < symbols.length; i += CHUNK) {
        const slice = symbols.slice(i, i + CHUNK);
        for (const s of slice) {
          if (!s || !s.symbol) continue;
          p.set(kSym(brokerName, String(s.symbol)), packJSON(s));
        }
      }
    }

    // 3) optionally store legacy full snapshot if client really sends full (rare in batch mode)
    // Nếu bạn muốn giữ tương thích API cũ “BROKER:<name>” full JSON:
    // chỉ lưu khi symbols.length >= expectedTotal (coi như full)
    if (expectedTotal > 0 && symbols.length >= expectedTotal) {
      p.set(kLegacy(brokerName), packJSON(data));
    }

    await p.exec();

    return {
      success: true,
      action: 'batch_saved',
      batchSymbols: symbols.length,
      expectedTotal,
    };
  }

  // ===================== GET: META + RECONSTRUCT SYMBOLS =====================
  async getBroker(brokerName) {
    try {
      if (!brokerName) throw new Error('Broker name is required');

      const metaBuf = await this.client.getBuffer(kMeta(brokerName));
      if (!metaBuf) return null;

      const meta = unpackJSON(metaBuf);

      const syms = await this.client.smembers(kSymSet(brokerName));
      if (!syms || syms.length === 0) {
        return { ...meta, OHLC_Symbols: [], totalsymbol: meta.totalsymbol || '0' };
      }

      // MGET by chunks
      const OHLC_Symbols = [];
      const CHUNK = 500;
      for (let i = 0; i < syms.length; i += CHUNK) {
        const part = syms.slice(i, i + CHUNK);
        const keys = part.map((s) => kSym(brokerName, s));
        const bufs = await this.client.mgetBuffer(keys);

        for (const b of bufs) {
          if (!b) continue;
          try { OHLC_Symbols.push(unpackJSON(b)); } catch {}
        }
      }

      return {
        ...meta,
        OHLC_Symbols,
        totalsymbol: meta.totalsymbol || String(syms.length),
      };
    } catch (error) {
      console.error(`Error getting broker '${brokerName}' from Redis:`, error);
      return null;
    }
  }

  // ✅ nhanh: chỉ meta để biết broker nào đang lỗi/reset
  async getBrokerResetting() {
    const metaKeys = await this.scanKeys('BROKER:*:META');
    if (metaKeys.length === 0) return [];

    const p = this.client.pipeline();
    for (const mk of metaKeys) p.getBuffer(mk);
    const res = await p.exec();

    const metas = res
      .map(r => r?.[1])
      .filter(Boolean)
      .map(buf => {
        try { return unpackJSON(buf); } catch { return null; }
      })
      .filter(Boolean);

    return metas
      .filter(m => String(m.status || '').trim() !== 'True')
      .sort((a, b) => (Number(a.index) || 0) - (Number(b.index) || 0));
  }

  // ⚠️ nặng hơn: reconstruct full brokers (dùng cho UI)
  async getAllBrokers() {
    try {
      const metaKeys = await this.scanKeys('BROKER:*:META');
      if (metaKeys.length === 0) return [];

      // parse metas
      const p = this.client.pipeline();
      for (const mk of metaKeys) p.getBuffer(mk);
      const res = await p.exec();

      const metas = res
        .map(r => r?.[1])
        .filter(Boolean)
        .map(buf => {
          try { return unpackJSON(buf); } catch { return null; }
        })
        .filter(Boolean);

      // reconstruct each broker symbols
      const brokers = [];
      for (const meta of metas) {
        const name = meta.broker || meta.broker_ || '';
        if (!name) continue;

        const syms = await this.client.smembers(kSymSet(name));
        const OHLC_Symbols = [];
        if (syms && syms.length) {
          const CHUNK = 500;
          for (let i = 0; i < syms.length; i += CHUNK) {
            const part = syms.slice(i, i + CHUNK);
            const keys = part.map((s) => kSym(name, s));
            const bufs = await this.client.mgetBuffer(keys);
            for (const b of bufs) {
              if (!b) continue;
              try { OHLC_Symbols.push(unpackJSON(b)); } catch {}
            }
          }
        }

        brokers.push({
          ...meta,
          OHLC_Symbols,
          totalsymbol: meta.totalsymbol || String(syms?.length || 0),
        });
      }

      return brokers.sort((a, b) => (parseInt(a.index, 10) || 0) - (parseInt(b.index, 10) || 0));
    } catch (error) {
      console.error('Error getting brokers from Redis:', error);
      return [];
    }
  }

  async getAllBrokers_2() {
    const arr = await this.getAllBrokers();
    const obj = {};
    for (const b of arr) obj[b.broker] = b;
    return obj;
  }

  async getAllUniqueSymbols() {
    try {
      const brokers = await this.getAllBrokers();
      const unique = new Set();
      for (const b of brokers) {
        if (Array.isArray(b.OHLC_Symbols)) {
          for (const s of b.OHLC_Symbols) if (s?.symbol) unique.add(s.symbol);
        }
      }
      return Array.from(unique);
    } catch (e) {
      console.error('Error getting unique symbols:', e);
      return [];
    }
  }

  // ✅ lookup 1 symbol: scan META rồi GET key per symbol (không cần load full)
  async getSymbolDetails(symbolName) {
    try {
      const metaKeys = await this.scanKeys('BROKER:*:META');
      if (metaKeys.length === 0) return [];

      const p = this.client.pipeline();
      for (const mk of metaKeys) p.getBuffer(mk);
      const res = await p.exec();

      const metas = res
        .map(r => r?.[1])
        .filter(Boolean)
        .map(buf => { try { return unpackJSON(buf); } catch { return null; } })
        .filter(Boolean);

      const out = [];
      const p2 = this.client.pipeline();

      // hứng kết quả theo thứ tự
      const validMetas = [];
      for (const meta of metas) {
        if (meta.status !== 'True') continue;
        validMetas.push(meta);
        p2.getBuffer(kSym(meta.broker, symbolName));
      }

      const res2 = await p2.exec();

      for (let i = 0; i < validMetas.length; i++) {
        const meta = validMetas[i];
        const buf = res2[i]?.[1];
        if (!buf) continue;

        let sym = null;
        try { sym = unpackJSON(buf); } catch { sym = null; }
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
      console.error(`Error getSymbolDetails(${symbolName}):`, e);
      return [];
    }
  }

  async getMultipleSymbolDetails(symbols) {
    if (!symbols || symbols.length === 0) return new Map();

    try {
      const metaKeys = await this.scanKeys('BROKER:*:META');
      if (metaKeys.length === 0) return new Map();

      const p = this.client.pipeline();
      for (const mk of metaKeys) p.getBuffer(mk);
      const res = await p.exec();

      const metas = res
        .map(r => r?.[1])
        .filter(Boolean)
        .map(buf => { try { return unpackJSON(buf); } catch { return null; } })
        .filter(Boolean)
        .filter(m => m.status === 'True');

      const resultMap = new Map();
      for (const s of symbols) resultMap.set(s, []);

      // pipeline: mỗi broker MGETBuffer các symbol keys
      for (const meta of metas) {
        const keys = symbols.map(sym => kSym(meta.broker, sym));
        const bufs = await this.client.mgetBuffer(keys);

        for (let i = 0; i < symbols.length; i++) {
          const symName = symbols[i];
          const buf = bufs[i];
          if (!buf) continue;

          let sym = null;
          try { sym = unpackJSON(buf); } catch { sym = null; }
          if (!sym) continue;
          if (sym.trade !== 'TRUE') continue;

          resultMap.get(symName).push({
            Broker: meta.broker,
            Broker_: meta.broker_,
            Status: meta.status,
            Index: meta.index,
            Auto_Trade: meta.auto_trade,
            Typeaccount: meta.typeaccount,
            ...sym,
          });
        }
      }

      for (const [sym, details] of resultMap) {
        details.sort((a, b) => parseFloat(a.Index || 0) - parseFloat(b.Index || 0));
      }

      return resultMap;
    } catch (e) {
      console.error('Error in getMultipleSymbolDetails:', e);
      return new Map();
    }
  }

  async getSymbol(symbol) {
    try {
      if (!symbol) throw new Error('Symbol is required');

      const metaKeys = await this.scanKeys('BROKER:*:META');
      if (metaKeys.length === 0) return null;

      const p = this.client.pipeline();
      for (const mk of metaKeys) p.getBuffer(mk);
      const res = await p.exec();

      const metas = res
        .map(r => r?.[1])
        .filter(Boolean)
        .map(buf => { try { return unpackJSON(buf); } catch { return null; } })
        .filter(Boolean)
        .filter(m => m.status !== 'Disconnect');

      let best = null;
      let bestIndex = Number.MAX_SAFE_INTEGER;

      for (const meta of metas) {
        const brokerIndex = parseInt(meta.index, 10);
        if (isNaN(brokerIndex)) continue;

        const buf = await this.client.getBuffer(kSym(meta.broker, symbol));
        if (!buf) continue;

        let sym = null;
        try { sym = unpackJSON(buf); } catch { sym = null; }
        if (!sym) continue;
        if (sym.trade !== 'TRUE') continue;

        if (brokerIndex < bestIndex) {
          bestIndex = brokerIndex;
          best = { ...sym, Broker: meta.broker, BrokerIndex: meta.index };
        }
      }

      return best;
    } catch (e) {
      console.error('Error getSymbol:', e);
      return null;
    }
  }

  async findBrokerByIndex(index) {
    try {
      const metas = await this.getBrokerResetting(); // meta list already sorted
      const target = String(index);
      return metas.find(m => String(m.index) === target) || null;
    } catch (e) {
      console.error('Error findBrokerByIndex:', e);
      return null;
    }
  }

  async updateBrokerStatus(broker, newStatus) {
    try {
      const metaBuf = await this.client.getBuffer(kMeta(broker));
      if (!metaBuf) return null;

      const meta = unpackJSON(metaBuf);
      meta.status = String(newStatus);
      meta.timeUpdated = new Date().toISOString().slice(0, 19).replace('T', ' ');

      await this.client.set(kMeta(broker), packJSON(meta));
      return meta;
    } catch (e) {
      console.error('Lỗi update status:', e?.message || e);
      throw e;
    }
  }

  async Broker_names() {
    // chỉ meta cho nhẹ
    const list = await this.getBrokerResetting(); // meta list (status != True)
    return list.map(({ OHLC_Symbols, ...info }) => info);
  }

  // ===================== CONFIG / ANALYSIS (GIỮ NHƯ CŨ) =====================
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

  // ===================== DELETE / CLEAR =====================
  async deleteBroker(brokerName) {
    try {
      if (!brokerName) throw new Error('Broker name is required');

      const syms = await this.client.smembers(kSymSet(brokerName));
      const p = this.client.pipeline();

      // delete symbols
      if (syms && syms.length) {
        for (const s of syms) p.del(kSym(brokerName, s));
      }

      p.del(kSymSet(brokerName));
      p.del(kMeta(brokerName));
      p.del(kLegacy(brokerName)); // nếu có

      // dọn legacy symbol:* nếu bạn còn dùng nơi khác
      const legacySymbolKeys = await this.scanKeys(`symbol:${brokerName}:*`);
      for (const k of legacySymbolKeys) p.del(k);

      await p.exec();
      return { success: true, message: `Deleted broker "${brokerName}" and related data` };
    } catch (e) {
      console.error('Error deleting broker:', e);
      return { success: false, message: e.message };
    }
  }

  async clearData() {
    await this.client.flushall();
    log(colors.green, 'REDIS', colors.reset, 'Redis data cleared successfully.');
  }

  async clearAllAppData() {
    const patterns = ['BROKER:*:META', 'BROKER:*:SYMBOLS', 'BROKER:*:SYM:*', 'BROKER:*', 'symbol:*', 'Analysis:*'];
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
      log(colors.yellow, 'REDIS', colors.reset, `Deleted ${keys.length} keys matching "${pattern}"`);
    }

    log(colors.green, 'REDIS', colors.reset, `✅ Cleared ${totalDeleted} keys total.`);
    return { success: true, totalDeleted };
  }

  async clearAllBroker() {
    return this.clearAllAppData();
  }

  // ===================== RESET PROGRESS TRACKING (GIỮ NGUYÊN) =====================
  async startResetTracking(brokers) {
    const data = {
      brokers: brokers.map(b => ({ name: b.broker_ || b.broker, percentage: 0, completed: false })),
      currentIndex: 0,
      startedAt: Date.now(),
    };
    await this.client.setex('reset_progress', 3600, JSON.stringify(data));
    return true;
  }

  async updateResetProgress(brokerName, percentage) {
    const data = await this.client.get('reset_progress');
    if (!data) return false;

    const progress = JSON.parse(data);
    const broker = progress.brokers.find(b => b.name === brokerName);
    if (!broker) return false;

    broker.percentage = percentage;
    if (percentage >= 30) broker.completed = true;

    await this.client.setex('reset_progress', 3600, JSON.stringify(progress));
    return true;
  }

  async isResetCompleted(brokerName) {
    const data = await this.client.get('reset_progress');
    if (!data) return false;
    const progress = JSON.parse(data);
    const broker = progress.brokers.find(b => b.name === brokerName);
    return broker ? broker.completed : false;
  }

  async isResetting() {
    return (await this.client.exists('reset_progress')) === 1;
  }

  async getResetStatus() {
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
  }

  async clearResetTracking() {
    await this.client.del('reset_progress');
  }

  // ===================== SHUTDOWN =====================
  async disconnect() {
    try {
      this.messageHandlers.clear();
      await this.publisherClient.quit();
      await this.subscriberClient.quit();
      await this.client.quit();
      log(colors.yellow, 'REDIS', colors.reset, 'Disconnected gracefully');
    } catch (e) {
      console.error('Error disconnecting:', e);
    }
  }
}

module.exports = new RedisManager();
