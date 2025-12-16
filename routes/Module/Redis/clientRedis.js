'use strict';

const Redis = require('ioredis');
const { log, colors } = require('../Helpers/Log');

class RedisManager {
  constructor() {
    this.messageHandlers = new Map();

    const redisConfig = {
      host: '127.0.0.1', // ổn định hơn localhost trên Windows
      port: 6379,
      db: 0,

      enableAutoPipelining: true,
      maxRetriesPerRequest: null,
      keepAlive: 10000,
      connectTimeout: 10000,
      retryStrategy: (times) => Math.min(times * 50, 2000),
    };

    this.client = new Redis(redisConfig);
    this.publisherClient = this.client.duplicate();
    this.subscriberClient = this.client.duplicate();

    this.setupEventHandlers();
  }

  setupEventHandlers() {
    this.publisherClient.on('connect', () => {
      log(colors.green, 'REDIS', colors.reset, 'Publisher connected');
    });
    this.publisherClient.on('error', (err) => {
      console.error('Redis Publisher Error:', err?.message || err);
    });

    this.subscriberClient.on('connect', () => {
      log(colors.green, 'REDIS', colors.reset, 'Subscriber connected');
    });
    this.subscriberClient.on('error', (err) => {
      console.error('Redis Subscriber Error:', err?.message || err);
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

  tryParseJSON(message) {
    try {
      return JSON.parse(message);
    } catch {
      return message;
    }
  }

  // ===== SCAN SAFE =====
  async scanKeys(pattern) {
    const keys = [];
    const stream = this.client.scanStream({ match: pattern, count: 1000 });

    return await new Promise((resolve, reject) => {
      stream.on('data', (arr) => { for (const k of arr) keys.push(k); });
      stream.on('end', () => resolve(keys));
      stream.on('error', reject);
    });
  }

  // ===================== FAST + SAFE BATCH MERGE (LEGACY FORMAT) =====================
  /**
   * LƯU DẠNG CŨ: BROKER:<broker> = JSON string
   * Batch đến đâu merge đến đó (không mất dữ liệu)
   * Tối ưu: parse 1 lần + Map merge O(n)
   */
  async saveBrokerData(broker, data) {
    const key = `BROKER:${broker}`;

    // batch symbols
    const newSymbols = Array.isArray(data?.OHLC_Symbols) ? data.OHLC_Symbols : [];

    // Nếu không có symbols thì chỉ update meta nhẹ
    if (newSymbols.length === 0) {
      const raw = await this.client.get(key);
      if (!raw) {
        await this.client.set(key, JSON.stringify(data));
        return { success: true, action: 'created_meta_only' };
      }
      let oldObj = null;
      try { oldObj = JSON.parse(raw); } catch { oldObj = {}; }

      Object.assign(oldObj, {
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
      });

      await this.client.set(key, JSON.stringify(oldObj));
      return { success: true, action: 'updated_meta_only' };
    }

    // ✅ normal merge
    const raw = await this.client.get(key);

    // nếu chưa có -> set luôn
    if (!raw) {
      // đảm bảo totalsymbol đúng theo size hiện có
      if (!data.totalsymbol) data.totalsymbol = String(newSymbols.length);
      await this.client.set(key, JSON.stringify(data));
      return { success: true, action: 'created', added: newSymbols.length, updated: 0 };
    }

    let oldObj;
    try {
      oldObj = JSON.parse(raw);
    } catch {
      oldObj = {};
    }

    const oldArr = Array.isArray(oldObj.OHLC_Symbols) ? oldObj.OHLC_Symbols : [];

    // ✅ Map merge: nhanh
    const map = new Map();
    for (const s of oldArr) {
      if (s && s.symbol) map.set(String(s.symbol), s);
    }

    let added = 0;
    let updated = 0;

    for (const s of newSymbols) {
      if (!s || !s.symbol) continue;
      const sym = String(s.symbol);
      if (map.has(sym)) updated++;
      else added++;
      map.set(sym, s);
    }

    // update meta (giữ y hệt bạn)
    Object.assign(oldObj, {
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
    });

    oldObj.OHLC_Symbols = Array.from(map.values());
    oldObj.totalsymbol = String(oldObj.OHLC_Symbols.length);

    await this.client.set(key, JSON.stringify(oldObj));

    return { success: true, action: 'merged', added, updated, total: oldObj.OHLC_Symbols.length };
  }

  async saveConfigAdmin(data) {
    await this.client.set('CONFIG', JSON.stringify(data));
  }

  async getConfigAdmin() {
    const raw = await this.client.get('CONFIG');
    if (!raw) return null;
    try { return JSON.parse(raw); } catch { return null; }
  }

  async getAllBrokers() {
    try {
      const keys = await this.scanKeys('BROKER:*');
      if (keys.length === 0) return [];

      const values = await this.client.mget(keys);

      const brokers = values
        .map((v) => {
          if (!v) return null;
          try { return JSON.parse(v); } catch { return null; }
        })
        .filter(Boolean);

      return brokers.sort((a, b) => (parseInt(a.index, 10) || 0) - (parseInt(b.index, 10) || 0));
    } catch (e) {
      console.error('getAllBrokers error:', e);
      return [];
    }
  }

  async getAllUniqueSymbols() {
    try {
      const brokers = await this.getAllBrokers();
      const unique = new Set();
      for (const b of brokers) {
        const arr = Array.isArray(b.OHLC_Symbols) ? b.OHLC_Symbols : [];
        for (const s of arr) if (s?.symbol) unique.add(s.symbol);
      }
      return Array.from(unique);
    } catch (e) {
      console.error('getAllUniqueSymbols error:', e);
      return [];
    }
  }

  async getSymbolDetails(symbolName) {
    try {
      const brokers = await this.getAllBrokers();
      const out = [];

      for (const b of brokers) {
        if (b.status !== 'True') continue;
        const arr = Array.isArray(b.OHLC_Symbols) ? b.OHLC_Symbols : [];
        const sym = arr.find((x) => x?.symbol === symbolName && x.trade === 'TRUE');
        if (!sym) continue;

        out.push({
          Broker: b.broker,
          Broker_: b.broker_,
          Status: b.status,
          Index: b.index,
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
      const brokers = await this.getAllBrokers();
      const symbolSet = new Set(symbols);
      const resultMap = new Map();
      for (const s of symbols) resultMap.set(s, []);

      for (const b of brokers) {
        if (b.status !== 'True') continue;
        const arr = Array.isArray(b.OHLC_Symbols) ? b.OHLC_Symbols : [];

        for (const item of arr) {
          const sym = item?.symbol;
          if (!sym || !symbolSet.has(sym)) continue;
          if (item.trade !== 'TRUE') continue;

          resultMap.get(sym).push({
            Broker: b.broker,
            Broker_: b.broker_,
            Status: b.status,
            Index: b.index,
            Auto_Trade: b.auto_trade,
            Typeaccount: b.typeaccount,
            ...item,
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

  async getBrokerResetting() {
    const brokers = await this.getAllBrokers();
    return brokers
      .filter((b) => String(b.status || '').trim() !== 'True')
      .sort((a, b) => Number(a.index) - Number(b.index));
  }

  async updateBrokerStatus(broker, newStatus) {
    const key = `BROKER:${broker}`;
    const raw = await this.client.get(key);
    if (!raw) return null;

    const data = JSON.parse(raw);
    data.status = newStatus;
    data.timeUpdated = new Date().toISOString().slice(0, 19).replace('T', ' ');
    await this.client.set(key, JSON.stringify(data));
    return data;
  }

  async getSymbol(symbol) {
    try {
      const brokers = await this.getAllBrokers();
      let result = null;
      let minIndex = Number.MAX_SAFE_INTEGER;

      for (const b of brokers) {
        const idx = parseInt(b.index, 10);
        if (isNaN(idx)) continue;
        const arr = Array.isArray(b.OHLC_Symbols) ? b.OHLC_Symbols : [];
        const info = arr.find((x) => x?.symbol === symbol && x.trade === 'TRUE' && b.status !== 'Disconnect');
        if (!info) continue;

        if (idx < minIndex) {
          minIndex = idx;
          result = { ...info, Broker: b.broker, BrokerIndex: b.index };
        }
      }
      return result;
    } catch (e) {
      console.error('getSymbol error:', e);
      return null;
    }
  }

  async deleteBroker(brokerName) {
    try {
      const brokerKey = `BROKER:${brokerName}`;
      const exists = await this.client.exists(brokerKey);
      if (!exists) return { success: false, message: `Broker "${brokerName}" does not exist` };

      const symbolKeys = await this.scanKeys(`symbol:${brokerName}:*`);
      const p = this.client.pipeline();
      for (const k of symbolKeys) p.del(k);
      p.del(brokerKey);

      await p.exec();
      return { success: true, message: `Deleted broker "${brokerName}" and related symbols` };
    } catch (e) {
      return { success: false, message: e.message };
    }
  }

  async clearAllAppData() {
    try {
      const patterns = ['BROKER:*', 'symbol:*', 'Analysis:*'];
      let totalDeleted = 0;

      for (const pattern of patterns) {
        const keys = await this.scanKeys(pattern);
        if (!keys.length) continue;

        const batchSize = 1000;
        for (let i = 0; i < keys.length; i += batchSize) {
          await this.client.del(...keys.slice(i, i + batchSize));
        }
        totalDeleted += keys.length;
      }

      return { success: true, totalDeleted };
    } catch (e) {
      return { success: false, error: e.message };
    }
  }

  async clearAllBroker() {
    return this.clearAllAppData();
  }

  async clearData() {
    await this.client.flushall();
  }

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
