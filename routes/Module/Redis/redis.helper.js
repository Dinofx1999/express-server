// redis.helper.js (FULL - keep old APIs + on-demand OHLC realtime via PubSub)
'use strict';

const Redis = require('ioredis');
const zlib = require('zlib');
const fs = require('fs');
const path = require('path');

// =====================
// DEFAULT CONFIG
// =====================
const DEFAULTS = {
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: Number(process.env.REDIS_PORT || 6379),
  db: Number(process.env.REDIS_DB || 0),
  password: process.env.REDIS_PASSWORD || undefined,

  lazyConnect: true,
  maxRetriesPerRequest: 2,

  compress: String(process.env.REDIS_COMPRESS || '0') === '1',

  // TTL for base meta/symbols (0 = no expire)
  ttlSeconds: Number(process.env.REDIS_TTL_SECONDS || 0),

  // ON-DEMAND chart watcher TTL (avoid leak if FE crash)
  watchTTL: Number(process.env.REDIS_WATCH_TTL || 60), // seconds

  // Streams (keep compatibility - optional)
  streamEnabled: String(process.env.REDIS_STREAM_ENABLED || '0') === '1',
  streamMaxLen: Number(process.env.REDIS_STREAM_MAXLEN || 2000),
};

let _redis = null;
let _sub = null;
let _pub = null;
let _cfg = { ...DEFAULTS };

// =====================
// UTILS
// =====================
function nowMs() { return Date.now(); }

function safeKeyPart(s) {
  return String(s || '')
    .trim()
    .replace(/\s+/g, '-')
    .replace(/[^a-zA-Z0-9._:-]/g, '');
}

function keySymbols(broker_) { return `broker:${safeKeyPart(broker_)}:symbols`; }
function keyMeta(broker_) { return `broker:${safeKeyPart(broker_)}:meta`; }
function keyStream(broker_) { return `broker:${safeKeyPart(broker_)}:stream`; }
function keySymbolMeta(broker_) {
  return `broker:${safeKeyPart(broker_)}:meta_symbol`;
}

// ON-DEMAND: store OHLC separate key
function keyChartOHLC(broker_, sym, tf) {
  return `chart:ohlc:${safeKeyPart(broker_)}:${String(tf).toUpperCase()}:${String(sym).toUpperCase()}`;
}

function keyTick(broker_, symbol) {
  return `tick:${safeKeyPart(broker_)}:${String(symbol || '').toUpperCase()}`;
}

async function publishTickFromSymbol(broker_, symbolObj) {
  await ensureConnectedPubSub();
  const pub = getPublisher();

  const sym = String(symbolObj?.symbol || symbolObj?.symbol_raw || '').toUpperCase();
  if (!sym) return;

  // chỉ publish khi trade TRUE (tuỳ bạn)
  if (String(symbolObj.trade || '').toUpperCase() !== 'TRUE') return;

  // payload realtime nhẹ, KHÔNG có ohlc
  const payload = {
    broker_: safeKeyPart(broker_),
    symbol: sym,
    bid: symbolObj.bid,
    ask: symbolObj.ask,
    bid_mdf: symbolObj.bid_mdf,
    ask_mdf: symbolObj.ask_mdf,
    digit: symbolObj.digit,
    timecurrent: symbolObj.timecurrent,
    timedelay: symbolObj.timedelay,
  };

  await pub.publish(keyTick(broker_, sym), JSON.stringify(payload));
}

// ✅ KEY lưu ohlc cho chart (per broker/symbol/tf)
function keyChartSub(broker_, symbol, tf) {
  return `chart:sub:${safeKeyPart(broker_)}:${String(symbol || '').toUpperCase()}:${String(tf || 'M1').toUpperCase()}`;
}
function keyChartOHLC(broker_, symbol, tf) {
  return `chart:ohlc:${safeKeyPart(broker_)}:${String(symbol || '').toUpperCase()}:${String(tf || 'M1').toUpperCase()}`;
}
function keyOHLCChannel(broker_, symbol, tf) {
  return `ohlc:${safeKeyPart(broker_)}:${String(symbol || '').toUpperCase()}:${String(tf || 'M1').toUpperCase()}`;
}

// ON-DEMAND: watcher ref-count
function keyWatch(broker_, sym, tf) {
  return `watch:${safeKeyPart(broker_)}:${String(tf).toUpperCase()}:${String(sym).toUpperCase()}`;
}

// PubSub channels (for web ws to subscribe)
function chTick(broker_, symbol) {
  return `tick:${safeKeyPart(broker_)}:${safeKeyPart(symbol)}`;
}
function chOHLC(broker_, symbol, tf = 'M1') {
  return `ohlc:${safeKeyPart(broker_)}:${safeKeyPart(symbol)}:${safeKeyPart(tf)}`;
}

function gzipSync(str) { return zlib.gzipSync(Buffer.from(str, 'utf8')); }
function gunzipSync(buf) { return zlib.gunzipSync(buf).toString('utf8'); }

function packValue(obj, cfg) {
  const json = typeof obj === 'string' ? obj : JSON.stringify(obj);
  if (!cfg.compress) return json;
  const b64 = gzipSync(json).toString('base64');
  return `gz:${b64}`;
}

function unpackValue(val, cfg) {
  if (val == null) return null;
  const s = String(val);

  if (!s.startsWith('gz:')) {
    try { return JSON.parse(s); } catch { return s; }
  }

  const b64 = s.slice(3);
  try {
    const json = gunzipSync(Buffer.from(b64, 'base64'));
    try { return JSON.parse(json); } catch { return json; }
  } catch {
    return s;
  }
}

function toFlatArgs(obj) {
  const out = [];
  for (const [k, v] of Object.entries(obj || {})) out.push(String(k), String(v));
  return out;
}

function ensureEvenArgs(arr) {
  if (!Array.isArray(arr)) return [];
  if (arr.length % 2 === 1) arr.pop();
  return arr;
}

// Strip ohlc before storing into broker:*:symbols
function stripOHLC(symObj) {
  if (!symObj || typeof symObj !== 'object') return symObj;
  const copy = { ...symObj };
  delete copy.ohlc;
  return copy;
}

// =====================
// CORE
// =====================
function initRedis(options = {}) {
  _cfg = { ...DEFAULTS, ...options };
  if (_redis) return _redis;

  _redis = new Redis({
    host: _cfg.host,
    port: _cfg.port,
    db: _cfg.db,
    password: _cfg.password,
    lazyConnect: _cfg.lazyConnect,
    maxRetriesPerRequest: _cfg.maxRetriesPerRequest,
    retryStrategy: (times) => Math.min(times * 50, 2000),
    enableReadyCheck: true,
  });
    // ✅ separate connections for pub/sub
  _pub = new Redis({
    host: _cfg.host,
    port: _cfg.port,
    db: _cfg.db,
    password: _cfg.password,
    lazyConnect: _cfg.lazyConnect,
    maxRetriesPerRequest: _cfg.maxRetriesPerRequest,
    retryStrategy: (times) => Math.min(times * 50, 2000),
    enableReadyCheck: true,
  });

  _sub = new Redis({
    host: _cfg.host,
    port: _cfg.port,
    db: _cfg.db,
    password: _cfg.password,
    lazyConnect: _cfg.lazyConnect,
    maxRetriesPerRequest: _cfg.maxRetriesPerRequest,
    retryStrategy: (times) => Math.min(times * 50, 2000),
    enableReadyCheck: true,
  });

  _pub.on('error', (e) => console.error('[RedisH] pub error:', e?.message || e));
  _sub.on('error', (e) => console.error('[RedisH] sub error:', e?.message || e));


  _redis.__cfg = _cfg;

  _redis.on('connect', () => {
    console.log(`[RedisH] connect ${_cfg.host}:${_cfg.port} db=${_cfg.db} compress=${_cfg.compress} stream=${_cfg.streamEnabled}`);
  });

  _redis.on('error', (err) => {
    console.error('[RedisH] error:', err?.message || err);
  });

  return _redis;
}

function getRedis() {
  if (!_redis) initRedis();
  return _redis;
}

function getPublisher() {
  if (!_pub) initRedis();
  return _pub;
}

function getSubscriber() {
  if (_sub) return _sub;
  if (!_redis) initRedis();

  const cfg = _cfg;
  _sub = new Redis({
    host: cfg.host,
    port: cfg.port,
    db: cfg.db,
    password: cfg.password,
    lazyConnect: true,
    maxRetriesPerRequest: 2,
    retryStrategy: (times) => Math.min(times * 50, 2000),
    enableReadyCheck: true,
  });

  _sub.on('error', (err) => console.error('[RedisH SUB] error:', err?.message || err));
  return _sub;
}


const WATCH_TTL_SECONDS = Number(process.env.CHART_WATCH_TTL || 20); // FE giữ watch 20s, WS sẽ refresh

async function watchSymbol(broker_, symbol, tf = 'M1', ttl = WATCH_TTL_SECONDS) {
  await ensureConnected();
  const r = assertRedis();
  const k = keyWatch(broker_, symbol, tf);

  // dùng counter để nhiều client mở cùng lúc vẫn OK
  const v = await r.incr(k);
  await r.expire(k, ttl);
  return v;
}

async function refreshWatch(broker_, symbol, tf = 'M1', ttl = WATCH_TTL_SECONDS) {
  await ensureConnected();
  const r = assertRedis();
  const k = keyWatch(broker_, symbol, tf);
  const exists = await r.exists(k);
  if (exists) await r.expire(k, ttl);
  return !!exists;
}

async function unwatchSymbol(broker_, symbol, tf = 'M1') {
  await ensureConnected();
  const r = assertRedis();
  const k = keyWatch(broker_, symbol, tf);

  const v = await r.decr(k);
  if (v <= 0) await r.del(k);
  return v;
}


async function getChartOHLC(broker_, symbol, tf = 'M1') {
  await ensureConnected();
  const r = assertRedis();
  const raw = await r.get(keyChartOHLC(broker_, symbol, tf));
  if (!raw) return [];
  try { return JSON.parse(raw); } catch { return []; }
}


async function ensureConnectedPubSub() {
  const pub = getPublisher();
  const sub = getSubscriber();

  if (pub.status !== 'ready') await pub.connect().catch(() => {});
  if (sub.status !== 'ready') await sub.connect().catch(() => {});
  return { pub, sub };
}


function assertRedis() {
  const r = getRedis();
  if (!r) throw new Error('RedisH: Redis not initialized');
  return r;
}

async function ensureConnected() {
  const r = assertRedis();
  if (r.status !== 'ready') {
    await r.connect().catch(() => {});
  }
  return r;
}

async function closeRedis() {
  if (_redis) { try { await _redis.quit(); } catch {} _redis = null; }
  if (_pub)   { try { await _pub.quit(); } catch {} _pub = null; }
  if (_sub)   { try { await _sub.quit(); } catch {} _sub = null; }
}


// =====================
// ON-DEMAND CHART WATCH
// =====================
async function chartOpen(broker_, symbol, tf = 'M1') {
  await ensureConnected();
  const r = assertRedis();
  const wk = keyWatch(broker_, String(symbol || '').toUpperCase(), tf);

  const n = await r.incr(wk);
  await r.expire(wk, _cfg.watchTTL);

  return { watchKey: wk, watchers: Number(n) || 1 };
}
async function chartSubscribe(broker_, symbol, tf = 'M1', ttlSec = 30) {
  await ensureConnected();
  const r = assertRedis();
  const k = keyChartSub(broker_, symbol, tf);
  await r.set(k, '1', 'EX', Math.max(5, Number(ttlSec || 30))); // auto expire
  return true;
}

async function getChartOHLC(broker_, symbol, tf = 'M1') {
  await ensureConnected();
  const r = assertRedis();
  const cfg = r.__cfg || _cfg;
  const raw = await r.get(keyChartOHLC(broker_, symbol, tf));
  if (!raw) return [];
  const v = unpackValue(raw, cfg);
  return Array.isArray(v) ? v : [];
}

async function publishOHLC(broker_, symbol, tf, ohlcArr) {
  await ensureConnectedPubSub();async function chartSubscribe(broker_, symbol, tf = 'M1', ttlSec = 30) {
  await ensureConnected();
  const r = assertRedis();
  const k = keyChartSub(broker_, symbol, tf);
  await r.set(k, '1', 'EX', Math.max(5, Number(ttlSec || 30))); // auto expire
  return true;
}

async function getChartOHLC(broker_, symbol, tf = 'M1') {
  await ensureConnected();
  const r = assertRedis();
  const cfg = r.__cfg || _cfg;
  const raw = await r.get(keyChartOHLC(broker_, symbol, tf));
  if (!raw) return [];
  const v = unpackValue(raw, cfg);
  return Array.isArray(v) ? v : [];
}

async function publishOHLC(broker_, symbol, tf, ohlcArr) {
  await ensureConnectedPubSub();
  const pub = getPublisher();
  const payload = {
    broker_: safeKeyPart(broker_),
    symbol: String(symbol || '').toUpperCase(),
    tf: String(tf || 'M1').toUpperCase(),
    ohlc: Array.isArray(ohlcArr) ? ohlcArr : [],
    ts: Date.now()
  };
  await pub.publish(keyOHLCChannel(broker_, symbol, tf), JSON.stringify(payload));
}

  const pub = getPublisher();
  const payload = {
    broker_: safeKeyPart(broker_),
    symbol: String(symbol || '').toUpperCase(),
    tf: String(tf || 'M1').toUpperCase(),
    ohlc: Array.isArray(ohlcArr) ? ohlcArr : [],
    ts: Date.now()
  };
  await pub.publish(keyOHLCChannel(broker_, symbol, tf), JSON.stringify(payload));
}

async function chartClose(broker_, symbol, tf = 'M1') {
  await ensureConnected();
  const r = assertRedis();

  const sym = String(symbol || '').toUpperCase();
  const wk = keyWatch(broker_, sym, tf);
  const ck = keyChartOHLC(broker_, sym, tf);

  const cur = Number(await r.get(wk) || 0);

  if (cur <= 1) {
    const pipe = r.pipeline();
    pipe.del(wk);
    pipe.del(ck); // ✅ drop OHLC when last watcher
    await pipe.exec();
    return { watchKey: wk, watchers: 0, cleaned: true };
  }

  const n = await r.decr(wk);
  await r.expire(wk, _cfg.watchTTL);
  return { watchKey: wk, watchers: Number(n) || 0, cleaned: false };
}

async function isChartActive(broker_, symbol, tf = 'M1') {
  await ensureConnected();
  const r = assertRedis();
  const sym = String(symbol || '').toUpperCase();
  const n = Number(await r.get(keyWatch(broker_, sym, tf)) || 0);
  return n > 0;
}

async function getChartOHLC(broker_, symbol, tf = 'M1') {
  await ensureConnected();
  const r = assertRedis();
  const cfg = r.__cfg || _cfg;

  const sym = String(symbol || '').toUpperCase();
  const v = await r.get(keyChartOHLC(broker_, sym, tf));
  if (v == null) return null;

  const data = unpackValue(v, cfg);
  return data;
}

// =====================
// WRITE
// =====================
/**
 * SAVE 1 batch into Redis
 * - store symbols WITHOUT ohlc
 * - if watched -> publish tick + store/publish ohlc to chart key
 * Compatible for old redis by using HMSET
 */
async function saveBrokerBatch(payload) {
  await ensureConnected();
  const r = assertRedis();
  const cfg = r.__cfg || _cfg;

  const t0 = Date.now();

  // =====================
  // VALIDATE
  // =====================
  if (!payload) throw new Error('saveBrokerBatch: payload missing');

  const broker = String(payload.broker || '').trim();
  const broker_ = String(payload.broker_ || broker).trim();
  if (!broker_) throw new Error('saveBrokerBatch: broker_ is required');

  const symbolsArr = Array.isArray(payload.OHLC_Symbols) ? payload.OHLC_Symbols : [];
  if (symbolsArr.length === 0) {
    console.warn('[SAVE] EMPTY OHLC_Symbols:', broker_);
  }

  // =====================
  // KEYS
  // =====================
  const symbolsKey = keySymbols(broker_);
  const metaKey = keyMeta(broker_);

  // ✅ key OHLC riêng (không timeframe)
  const keyChartOHLC = (b, s) =>
    `chart:ohlc:${safeKeyPart(b)}:${String(s).toUpperCase().trim()}`;

  // =====================
  // BUILD SYMBOL HMSET (LITE: KHÔNG LƯU OHLC)
  // + BUILD OHLC SET (LƯU RIÊNG)
  // =====================
  const symbolArgs = [];
  let wrote = 0;

  // pipeline phần OHLC để set hàng loạt
  const pipe = r.pipeline();

  for (const s of symbolsArr) {
    if (!s) continue;

    const sym = String(s.symbol || s.symbol_raw || '').toUpperCase().trim();
    if (!sym) continue;

    // ---------- 1) LITE (bỏ ohlc) ----------
    const lite = { ...s };
    const ohlcArr = Array.isArray(lite.ohlc) ? lite.ohlc : null;
    if (lite.ohlc) delete lite.ohlc;

    symbolArgs.push(sym, packValue(lite, cfg));
    wrote++;

    // ---------- 2) OHLC riêng ----------
    if (ohlcArr && ohlcArr.length) {
      const ohlcKey = keyChartOHLC(broker_, sym);

      // Lưu đúng format array ohlc (nhẹ nhất)
      // Nếu bạn muốn kèm bid/ask trong ohlc thì MT4 đã gửi trong ohlc item rồi.
      pipe.set(ohlcKey, packValue(ohlcArr, cfg));

      // TTL cho ohlc (nếu bạn có ttlSeconds)
      if (cfg.ttlSeconds > 0) {
        pipe.expire(ohlcKey, cfg.ttlSeconds);
      }
    }
  }

  if (symbolArgs.length === 0) {
    console.warn('[SAVE] NO VALID SYMBOL AFTER FILTER:', broker_);
  }

  // đảm bảo HMSET không lỗi
  ensureEvenArgs(symbolArgs);

  // =====================
  // META
  // =====================
  const meta = {
    broker,
    broker_,
    index: String(payload.index ?? ''),
    port: String(payload.port ?? ''),
    version: String(payload.version ?? ''),
    typeaccount: String(payload.typeaccount ?? ''),
    totalsymbol: String(payload.totalsymbol ?? ''),
    batch: String(payload.batch ?? ''),
    totalBatches: String(payload.totalBatches ?? ''),
    timecurent: String(payload.timecurent ?? ''),
    timeUpdated: String(payload.timeUpdated ?? ''),
    auto_trade: String(payload.auto_trade ?? ''),
    status: String(payload.status ?? ''),
    lastWriteMs: String(Date.now()),
    lastBatchSymbols: String(wrote),
  };

  const metaArgs = ensureEvenArgs(toFlatArgs(meta));

  // =====================
  // PIPELINE WRITE (CORE)
  // =====================
  // ✅ HMSET symbols
  if (symbolArgs.length >= 2) {
    pipe.hmset(symbolsKey, ...symbolArgs);
  }

  // ✅ HMSET meta
  pipe.hmset(metaKey, ...metaArgs);

  // TTL (nếu có)
  if (cfg.ttlSeconds > 0) {
    pipe.expire(metaKey, cfg.ttlSeconds);
    if (symbolArgs.length >= 2) pipe.expire(symbolsKey, cfg.ttlSeconds);
  }

  // EXEC
  const res = await pipe.exec();
  for (const [err] of res) {
    if (err) {
      console.error('[SAVE] REDIS ERROR:', err);
      throw err;
    }
  }

  const ms = Date.now() - t0;

  // =====================
  // DEBUG CONFIRM (optional)
  // =====================
  if (ms > 60) {
    console.log(`[SAVE SLOW] broker=${broker_} symbols=${wrote} time=${ms}ms`);
  }

  return {
    ok: true,
    broker_,
    wrote,
    ms,
    keys: { symbolsKey, metaKey },
  };
}

/**
 * Thủ công lưu toàn bộ priceBuffer hiện tại vào Redis
 * Dùng để test khi flush định kỳ chưa chạy hoặc muốn force lưu ngay
 * @returns {Object} kết quả lưu
 */
async function saveAllBroker() {if (priceBuffer.size === 0) {
    console.log('[saveAllBrokerToRedis] Buffer empty, nothing to save');
    return { ok: false, wrote: 0, reason: 'empty buffer' };
  }

  await RedisH.ensureConnected();
  const client = RedisH.getRedis();

  const pipeline = client.pipeline();
  const now = Date.now();

  let count = 0;
  for (const [key, data] of priceBuffer) {
    const redisKey = `prices:${key}`;

    pipeline.hset(redisKey,
      'bid', String(data.bid ?? '0'),
      'ask', String(data.ask ?? '0'),
      'spread', String(data.spread ?? '0'),
      'digit', String(data.digit ?? '5'),
      'trade', String(data.trade ?? 'false').toUpperCase(),
      'timedelay', String(data.timedelay ?? '0'),
      'broker_sync', String(data.broker_sync ?? ''),
      'last_reset', String(data.last_reset ?? ''),
      'timecurrent', String(data.timecurrent ?? ''),
      'broker', String(data.broker ?? ''),
      'symbol', String(data.symbol ?? ''),
      'updatedAt', String(now)
    );

    pipeline.expire(redisKey, 3600);
    count++;
  }

  try {
    const results = await pipeline.exec();
    const errors = results.filter(([err]) => err !== null);

    if (errors.length > 0) {
      console.error(`[saveAllBrokerToRedis] Failed on ${errors.length}/${count} keys`);
      return { ok: false, wrote: count, errors: errors.length };
    }

    console.log(`[saveAllBrokerToRedis] SUCCESS: Saved ${count} prices to Redis`);
    return { ok: true, wrote: count };
  } catch (err) {
    console.error('[saveAllBrokerToRedis] Error:', err.message);
    return { ok: false, error: err.message };
  }}

// ✅ ADD THESE 2 FUNCTIONS INTO redis.helper.js (RedisH)
// - saveBrokerPrice(payload): handle PRICE_SNAP (Symbols[] no ohlc)
// - saveBrokerOHLC(payload): handle OHLC_SNAP (Symbols[] with ohlc)
// Copy-paste đúng vào file redis.helper.js của bạn (cùng scope với saveBrokerBatch)

async function saveBrokerPrice(payload, options = {}) {
  await ensureConnected();
  const r = assertRedis();
  const cfg = r.__cfg || _cfg;

  const {
    publishTicks = false, // bật nếu bạn muốn PubSub tick (mặc định OFF cho nhẹ)
  } = options;

  if (!payload) throw new Error('saveBrokerPrice: payload missing');

  const broker = String(payload.broker || '').trim();
  const broker_ = String(payload.broker_ || broker).trim();
  if (!broker_) throw new Error('saveBrokerPrice: broker_ is required');

  const symbolsArr = Array.isArray(payload.Symbols) ? payload.Symbols : [];
  if (!symbolsArr.length) return { ok: true, broker_, wrote: 0, ms: 0 };

  const t0 = Date.now();

  const symbolsKey = keySymbols(broker_);
  const metaKey = keyMeta(broker_);

  const symbolArgs = [];
  let wrote = 0;

  // pipeline write (1 round)
  const pipe = r.pipeline();

  for (const s of symbolsArr) {
    if (!s) continue;

    const sym = String(s.symbol || s.symbol_raw || '').toUpperCase().trim();
    if (!sym) continue;

    // ✅ đảm bảo PRICE_SNAP không lưu ohlc
    const lite = { ...s };
    if (lite.ohlc) delete lite.ohlc;

    // ✅ HMSET field=SYM value=packed lite
    symbolArgs.push(sym, packValue(lite, cfg));
    wrote++;

    // ✅ optional PubSub tick
    if (publishTicks) {
      // publish realtime nhẹ, không block write redis
      // (không await từng cái để tránh nghẽn)
      publishTickFromSymbol(broker_, lite).catch(() => {});
    }
  }

  ensureEvenArgs(symbolArgs);

  // ✅ update meta nhẹ (không đụng totalsymbol/batch nếu bạn không gửi)
  const meta = {
    broker,
    broker_,
    index: String(payload.index ?? ''),
    port: String(payload.port ?? ''),
    version: String(payload.version ?? ''),
    typeaccount: String(payload.typeaccount ?? ''),
    totalsymbol: String(payload.totalsymbol ?? ''),
    timecurent: String(payload.timecurent ?? ''),
    timeUpdated: String(payload.timeUpdated ?? ''),
    auto_trade: String(payload.auto_trade ?? ''),
    status: String(payload.status ?? ''),
    lastWriteMs: String(Date.now()),
    lastPriceWriteMs: String(Date.now()),
    lastBatchSymbols: String(wrote),
    // bạn có thể thêm field khác nếu cần
  };

  const metaArgs = ensureEvenArgs(toFlatArgs(meta));

  if (symbolArgs.length >= 2) pipe.hmset(symbolsKey, ...symbolArgs);
  pipe.hmset(metaKey, ...metaArgs);

  // TTL nếu bạn có set
  if (cfg.ttlSeconds > 0) {
    pipe.expire(metaKey, cfg.ttlSeconds);
    if (symbolArgs.length >= 2) pipe.expire(symbolsKey, cfg.ttlSeconds);
  }

  const res = await pipe.exec();
  for (const [err] of res) {
    if (err) throw err;
  }

  const ms = Date.now() - t0;
  return { ok: true, broker_, wrote, ms, keys: { symbolsKey, metaKey } };
}

async function saveBrokerOHLC(payload, options = {}) {
  await ensureConnected();
  const r = assertRedis();
  const cfg = r.__cfg || _cfg;

  const {
    publish = false, // bật nếu bạn muốn PubSub OHLC theo tf/symbol (mặc định OFF)
  } = options;

  if (!payload) throw new Error('saveBrokerOHLC: payload missing');

  const broker = String(payload.broker || '').trim();
  const broker_ = String(payload.broker_ || broker).trim();
  if (!broker_) throw new Error('saveBrokerOHLC: broker_ is required');

  const tf = String(payload.TF || 'M1').toUpperCase().trim();

  const symbolsArr = Array.isArray(payload.Symbols) ? payload.Symbols : [];
  if (!symbolsArr.length) return { ok: true, broker_, wrote: 0, ms: 0 };

  const t0 = Date.now();

  // ✅ pipeline: set ohlc keys
  const pipe = r.pipeline();
  let wrote = 0;

  for (const s of symbolsArr) {
    if (!s) continue;

    const sym = String(s.symbol || s.symbol_raw || '').toUpperCase().trim();
    if (!sym) continue;

    const ohlcArr = Array.isArray(s.ohlc) ? s.ohlc : null;
    if (!ohlcArr || !ohlcArr.length) continue;

    // ✅ key theo helper bạn đang dùng (không tf) -> chart:ohlc:<broker_>:<SYMBOL>
    // Nếu bạn muốn phân TF: đổi sang keyChartOHLC(broker_, sym, tf)
    const k = keyChartOHLC(broker_, sym);

    pipe.set(k, packValue(ohlcArr, cfg));
    if (cfg.ttlSeconds > 0) pipe.expire(k, cfg.ttlSeconds);

    wrote++;

    if (publish) {
      publishOHLC(broker_, sym, tf, ohlcArr).catch(() => {});
    }
  }

  // ✅ update meta để bạn debug lag
  const metaKey = keyMeta(broker_);
  const meta = {
    broker,
    broker_,
    lastOhlcWriteMs: String(Date.now()),
    lastOhlcTf: tf,
    lastOhlcSymbols: String(wrote),
    timeUpdated: String(payload.timeUpdated ?? ''),
  };
  pipe.hmset(metaKey, ...ensureEvenArgs(toFlatArgs(meta)));

  const res = await pipe.exec();
  for (const [err] of res) {
    if (err) throw err;
  }

  const ms = Date.now() - t0;
  return { ok: true, broker_, tf, wrote, ms };
}


async function savePriceData(payload) {
  await ensureConnected();
  const r = assertRedis();
  const cfg = r.__cfg || _cfg;

  const t0 = Date.now();

  // Validate
  if (!payload) throw new Error('savePriceData: payload missing');

  const broker = String(payload.broker || '').trim();
  const broker_ = String(payload.broker_ || broker).trim();
  if (!broker_) throw new Error('savePriceData: broker_ is required');

  const symbolsArr = Array.isArray(payload.symbols) ? payload.symbols : [];
  if (symbolsArr.length === 0) {
    console.warn('[PRICE] EMPTY symbols:', broker_);
    return { ok: true, broker_, wrote: 0, ms: 0 };
  }

  // Keys
  const symbolsKey = keySymbols(broker_);
  const metaKey = keyMeta(broker_);

  // Pipeline
  const pipe = r.pipeline();
  let wrote = 0;
  const tickPromises = [];

  for (const s of symbolsArr) {
    if (!s) continue;

    const sym = String(s.symbol || '').toUpperCase().trim();
    if (!sym) continue;

    // Get existing data to merge (avoid losing OHLC ref if set separately)
    const existing = await r.hget(symbolsKey, sym);
    let symbolData = existing ? unpackValue(existing, cfg) : {};

    // Update ONLY price fields
    symbolData = {
      ...symbolData,
      symbol: sym,
      bid: s.bid,
      ask: s.ask,
      bid_mdf: s.bid_mdf,
      ask_mdf: s.ask_mdf,
      spread: s.spread,
      spread_mdf: s.spread_mdf,
      digit: s.digit,
      timecurrent: s.timecurrent,
      timedelay: s.timedelay,
      trade: s.trade,
      broker_sync: s.broker_sync,
      longcandle: s.longcandle,
      longcandle_mdf: s.longcandle_mdf,
      symbol_raw: s.symbol_raw,
    };

    // NO OHLC
    delete symbolData.ohlc;

    pipe.hset(symbolsKey, sym, packValue(symbolData, cfg));
    wrote++;

    // Publish tick realtime (if trade = TRUE)
    if (String(s.trade || '').toUpperCase() === 'TRUE') {
      tickPromises.push(
        publishTickFromSymbol(broker_, symbolData).catch(e => 
          console.error('[PRICE] Publish tick error:', sym, e.message)
        )
      );
    }
  }

  // Update meta with timestamp
  pipe.hmset(
    metaKey,
    'broker', broker,
    'broker_', broker_,
    'timecurent', String(payload.timecurent || Date.now()),
    'lastWriteMs', String(Date.now()),
    'lastPriceUpdate', String(Date.now())
  );

  if (cfg.ttlSeconds > 0) {
    pipe.expire(symbolsKey, cfg.ttlSeconds);
    pipe.expire(metaKey, cfg.ttlSeconds);
  }

  // Execute
  const res = await pipe.exec();
  for (const [err] of res) {
    if (err) {
      console.error('[PRICE] REDIS ERROR:', err);
      throw err;
    }
  }

  // Wait for tick publishes (non-blocking)
  Promise.all(tickPromises).catch(() => {});

  const ms = Date.now() - t0;

  if (ms > 50) {
    console.log(`[PRICE] broker=${broker_} symbols=${wrote} time=${ms}ms`);
  }

  return {
    ok: true,
    broker_,
    wrote,
    ms,
    type: 'PRICE_DATA',
  };
}

/**
 * Handle OHLC_DATA message from MT4 (NEW - Heavy, Infrequent)
 * - Only updates: OHLC array
 * - Stores in chart:ohlc:* keys
 * - Publishes to OHLC channel if watched
 * 
 * Usage:
 *   await RedisH.saveOHLCData({
 *     broker: 'ABC',
 *     broker_: 'abc-broker',
 *     symbols: [
 *       {
 *         symbol: 'EURUSD',
 *         ohlc: [
 *           {time, open, high, low, close},
 *           ...
 *         ]
 *       }
 *     ]
 *   });
 */

async function saveBrokerMeta(payload) {
  await ensureConnected();
  const r = assertRedis();
  const cfg = r.__cfg || _cfg;

  const broker = String(payload.broker || '').trim();
  const broker_ = String(payload.broker_ || broker).trim();
  if (!broker_) throw new Error('saveBrokerMeta: broker_ is required');

  const arr = Array.isArray(payload.META_Symbols) ? payload.META_Symbols : [];
  if (!arr.length) return { ok: true, broker_, wrote: 0, ms: 0 };

  const t0 = Date.now();
  const pipe = r.pipeline();

  const metaArgs = [];
  let wrote = 0;

  for (const s of arr) {
    if (!s) continue;
    const sym = String(s.symbol || s.symbol_raw || '').toUpperCase().trim();
    if (!sym) continue;

    // chỉ lưu 3 thứ bạn muốn
    const metaLite = {
      symbol: sym,
      digit: s.digit,
      trade: s.trade,
      timetrade: s.timetrade || [],
      ts: Date.now(),
    };

    metaArgs.push(sym, packValue(metaLite, cfg));
    wrote++;
  }

  ensureEvenArgs(metaArgs);

  const symMetaKey = keySymbolMeta(broker_);
  if (metaArgs.length >= 2) pipe.hmset(symMetaKey, ...metaArgs);

  // TTL nếu bạn dùng
  if (cfg.ttlSeconds > 0) pipe.expire(symMetaKey, cfg.ttlSeconds);

  // cập nhật broker meta chung (optional, để debug)
  pipe.hmset(keyMeta(broker_), 'lastMetaMs', String(Date.now()), 'lastMetaCount', String(wrote));

  const res = await pipe.exec();
  for (const [err] of res) if (err) throw err;

  return { ok: true, broker_, wrote, ms: Date.now() - t0, key: symMetaKey };
}

async function saveOHLCData(payload) {
  await ensureConnected();
  const r = assertRedis();
  const cfg = r.__cfg || _cfg;

  const t0 = Date.now();

  // Validate
  if (!payload) throw new Error('saveOHLCData: payload missing');

  const broker = String(payload.broker || '').trim();
  const broker_ = String(payload.broker_ || broker).trim();
  if (!broker_) throw new Error('saveOHLCData: broker_ is required');

  const symbolsArr = Array.isArray(payload.symbols) ? payload.symbols : [];
  if (symbolsArr.length === 0) {
    console.warn('[OHLC] EMPTY symbols:', broker_);
    return { ok: true, broker_, wrote: 0, ms: 0 };
  }

  // Pipeline
  const pipe = r.pipeline();
  let wrote = 0;
  const publishPromises = [];

  for (const s of symbolsArr) {
    if (!s) continue;

    const sym = String(s.symbol || '').toUpperCase().trim();
    if (!sym) continue;

    const ohlcArr = Array.isArray(s.ohlc) ? s.ohlc : [];
    if (ohlcArr.length === 0) continue;

    // Store OHLC in chart key (M1 timeframe)
    const tf = 'M1'; // MT4 sends M1, can extend for other TFs
    const ohlcKey = keyChartOHLC(broker_, sym, tf);

    pipe.set(ohlcKey, packValue(ohlcArr, cfg));

    if (cfg.ttlSeconds > 0) {
      pipe.expire(ohlcKey, cfg.ttlSeconds);
    }

    wrote++;

    // Publish OHLC if symbol is watched (non-blocking)
    publishPromises.push(
      isChartActive(broker_, sym, tf).then(active => {
        if (active) {
          return publishOHLC(broker_, sym, tf, ohlcArr);
        }
      }).catch(e => 
        console.error('[OHLC] Publish error:', sym, e.message)
      )
    );
  }

  // Update meta timestamp
  const metaKey = keyMeta(broker_);
  pipe.hmset(
    metaKey,
    'lastOHLCUpdate', String(Date.now())
  );

  // Execute
  const res = await pipe.exec();
  for (const [err] of res) {
    if (err) {
      console.error('[OHLC] REDIS ERROR:', err);
      throw err;
    }
  }

  // Wait for publishes (non-blocking)
  Promise.all(publishPromises).catch(() => {});

  const ms = Date.now() - t0;

  if (ms > 100) {
    console.log(`[OHLC] broker=${broker_} symbols=${wrote} time=${ms}ms`);
  }

  return {
    ok: true,
    broker_,
    wrote,
    ms,
    type: 'OHLC_DATA',
  };
}



/**
 * RedisH-first: Lấy details cho nhiều symbols từ Redis theo format saveBrokerBatch()
 * - metaKey = keyMeta(broker_)
 * - symbolsKey = keySymbols(broker_)
 * - symbolsKey hash: field = SYMBOL, value = packValue(lite)
 *
 * Return: Map<symbol, details[]>
 */
async function getMultipleSymbolDetails_RedisH(symbols) {
  if (!symbols || symbols.length === 0) return new Map();

  await ensureConnected();
  const r = assertRedis();
  const cfg = r.__cfg || _cfg;

  // ✅ normalize symbol giống lúc lưu (saveBrokerBatch dùng toUpperCase)
  const normSymbols = symbols
    .map((s) => String(s || "").toUpperCase().trim())
    .filter(Boolean);

  // ✅ luôn tạo map theo input => không bao giờ Map(0) khi input hợp lệ
  const resultMap = new Map();
  for (const sym of normSymbols) resultMap.set(sym, []);

  if (normSymbols.length === 0) return resultMap;

  const unpack = (v) => {
    if (v == null) return null;
    try {
      if (typeof unpackValue === "function") return unpackValue(v, cfg);
      return JSON.parse(v);
    } catch {
      return null;
    }
  };

  const isTrue = (v) => {
    const s = String(v ?? "").toLowerCase();
    return s === "true" || s === "1" || s === "yes";
  };

  // =========================
  // 1) SCAN meta keys đúng pattern bạn đang có
  // =========================
  const META_PATTERN = "broker:*:meta";
  const metaKeys = [];
  let cursor = "0";

  do {
    const [next, keys] = await r.scan(cursor, "MATCH", META_PATTERN, "COUNT", 500);
    cursor = next;
    if (keys && keys.length) metaKeys.push(...keys);
  } while (cursor !== "0");

  if (metaKeys.length === 0) return resultMap;

  // =========================
  // 2) Pipeline: HGETALL meta
  // =========================
  const pipeMeta = r.pipeline();
  metaKeys.forEach((k) => pipeMeta.hgetall(k));
  const metaExec = await pipeMeta.exec();

  // =========================
  // 3) Build list broker active + derive broker_
  // metaKey format: broker:{broker_}:meta
  // =========================
  const activeBrokers = [];
  for (let i = 0; i < metaExec.length; i++) {
    const [err, meta] = metaExec[i] || [];
    if (err) continue;
    if (!meta || Object.keys(meta).length === 0) continue;

    const metaKey = metaKeys[i]; // broker:abc:meta
    const parts = String(metaKey).split(":");
    const broker_ = parts.length >= 3 ? parts[1] : String(meta.broker_ || "").trim();
    if (!broker_) continue;

    if (!isTrue(meta.status)) continue;

    activeBrokers.push({
      broker_: broker_,
      broker: String(meta.broker || broker_),
      status: String(meta.status || ""),
      index: String(meta.index || ""),
      auto_trade: String(meta.auto_trade || ""),
      typeaccount: String(meta.typeaccount || ""),
    });
  }

  if (activeBrokers.length === 0) return resultMap;

  // =========================
  // 4) Pipeline: HMGET symbols hash theo từng broker
  // symbolsKey format: broker:{broker_}:symbols
  // =========================
  const pipeSym = r.pipeline();
  activeBrokers.forEach((b) => {
    const symbolsKey = `broker:${b.broker_}:symbols`;
    pipeSym.hmget(symbolsKey, ...normSymbols);
  });

  const symExec = await pipeSym.exec();

  // =========================
  // 5) Fill Map<symbol, details[]>
  // =========================
  for (let i = 0; i < symExec.length; i++) {
    const [err, values] = symExec[i] || [];
    if (err) continue;

    const b = activeBrokers[i];
    if (!Array.isArray(values)) continue;

    for (let j = 0; j < values.length; j++) {
      const packed = values[j];
      if (!packed) continue; // field không tồn tại

      const sym = normSymbols[j];
      const symbolInfo = unpack(packed);
      if (!symbolInfo) continue;

      // ✅ filter trade như code bạn
      if (String(symbolInfo.trade ?? "").toUpperCase() !== "TRUE") continue;

      const details = {
        Broker: b.broker,
        Broker_: b.broker_,
        Status: b.status,
        Index: b.index,
        Auto_Trade: b.auto_trade,
        Typeaccount: b.typeaccount,
        ...symbolInfo,
      };

      resultMap.get(sym).push(details);
    }
  }

  // ✅ sort theo Index như bạn
  for (const [sym, arr] of resultMap) {
    arr.sort((a, b) => parseFloat(a.Index || 0) - parseFloat(b.Index || 0));
  }

  return resultMap;
}



function keyChartOHLC(broker_, symbol) {
  return `chart:ohlc:${safeKeyPart(broker_)}:${String(symbol || '').toUpperCase().trim()}`;
}

// helper đọc OHLC (tự giải nén)
async function getChartOHLC(broker_, symbol) {
  await ensureConnected();
  const r = assertRedis();
  const cfg = r.__cfg || _cfg;

  const raw = await r.get(keyChartOHLC(broker_, symbol));
  const data = unpackValue(raw, cfg);
  return Array.isArray(data) ? data : [];
}

// =====================
// READ
// =====================

// ✅ Đếm tổng broker (dựa vào key meta: broker:*:meta)
// options.onlyActive=true -> chỉ đếm broker status === "True"
async function getTotalBrokers(options = {}) {
  const { onlyActive = false } = options;

  await ensureConnected();
  const r = assertRedis();

  let total = 0;

  // Duyệt theo SCAN để không chết Redis khi nhiều key
  let cursor = '0';
  do {
    const [nextCursor, keys] = await r.scan(cursor, 'MATCH', 'broker:*:meta', 'COUNT', 1000);
    cursor = nextCursor;

    if (!keys || keys.length === 0) continue;

    if (!onlyActive) {
      total += keys.length;
      continue;
    }

    // onlyActive: pipeline HMGET status
    const pipe = r.pipeline();
    for (const k of keys) pipe.hmget(k, 'status');
    const res = await pipe.exec();

    for (const item of res) {
      const err = item?.[0];
      const data = item?.[1]; // hmget -> [status]
      if (err) continue;

      const status = Array.isArray(data) ? data[0] : null;
      if (String(status) === 'True') total += 1;
    }
  } while (cursor !== '0');

  return total;
}

async function getAllBrokers(options = {}) {
  const {
    onlyActive = false,
    withSymbols = true,
    withOhlc = false, // ⚠️ nặng, off mặc định
  } = options;

  await ensureConnected();
  const r = assertRedis();
  const cfg = r.__cfg || _cfg;

  const brokers = [];
  let cursor = '0';

  // helper: từ metaKey -> broker_
  const parseBrokerFromMetaKey = (metaKey) => {
    // broker:<broker_>:meta
    const parts = String(metaKey).split(':');
    // ["broker", "<broker_>", "meta"]
    return parts.length >= 3 ? parts[1] : '';
  };

  // key OHLC riêng (như bạn saveBrokerBatch)
  const keyChartOHLC = (b, s) => `chart:ohlc:${safeKeyPart(b)}:${String(s).toUpperCase().trim()}`;

  do {
    const [nextCursor, metaKeys] = await r.scan(cursor, 'MATCH', 'broker:*:meta', 'COUNT', 300);
    cursor = nextCursor;

    if (!metaKeys || metaKeys.length === 0) continue;

    // 1) Lấy meta hàng loạt
    const pipeMeta = r.pipeline();
    for (const mk of metaKeys) pipeMeta.hgetall(mk);
    const metaRes = await pipeMeta.exec();

    for (let i = 0; i < metaKeys.length; i++) {
      const metaKey = metaKeys[i];
      const [err, meta] = metaRes[i] || [];
      if (err || !meta || Object.keys(meta).length === 0) continue;

      const broker_ = meta.broker_ || parseBrokerFromMetaKey(metaKey);
      if (!broker_) continue;

      if (onlyActive && String(meta.status) !== 'True') continue;

      const item = {
        // meta chuẩn
        broker: meta.broker || '',
        broker_: broker_,
        index: meta.index ?? '',
        port: meta.port ?? '',
        version: meta.version ?? '',
        typeaccount: meta.typeaccount ?? '',
        totalsymbol: meta.totalsymbol ?? '',
        batch: meta.batch ?? '',
        totalBatches: meta.totalBatches ?? '',
        timecurent: meta.timecurent ?? '',
        timeUpdated: meta.timeUpdated ?? '',
        auto_trade: meta.auto_trade ?? '',
        status: meta.status ?? '',
        lastWriteMs: meta.lastWriteMs ?? '',
        lastBatchSymbols: meta.lastBatchSymbols ?? '',

        // data symbols
        OHLC_Symbols: [],
      };

      // 2) Lấy symbols (hash broker:<broker_>:symbols)
      if (withSymbols) {
        const symbolsKey = `broker:${broker_}:symbols`;
        let hmap = null;

        try {
          hmap = await r.hgetall(symbolsKey);
        } catch (e) {
          hmap = null;
        }

        if (hmap && Object.keys(hmap).length) {
          const symArr = [];
          for (const [sym, packed] of Object.entries(hmap)) {
            if (!packed) continue;

            let obj = null;
            try {
              // bạn đang dùng packValue/unpackValue -> dùng unpackValue nếu có
              obj = typeof unpackValue === 'function' ? unpackValue(packed, cfg) : JSON.parse(packed);
            } catch (_) {
              obj = null;
            }

            if (!obj) continue;

            // gắn symbol name (đảm bảo có)
            if (!obj.symbol) obj.symbol = sym;

            symArr.push(obj);
          }

          // sort theo Index nếu muốn (nhưng symbol hash của 1 broker không cần)
          item.OHLC_Symbols = symArr;
        }
      }

      // 3) (Optional) load ohlc riêng theo từng symbol -> rất nặng
      if (withOhlc && item.OHLC_Symbols.length) {
        const pipeOhlc = r.pipeline();
        for (const s of item.OHLC_Symbols) {
          const sym = String(s.symbol || s.symbol_raw || '').toUpperCase().trim();
          if (!sym) continue;
          pipeOhlc.get(keyChartOHLC(broker_, sym));
        }

        const ohlcRes = await pipeOhlc.exec();

        let idx = 0;
        for (const s of item.OHLC_Symbols) {
          const sym = String(s.symbol || s.symbol_raw || '').toUpperCase().trim();
          if (!sym) continue;

          const [e, packedOhlc] = ohlcRes[idx] || [];
          idx++;

          if (e || !packedOhlc) continue;

          try {
            const ohlc = typeof unpackValue === 'function' ? unpackValue(packedOhlc, cfg) : JSON.parse(packedOhlc);
            s.ohlc = Array.isArray(ohlc) ? ohlc : ohlc;
          } catch (_) {}
        }
      }

      brokers.push(item);
    }
  } while (cursor !== '0');

  // sort theo index (giống bạn hay dùng)
  brokers.sort((a, b) => parseFloat(a.index || 0) - parseFloat(b.index || 0));

  return brokers;
}

async function getBrokerMeta(broker_) {
  const r = assertRedis();
  const meta = await r.hgetall(keyMeta(broker_));
  return meta && Object.keys(meta).length ? meta : null;
}

async function getBroker(broker_) {
  await ensureConnected();
  const r = assertRedis();
  const cfg = r.__cfg || _cfg;

  const symbolsKey = keySymbols(broker_);
  let cursor = '0';
  const out = [];

  do {
    const [next, arr] = await r.hscan(symbolsKey, cursor, 'COUNT', 200);
    cursor = next;

    for (let i = 0; i < arr.length; i += 2) {
      const fieldSym = arr[i];
      const rawVal = arr[i + 1];

      const data = unpackValue(rawVal, cfg);
      if (data && typeof data === 'object') {
        if (!data.symbol) data.symbol = fieldSym;
        out.push(data);
      } else {
        out.push({ symbol: fieldSym, raw: data });
      }
    }
  } while (cursor !== '0');

  return out;
}

async function getSymbol(broker_, symbol) {
  await ensureConnected();
  const r = assertRedis();
  const cfg = r.__cfg || _cfg;

  const sym = String(symbol || '').trim().toUpperCase();
  const val = await r.hget(keySymbols(broker_), sym);
  if (val == null) return null;
  return unpackValue(val, cfg);
}

async function getSymbols(broker_, symbols = []) {
  await ensureConnected();
  const r = assertRedis();
  const cfg = r.__cfg || _cfg;

  const fields = (symbols || []).map(s => String(s).toUpperCase());
  if (!fields.length) return {};

  const vals = await r.hmget(keySymbols(broker_), ...fields);

  const out = {};
  for (let i = 0; i < fields.length; i++) {
    out[fields[i]] = vals[i] == null ? null : unpackValue(vals[i], cfg);
  }
  return out;
}

// =====================
// DELETE BROKER (FULL CLEAN)
// =====================
/**
 * deleteBroker('abc')
 * - Xoá toàn bộ dữ liệu của broker trong Redis
 * - Bao gồm:
 *   broker:<broker_>:symbols
 *   broker:<broker_>:meta
 *   broker:<broker_>:stream (nếu có)
 *   chart:ohlc:<broker_>:*
 */
async function deleteBroker(broker_) {
  await ensureConnected();
  const r = assertRedis();

  if (!broker_) throw new Error('deleteBroker: broker_ is required');

  const b = safeKeyPart(broker_);

  const keysToDelete = [];

  // Core keys
  keysToDelete.push(`broker:${b}:symbols`);
  keysToDelete.push(`broker:${b}:meta`);
  keysToDelete.push(`broker:${b}:stream`);

  // Chart OHLC keys: chart:ohlc:<broker_>:*
  let cursor = '0';
  do {
    const [next, keys] = await r.scan(
      cursor,
      'MATCH',
      `chart:ohlc:${b}:*`,
      'COUNT',
      200
    );
    cursor = next;
    if (keys && keys.length) keysToDelete.push(...keys);
  } while (cursor !== '0');

  if (keysToDelete.length === 0) {
    return { ok: true, broker_: b, deleted: 0 };
  }

  // DEL theo batch để an toàn
  const pipe = r.pipeline();
  for (const k of keysToDelete) {
    pipe.del(k);
  }

  const res = await pipe.exec();

  let deleted = 0;
  for (const [, val] of res) {
    deleted += Number(val || 0);
  }

  return {
    ok: true,
    broker_: b,
    deletedKeys: keysToDelete.length,
    deleted,
  };
}


// =====================
// LIST
// =====================
async function Broker_names() {
  await ensureConnected();
  const r = assertRedis();

  let cursor = '0';
  const out = [];

  do {
    const [next, keys] = await r.scan(cursor, 'MATCH', 'broker:*:meta', 'COUNT', 200);
    cursor = next;

    if (keys.length) {
      const pipe = r.pipeline();
      keys.forEach(k => pipe.hgetall(k));
      const rows = await pipe.exec();

      for (const [, meta] of rows) {
        if (!meta || (!meta.broker && !meta.broker_)) continue;
        out.push({
          broker: meta.broker || '',
          broker_: meta.broker_ || '',
          index: meta.index || '',
          version: meta.version || '',
          typeaccount: meta.typeaccount || '',
          timecurent: meta.timecurent || '',
          status: meta.status || '',
          port: meta.port || '',
          totalsymbol: meta.totalsymbol || '',
          auto_trade: meta.auto_trade || '',
          timeUpdated: meta.timeUpdated || '',
        });
      }
    }
  } while (cursor !== '0');

  out.sort((a, b) => Number(a.index ?? 0) - Number(b.index ?? 0));
  return out;
}

// =====================
// UNION SYMBOLS (NO DUPLICATE)
// =====================
async function getUnionSymbols(brokers = []) {
  await ensureConnected();
  const r = assertRedis();
  const symbolSet = new Set();

  for (const broker_ of brokers) {
    const symbolsKey = keySymbols(broker_);
    let cursor = '0';

    do {
      const [next, arr] = await r.hscan(symbolsKey, cursor, 'COUNT', 200);
      cursor = next;

      for (let i = 0; i < arr.length; i += 2) {
        symbolSet.add(arr[i]);
      }
    } while (cursor !== '0');
  }

  return Array.from(symbolSet);
}

async function getUnionSymbolsAllBrokers() {
  await ensureConnected();
  const r = assertRedis();
  const symbolSet = new Set();

  let cursorKeys = '0';
  const symbolsKeys = [];

  do {
    const [next, keys] = await r.scan(cursorKeys, 'MATCH', 'broker:*:symbols', 'COUNT', 200);
    cursorKeys = next;
    if (keys && keys.length) symbolsKeys.push(...keys);
  } while (cursorKeys !== '0');

  for (const skey of symbolsKeys) {
    let cursor = '0';
    do {
      const [next, arr] = await r.hscan(skey, cursor, 'COUNT', 200);
      cursor = next;
      for (let i = 0; i < arr.length; i += 2) symbolSet.add(arr[i]);
    } while (cursor !== '0');
  }

  return Array.from(symbolSet);
}

// =====================
// SYMBOL ACROSS ALL BROKERS (trade TRUE) sorted by Index
// =====================
async function getSymbolAllBroker(symbol) {
  await ensureConnected();
  const r = assertRedis();
  const cfg = r.__cfg || _cfg;

  const sym = String(symbol || '').trim().toUpperCase();
  if (!sym) return [];

  let cursorKeys = '0';
  const symbolsKeys = [];

  do {
    const [next, keys] = await r.scan(cursorKeys, 'MATCH', 'broker:*:symbols', 'COUNT', 5000);
    cursorKeys = next;
    if (keys && keys.length) symbolsKeys.push(...keys);
  } while (cursorKeys !== '0');

  if (symbolsKeys.length === 0) return [];

  const pipe = r.pipeline();
  const brokerIds = [];

  for (const skey of symbolsKeys) {
    const parts = String(skey).split(':');
    const broker_ = parts.length >= 3 ? parts[1] : '';
    if (!broker_) continue;

    brokerIds.push(broker_);
    pipe.hget(skey, sym);
    pipe.hgetall(keyMeta(broker_));
  }

  const rows = await pipe.exec();
  const out = [];

  for (let i = 0, bi = 0; i < rows.length; i += 2, bi++) {
    const [errVal, val] = rows[i];
    const [errMeta, meta] = rows[i + 1];

    if (errVal) throw new Error(`getSymbolAllBroker HGET error: ${errVal.message || errVal}`);
    if (errMeta) throw new Error(`getSymbolAllBroker HGETALL meta error: ${errMeta.message || errMeta}`);

    if (val == null) continue;

    const data = unpackValue(val, cfg);
    if (!data || typeof data !== 'object') continue;

    // ✅ trade TRUE filter
    if (String(data.trade || '').toUpperCase() !== 'TRUE') continue;

    const m = meta || {};
    const item = Object.assign(
      {
        Broker: m.broker || (m.broker_ ? String(m.broker_).toUpperCase() : ''),
        Broker_: m.broker_ || brokerIds[bi],
        Status: m.status || '',
        Index: m.index || '',
        Auto_Trade: m.auto_trade || '',
        Typeaccount: m.typeaccount || '',
        timecurent: m.timecurent || '',
        timeUpdated: m.timeUpdated || '',
      },
      data
    );

    out.push(item);
  }

  out.sort((a, b) => Number(a.Index ?? 0) - Number(b.Index ?? 0));
  return out;
}

// =====================
// INTERNAL: BEST SYMBOL BY SMALLEST INDEX + trade === "TRUE"
// includeOHLC:
// - false: no ohlc
// - true: attach ohlc from chart key (only exists if watched) OR empty
// =====================
async function _getBestSymbolByIndex(symbol, opts = { includeOHLC: false, tf: 'M1' }) {
  await ensureConnected();
  const r = assertRedis();
  const cfg = r.__cfg || _cfg;

  const sym = String(symbol || '').trim().toUpperCase();
  if (!sym) return null;

  let cursorKeys = '0';
  let best = null;
  let bestIndex = Infinity;

  do {
    const [next, keys] = await r.scan(cursorKeys, 'MATCH', 'broker:*:symbols', 'COUNT', 200);
    cursorKeys = next;

    if (!keys || keys.length === 0) continue;

    const pipe = r.pipeline();
    const brokerIds = [];

    for (const skey of keys) {
      const parts = String(skey).split(':');
      const broker_ = parts.length >= 3 ? parts[1] : '';
      if (!broker_) continue;

      brokerIds.push(broker_);
      pipe.hget(skey, sym);
      pipe.hgetall(keyMeta(broker_));
    }

    const rows = await pipe.exec();

    for (let i = 0, bi = 0; i < rows.length; i += 2, bi++) {
      const [errVal, val] = rows[i];
      const [errMeta, meta] = rows[i + 1];

      if (errVal) throw new Error(`getBestSymbol HGET error: ${errVal.message || errVal}`);
      if (errMeta) throw new Error(`getBestSymbol meta error: ${errMeta.message || errMeta}`);

      if (val == null || !meta || meta.index == null) continue;

      const idx = Number(meta.index);
      if (!Number.isFinite(idx)) continue;

      const data = unpackValue(val, cfg);
      if (!data || typeof data !== 'object') continue;

      if (String(data.trade || '').toUpperCase() !== 'TRUE') continue;

      // base: NO ohlc
      if (data.ohlc) delete data.ohlc;

      if (idx < bestIndex) {
        bestIndex = idx;
        best = Object.assign(
          {
            Broker: meta.broker || (meta.broker_ ? String(meta.broker_).toUpperCase() : ''),
            Broker_: meta.broker_ || brokerIds[bi],
            Status: meta.status || '',
            Index: String(meta.index),
            Auto_Trade: meta.auto_trade || '',
            Typeaccount: meta.typeaccount || '',
            timecurent: meta.timecurent || '',
            timeUpdated: meta.timeUpdated || '',
          },
          data
        );
      }
    }
  } while (cursorKeys !== '0');

  if (!best) return null;

  // includeOHLC => lấy từ key chart riêng (nếu đang watched / đã được saveBrokerBatch set)
  if (opts.includeOHLC) {
    const tf = String(opts.tf || 'M1').toUpperCase();
    const snap = await getChartOHLC(best.Broker_, sym, tf);
    best.ohlc = snap?.ohlc || [];
  }

  return best;
}

// PUBLIC
async function getBestSymbolFast(symbol) {
  return _getBestSymbolByIndex(symbol, { includeOHLC: false, tf: 'M1' });
}
async function getBestSymbolFull(symbol, tf = 'M1') {
  return _getBestSymbolByIndex(symbol, { includeOHLC: true, tf });
}

// =====================
// FIND BROKER BY INDEX
// =====================
async function findBrokerByIndex(index) {
  await ensureConnected();
  const r = assertRedis();

  const idx = String(index);
  let cursor = '0';

  do {
    const [next, keys] = await r.scan(cursor, 'MATCH', 'broker:*:meta', 'COUNT', 200);
    cursor = next;

    if (keys.length > 0) {
      const pipe = r.pipeline();
      keys.forEach(k => pipe.hgetall(k));
      const rows = await pipe.exec();

      for (const [, meta] of rows) {
        if (!meta || !meta.index) continue;
        if (String(meta.index) === idx) {
          return {
            broker: meta.broker || '',
            broker_: meta.broker_ || '',
            index: meta.index,
            port: meta.port || '',
            version: meta.version || '',
            typeaccount: meta.typeaccount || '',
            status: meta.status || '',
            auto_trade: meta.auto_trade || '',
            timecurent: meta.timecurent || '',
            timeUpdated: meta.timeUpdated || '',
          };
        }
      }
    }
  } while (cursor !== '0');

  return null;
}

// =====================
// UPDATE BROKER STATUS (your old style: meta hash)
// =====================
async function updateBrokerStatus(broker_, newStatus) {
  await ensureConnected();
  const r = assertRedis();
  const metaKey = keyMeta(broker_);

  const exists = await r.exists(metaKey);
  if (!exists) return null;

  const timeUpdated = new Date().toISOString().slice(0, 19).replace('T', ' ');
  await r.hmset(
    metaKey,
    'status', String(newStatus),
    'timeUpdated', String(timeUpdated),
    'lastWriteMs', String(Date.now())
  );

  const meta = await r.hgetall(metaKey);
  return meta && Object.keys(meta).length ? meta : null;
}

// =====================
// EXPORT SAFE
// =====================
async function exportBrokerSymbolsNDJSON(broker_, filePath) {
  await ensureConnected();
  const r = assertRedis();
  const cfg = r.__cfg || _cfg;

  const symbolsKey = keySymbols(broker_);
  fs.mkdirSync(path.dirname(filePath), { recursive: true });

  const ws = fs.createWriteStream(filePath, { encoding: 'utf8' });

  let cursor = '0';
  let count = 0;

  do {
    const [next, arr] = await r.hscan(symbolsKey, cursor, 'COUNT', 200);
    cursor = next;

    for (let i = 0; i < arr.length; i += 2) {
      const sym = arr[i];
      const val = arr[i + 1];
      ws.write(JSON.stringify({ symbol: sym, data: unpackValue(val, cfg) }) + '\n');
      count++;
    }
  } while (cursor !== '0');

  ws.end();
  return { ok: true, filePath, count };
}

// =====================
// DEBUG
// =====================
async function debugScanKeys(pattern = '*', count = 200, limit = 2000) {
  await ensureConnected();
  const r = assertRedis();

  let cursor = '0';
  const keys = [];

  do {
    const [next, chunk] = await r.scan(cursor, 'MATCH', pattern, 'COUNT', count);
    cursor = next;
    keys.push(...chunk);
  } while (cursor !== '0' && keys.length < limit);

  return keys;
}

async function clearAllData() {
  await ensureConnected();
  const r = assertRedis();

  console.log('[RedisH] FLUSHDB start...');
  await r.flushdb();
  console.log('[RedisH] FLUSHDB done');
  return true;
}

module.exports = {
  // core
  initRedis,
  getRedis,
  ensureConnected,
  closeRedis,

  // write
  saveBrokerBatch,
 savePriceData,
  saveOHLCData,
  saveBrokerPrice,
  saveBrokerOHLC,
  saveAllBroker,
  // base read
  getBrokerMeta,
  getSymbol,
  getSymbols,
  getBroker,

  // list / union
  getUnionSymbols,
  getUnionSymbolsAllBrokers,
  getSymbolAllBroker,
  getMultipleSymbolDetails_RedisH,
  Broker_names,

  // best symbol
  getBestSymbolFast,
  getBestSymbolFull,

  // find
  findBrokerByIndex,

  // chart on-demand
  chartOpen,
  chartClose,
  isChartActive,
  getChartOHLC,
  getAllBrokers,



  // misc
  exportBrokerSymbolsNDJSON,
  updateBrokerStatus,
  debugScanKeys,
  clearAllData,

  getPublisher,
  getSubscriber,
  ensureConnectedPubSub,

  chartSubscribe,
  getChartOHLC,
  getTotalBrokers,
  publishOHLC,
  publishTickFromSymbol,
  getSubscriber,
keyWatch,
keyChartOHLC,
watchSymbol,
refreshWatch,
unwatchSymbol,
unpackValue,
  packValue,
  deleteBroker,
  saveBrokerMeta,

  // expose channels helpers (optional, dùng trong WS web)
  __channels: { chTick, chOHLC },
  __keys: { keyWatch, keyChartOHLC, keySymbols, keyMeta, keySymbolMeta },

  // ✅ THÊM 2 DÒNG NÀY ĐỂ SUPPORT MULTI/PIPELINE
  multi: () => getRedis().multi(),
  pipeline: () => getRedis().pipeline(), // alias nếu cần
  keys: (pattern) => getRedis().keys(pattern),
  scan: (cursor, options) => getRedis().scan(cursor, options),
};
