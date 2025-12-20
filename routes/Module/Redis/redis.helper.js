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
  const keyChartOHLC = (b, s) => `chart:ohlc:${safeKeyPart(b)}:${String(s).toUpperCase().trim()}`;

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
    broker: broker,
    broker_: broker_,
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
  if (ms > 20) {
    console.log(`[SAVE OK] broker=${broker_} symbols=${wrote} time=${ms}ms`);
  }

  return {
    ok: true,
    broker_,
    wrote,
    ms,
    keys: { symbolsKey, metaKey },
  };
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
async function getBrokerMeta(broker_) {
  await ensureConnected();
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
    const [next, keys] = await r.scan(cursorKeys, 'MATCH', 'broker:*:symbols', 'COUNT', 200);
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

  // base read
  getBrokerMeta,
  getSymbol,
  getSymbols,
  getBroker,

  // list / union
  getUnionSymbols,
  getUnionSymbolsAllBrokers,
  getSymbolAllBroker,
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

  // expose channels helpers (optional, dùng trong WS web)
  __channels: { chTick, chOHLC },
  __keys: { keyWatch, keyChartOHLC, keySymbols, keyMeta },
};
