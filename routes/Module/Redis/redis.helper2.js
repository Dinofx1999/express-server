// redis.helper2.js  (Redis 3.0 compatible)
// - Atomic UPSERT snapshots (only update if receivedAt is newer) via Lua (no unpack)
// - Uses HMSET for multi-field writes (Redis 3.0 does NOT support multi-field HSET)
// - Indexes: brokers (SET), symbols:<broker> (SET)
// - Snapshots: snap:<broker>:<symbol> (HASH)
// - Meta: broker_meta:<broker> (HASH)
// - Includes query + cleanup helpers

"use strict";

const Redis = require("ioredis");


// ===================== CONFIG =====================
const DEFAULT_REDIS_CONFIG = {
  host: process.env.REDIS_HOST || "127.0.0.1",
  port: process.env.REDIS_PORT ? Number(process.env.REDIS_PORT) : 6379,
  password: process.env.REDIS_PASSWORD || undefined,
  db: process.env.REDIS_DB ? Number(process.env.REDIS_DB) : 0,

  // realtime-friendly
  maxRetriesPerRequest: 3,
  enableReadyCheck: true,
  lazyConnect: false,
  retryStrategy: (times) => Math.min(times * 50, 2000),
};

// Keys
const KEY_BROKERS_SET = "brokers"; // SET
const KEY_SYMBOLS_SET_PREFIX = "symbols:"; // SET: symbols:<broker>
const KEY_SNAP_PREFIX = "snap:"; // HASH: snap:<broker>:<symbol>
const KEY_BROKER_META_PREFIX = "broker_meta:"; // HASH: broker_meta:<broker>

// TTL (optional)
const SNAP_TTL_SECONDS = process.env.REDIS_SNAP_TTL ? Number(process.env.REDIS_SNAP_TTL) : 0;
const META_TTL_SECONDS = process.env.REDIS_META_TTL ? Number(process.env.REDIS_META_TTL) : 0;

// ===================== SINGLETON =====================
let _redis;

function normalizeBroker(broker) {
  if (!broker) return broker;
  return String(broker).trim().toLowerCase();
}

function getRedis() {
  if (_redis) return _redis;

  _redis = new Redis(DEFAULT_REDIS_CONFIG);



  _redis.on("connect", () => {
    // console.log("[REDIS] connected");
  });

  _redis.on("error", (err) => {
    console.error("[REDIS] error:", err?.message || err);
  });

  // Redis 3.0 compatible Lua (no unpack)
  _redis.defineCommand("upsertIfNewer", {
    numberOfKeys: 1,
    lua: `
      local key = KEYS[1]
      local newRecv = tonumber(ARGV[1]) or 0

      local cur = tonumber(redis.call('HGET', key, 'receivedAt')) or -1
      if cur ~= -1 and cur >= newRecv then
        return 0
      end

      local n = #ARGV
      -- ARGV[1] = receivedAt; remaining must be field/value pairs => (n-1) must be even
      if ((n - 1) % 2) ~= 0 then
        return redis.error_reply('FIELD_VALUE_NOT_EVEN')
      end

      for i = 2, n, 2 do
        -- HSET key field value (single pair) -> OK on Redis 3.0
        redis.call('HSET', key, ARGV[i], ARGV[i+1])
      end

      redis.call('HSET', key, 'receivedAt', tostring(newRecv))
      return 1
    `,
  });

  return _redis;
}

// ===================== UTIL =====================
function normalizeEntries(dataMap) {
  if (!dataMap) return [];
  if (dataMap instanceof Map) return [...dataMap.entries()];
  if (typeof dataMap === "object") return Object.entries(dataMap);
  return [];
}

function safeParseKey(mapKey) {
  if (!mapKey || typeof mapKey !== "string") return null;
  const idx = mapKey.indexOf(":");
  if (idx === -1) return null;

  const broker = normalizeBroker(mapKey.slice(0, idx));
  const symbol = mapKey.slice(idx + 1);

  if (!broker || !symbol) return null;
  return { broker, symbol };
}

function toFieldValueArgs(obj) {
  // => [field1, value1, field2, value2, ...] (always even length if built correctly)
  const out = [];
  for (const [k, v] of Object.entries(obj || {})) {
    if (v === undefined) continue;
    if (v === null) out.push(k, "null");
    else if (typeof v === "object") out.push(k, JSON.stringify(v));
    else out.push(k, String(v));
  }
  return out;
}
function keyChartOHLC(broker_, symbol) {
  return `chart:ohlc:${String(broker_ || '').trim()}:${String(symbol || '').toUpperCase().trim()}`;
}

function normalizeOHLCArray(ohlc = []) {
  if (!Array.isArray(ohlc)) return [];
  const out = [];
  for (const c of ohlc) {
    if (!c) continue;
    const time = String(c.time || '').trim();
    if (!time) continue;

    const o = Number.parseFloat(c.open);
    const h = Number.parseFloat(c.high);
    const l = Number.parseFloat(c.low);
    const cl = Number.parseFloat(c.close);

    out.push({
      time,
      open: Number.isFinite(o) ? o : c.open,
      high: Number.isFinite(h) ? h : c.high,
      low: Number.isFinite(l) ? l : c.low,
      close: Number.isFinite(cl) ? cl : c.close,
    });
  }
  return out;
}
function formatString(str) {
  if (!str) return '';
  return String(str).trim().toLowerCase();
} 
async function saveOHLC_SNAP_ArrayOnly(payload, opts = {}) {
  const { ttlSec = 3600, maxCandles = 600 } = opts;

  // ✅ DEBUG: biết chắc có vào hàm
  // console.log('[OHLC_SNAP] payload type:', Array.isArray(payload) ? payload?.[0]?.Type : payload?.Type);

  const msg = Array.isArray(payload) ? payload[0] : payload;
  if (!msg || msg.Type !== 'OHLC_SNAP') return { ok: false, reason: 'NOT_OHLC_SNAP' };
 
  const data = msg.data || {};
  // MT4 gửi broker_ nhưng bạn cần chuẩn hóa như WS_CHART
  const brokerRaw_ = String(data.broker_ || '').trim();
  const broker_ = formatString ? formatString(brokerRaw_) : brokerRaw_.toLowerCase();

  const symbols = Array.isArray(data.Symbols) ? data.Symbols : [];
  if (!broker_ || symbols.length === 0) return { ok: false, reason: 'INVALID_DATA', brokerRaw_ };

  const redis = getRedis();              // ✅ dùng đúng client của file này
  const pipe = redis.pipeline();

  let saved = 0;

  for (const s of symbols) {
    const symbol = String(s?.symbol || '').toUpperCase().trim();
    if (!symbol) continue;

    let ohlcArr = normalizeOHLCArray(s?.ohlc);
    if (maxCandles > 0 && ohlcArr.length > maxCandles) {
      ohlcArr = ohlcArr.slice(-maxCandles);
    }

    const key = keyChartOHLC(broker_, symbol);

    // ✅ lưu JSON string (WS_CHART sẽ parse/unpack tuỳ bạn, bên dưới mình chỉ cách)
    pipe.set(key, JSON.stringify(ohlcArr));
    if (ttlSec && ttlSec > 0) pipe.expire(key, ttlSec);

    saved++;
  }

  if (saved === 0) return { ok: false, reason: 'NO_VALID_SYMBOLS', broker_ };

  const res = await pipe.exec();
  // debug pipeline errors
  for (const [err] of res) if (err) console.error('[OHLC_PIPE_ERR]', err);

  return { ok: true, broker_, saved };
}
function snapKey(broker, symbol) {
  return `${KEY_SNAP_PREFIX}${broker}:${symbol}`;
}
function symbolsKey(broker) {
  return `${KEY_SYMBOLS_SET_PREFIX}${broker}`;
}
function brokerMetaKey(broker) {
  return `${KEY_BROKER_META_PREFIX}${broker}`;
}

// ===================== CORE: UPSERT SNAPSHOTS =====================

/**
 * Atomic UPSERT snapshots (anti out-of-order):
 * - Insert if key not exist
 * - Update only if receivedAt is newer than stored receivedAt
 *
 * dataMap:
 * - Map([["B:USDCHF", payload], ...]) or Object({ "B:USDCHF": payload, ... })
 *
 * options:
 * - setMeta (default true)
 * - ttlSeconds (default SNAP_TTL_SECONDS)
 */

// Normalize OHLC array (MT4 gửi string -> parseFloat)
function normalizeOHLCArray(ohlc = []) {
  if (!Array.isArray(ohlc)) return [];

  const out = [];
  for (const c of ohlc) {
    if (!c) continue;

    const time = String(c.time || '').trim();
    if (!time) continue;

    const o = Number.parseFloat(c.open);
    const h = Number.parseFloat(c.high);
    const l = Number.parseFloat(c.low);
    const cl = Number.parseFloat(c.close);

    out.push({
      time,
      open: Number.isFinite(o) ? o : c.open,
      high: Number.isFinite(h) ? h : c.high,
      low: Number.isFinite(l) ? l : c.low,
      close: Number.isFinite(cl) ? cl : c.close,
    });
  }
  return out;
}

/**
 * Save OHLC_SNAP payload (from MT4) to Redis as ARRAY (so getOHLC() returns Array)
 *
 * payload format:
 * [ { Type:"OHLC_SNAP", data:{ broker_:"abc", Symbols:[ {symbol:"EURUSD", ohlc:[...]} ] } } ]
 *
 * @param {any} payload
 * @param {{ ttlSec?: number, maxCandles?: number }} opts
 * @returns {Promise<{ok:boolean, broker_?:string, saved?:number, reason?:string}>}
 */

async function upsertSnapshotsAtomic(dataMap, options = {}) {
  const redis = getRedis();

  const entries = normalizeEntries(dataMap);
  if (!entries.length) return { updated: 0, skipped: 0, total: 0 };

  const setMeta = options.setMeta !== undefined ? !!options.setMeta : true;
  const ttlSeconds = options.ttlSeconds !== undefined ? Number(options.ttlSeconds) : SNAP_TTL_SECONDS;

  const pipe = redis.pipeline();

  // command count per entry depends on options:
  // sadd brokers (1) + sadd symbols (1) + upsert (1) + expire snap (optional 1) + hmset meta (optional 1) + expire meta (optional 1)
  const perEntry =
    3 +
    (ttlSeconds && ttlSeconds > 0 ? 1 : 0) +
    (setMeta ? 1 : 0) +
    (setMeta && META_TTL_SECONDS > 0 ? 1 : 0);

  for (const [mapKey, payloadRaw] of entries) {
    const payload = payloadRaw || {};

    // ensure receivedAt exists
    if (!payload.receivedAt) payload.receivedAt = Date.now();

    const parsed = safeParseKey(mapKey);

const rawBroker = payload.broker || (parsed ? parsed.broker : null);
const broker = normalizeBroker(rawBroker);

const symbol = payload.symbol || (parsed ? parsed.symbol : null);
    if (!broker || !symbol) continue;

    // indexes
    pipe.sadd(KEY_BROKERS_SET, broker);
    pipe.sadd(symbolsKey(broker), symbol);

    // atomic upsert
    const key = snapKey(broker, symbol);
    const recv = Number(payload.receivedAt || Date.now());
    const fv = toFieldValueArgs(payload);

    if (fv.length % 2 !== 0) {
      console.error("[REDIS] Odd field/value length - skip:", broker, symbol);
      continue;
    }

    pipe.upsertIfNewer(key, recv, ...fv);

    if (ttlSeconds && ttlSeconds > 0) {
      pipe.expire(key, ttlSeconds);
    }

    // meta (Redis 3.0: MUST HMSET for multi-field)
    if (setMeta) {
  // payload.broker ở hệ mới của bạn nên là broker_ lowercase để làm key
  // payload.broker_display hoặc payload.brokerName (nếu có) để hiển thị
  const meta = {
    // ✅ các field bạn cần xuất
    index: payload.index != null ? String(payload.index) : (payload.indexBroker != null ? String(payload.indexBroker) : ""),
    broker: payload.broker_display != null ? String(payload.broker_display) : (payload.brokerName != null ? String(payload.brokerName) : String(payload.broker || "")),
    version: payload.version != null ? String(payload.version) : "",
    totalsymbol: payload.totalsymbol != null ? String(payload.totalsymbol) : "",
    timecurent: payload.timecurent != null ? String(payload.timecurent) : "",   // đúng spelling MT4
    status: payload.status != null ? String(payload.status) : "",
    typeaccount: payload.typeaccount != null ? String(payload.typeaccount) : "",

    // ✅ các field vẫn nên giữ
    port: payload.port != null ? String(payload.port) : "",
    last_reset: payload.last_reset != null ? String(payload.last_reset) : "",
    timecurrent: payload.timecurrent != null ? String(payload.timecurrent) : "",
    timeUpdated: payload.timeUpdated != null ? String(payload.timeUpdated) : "",
    receivedAt: String(recv),
  };

  pipe.hmset(brokerMetaKey(broker), meta);

  if (META_TTL_SECONDS && META_TTL_SECONDS > 0) {
    pipe.expire(brokerMetaKey(broker), META_TTL_SECONDS);
  }
}
  }

  const res = await pipe.exec();

  // Print pipeline errors (important for debugging)
  for (const [err] of res) {
    if (err) console.error("[REDIS_PIPE_ERR]", err);
  }

  // Count updated/skipped only for upsertIfNewer command per entry:
  // offset: sadd(0), sadd(1), upsert(2)
  let updated = 0;
  let skipped = 0;
  let total = 0;

  for (let i = 0; i < entries.length; i++) {
    const baseIndex = i * perEntry;
    const upsertIndex = baseIndex + 2;

    const item = res[upsertIndex];
    if (!item) continue;

    const [err, val] = item;
    if (err) continue;

    total++;
    if (Number(val) === 1) updated++;
    else skipped++;
  }

  return { updated, skipped, total };
}

// ===================== READ HELPERS =====================

async function getAllBrokers() {
  const redis = getRedis();
  return await redis.smembers(KEY_BROKERS_SET);
}

async function getBrokerResetting() {
  const redis = getRedis(); // trong redis.helper2.js
  const brokerList = await getAllBrokers(); // ['b','ab',...]

  if (!brokerList || brokerList.length === 0) return [];

  // lấy meta của tất cả broker một lần (nhanh)
  const pipe = redis.pipeline();
  for (const b of brokerList) pipe.hgetall(`broker_meta:${b}`);
  const res = await pipe.exec();

  const brokers = [];
  for (let i = 0; i < brokerList.length; i++) {
    const broker_ = brokerList[i];
    const [err, meta] = res[i] || [];
    if (err) continue;

    const m = meta || {};
    // index ưu tiên indexBroker, fallback index
    const idx = (m.indexBroker != null ? m.indexBroker : m.index);

    brokers.push({
      broker_: broker_,
      broker: m.broker || broker_.toUpperCase(),
      status: m.status ?? '',
      index: idx ?? '',
      indexBroker: m.indexBroker ?? '',
      totalsymbol: m.totalsymbol ?? '',
      timeUpdated: m.timeUpdated ?? '',
      timecurent: m.timecurent ?? '',
      typeaccount: m.typeaccount ?? '',
      port: m.port ?? '',
    });
  }

  // giữ cùng logic cũ: status !== "True"
  return brokers
    .filter(b => String(b.status) !== 'True')
    .sort((a, b) => Number(a.index || a.indexBroker || 999999999) - Number(b.index || b.indexBroker || 999999999));
}


/**
 * Delete all Redis keys matching a pattern
 * Example: await deleteByPattern('chart:ohlc:*')
 */
async function deleteByPattern(pattern) {
  if (!pattern) return { deleted: 0 };

  const redis = getRedis();
  let cursor = '0';
  let totalDeleted = 0;

  do {
    const [next, keys] = await redis.scan(cursor, 'MATCH', pattern, 'COUNT', 1000);
    cursor = next;

    if (keys.length > 0) {
      await redis.del(...keys);
      totalDeleted += keys.length;
    }
  } while (cursor !== '0');

  return { deleted: totalDeleted };
}


async function getSymbolsByBroker(broker) {
  const redis = getRedis();
  if (!broker) return [];
  return await redis.smembers(symbolsKey(broker));
}
/**
 * Update broker status for all symbols
 * @param {string} broker - broker code (vd: 'b', 'ab')
 * @param {string} status - e.g. 'ACTIVE', 'OFFLINE', 'MAINTAIN'
 */
async function setBrokerStatus(broker, status) {
  if (!broker) throw new Error('broker is required');

  const redis = getRedis();
  const broker_ = String(broker).toLowerCase();
  const statusStr = String(status);

  // 1️⃣ Update broker meta
  await redis.hset(`broker_meta:${broker_}`, 'status', statusStr);

  // 2️⃣ Update all snapshot keys of this broker
  const symbols = await redis.smembers(`symbols:${broker_}`);
  if (!symbols || symbols.length === 0) {
    return {
      ok: true,
      broker: broker_,
      updated: 0,
      note: 'No symbols found'
    };
  }

  const pipe = redis.pipeline();
  for (const sym of symbols) {
    pipe.hset(`snap:${broker_}:${sym}`, 'status', statusStr);
  }

  await pipe.exec();

  return {
    ok: true,
    broker: broker_,
    updated: symbols.length,
    status: statusStr
  };
}

async function getSnapshot(broker, symbol) {
  const redis = getRedis();
  if (!broker || !symbol) return {};
  return await redis.hgetall(snapKey(broker, symbol));
}

async function getAllSnapshotsByBroker(broker) {
  const redis = getRedis();
  if (!broker) return {};

  const symbols = await redis.smembers(symbolsKey(broker));
  if (!symbols.length) return {};

  const pipe = redis.pipeline();
  for (const sym of symbols) pipe.hgetall(snapKey(broker, sym));
  const res = await pipe.exec();

  const out = {};
  symbols.forEach((sym, i) => {
    const [err, hash] = res[i] || [];
    if (!err && hash && Object.keys(hash).length) out[`${broker}:${sym}`] = hash;
  });

  return out;
}

async function getSymbolAcrossBrokers(symbol) {
  const redis = getRedis();
  if (!symbol) return {};

  const brokers = await redis.smembers(KEY_BROKERS_SET);
  if (!brokers.length) return {};

  const pipe = redis.pipeline();
  for (const b of brokers) pipe.hgetall(snapKey(b, symbol));
  const res = await pipe.exec();

  const out = {};
  brokers.forEach((b, i) => {
    const [err, hash] = res[i] || [];
    if (!err && hash && Object.keys(hash).length) out[`${b}:${symbol}`] = hash;
  });

  return out;
}

async function getBrokerMeta(broker) {
  const redis = getRedis();
  if (!broker) return {};
  return await redis.hgetall(brokerMetaKey(broker));
}

// ===================== DELETE / CLEANUP =====================

async function deleteSymbol(broker, symbol) {
  const redis = getRedis();
  if (!broker || !symbol) return { deleted: 0 };

  const pipe = redis.pipeline();
  pipe.del(snapKey(broker, symbol));
  pipe.srem(symbolsKey(broker), symbol);
  const res = await pipe.exec();

  const deleted = !res[0]?.[0] ? Number(res[0]?.[1] || 0) : 0;
  return { deleted };
}

async function deleteBroker(broker) {
  const redis = getRedis();
  if (!broker) return { deletedKeys: 0 };

  const symbols = await redis.smembers(symbolsKey(broker));

  const pipe = redis.pipeline();
  for (const sym of symbols) pipe.del(snapKey(broker, sym));

  pipe.del(symbolsKey(broker));
  pipe.del(brokerMetaKey(broker));
  pipe.srem(KEY_BROKERS_SET, broker);

  const res = await pipe.exec();

  let deletedKeys = 0;
  for (const [err, val] of res) {
    if (!err && typeof val === "number") deletedKeys += val;
  }

  return { deletedKeys };
}

async function flushAll() {
  const redis = getRedis();
  return await redis.flushdb();
}

// ===================== EXPORTS =====================
module.exports = {
  getRedis,

  // write
  upsertSnapshotsAtomic,
saveOHLC_SNAP_ArrayOnly,
setBrokerStatus,
  // read
  getAllBrokers,
  getSymbolsByBroker,
  getSnapshot,
  getAllSnapshotsByBroker,
  getSymbolAcrossBrokers,
  getBrokerMeta,
  getBrokerResetting,

  // cleanup
  deleteSymbol,
  deleteBroker,
  flushAll,
  deleteByPattern
};
