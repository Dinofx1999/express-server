// redis.helper2.js  (Redis 3.0 compatible) - OPTIMIZED v2.2 (Option 1.1 FIXED)
// ✅ FAST write path: HMSET
// ✅ NO-LOSS: 2-phase guard (HGET receivedAt + HGET tud) -> only HMSET newer
// ✅ Broker key normalize: "2 ABC" => "2-abc"
// ✅ timetrade always JSON string (never [object Object])
// ✅ Maintain best broker per symbol using ZSET: bestz:<SYMBOL>
// ✅ On delete broker: remove from bestz to avoid stale best
// ✅ OHLC stored at chart:ohlc:<broker_>:<SYMBOL> as JSON string (default ttlSec=0)

"use strict";

const { raw } = require("express");
const Redis = require("ioredis");

// ===================== CONFIG =====================
const DEFAULT_REDIS_CONFIG = {
  host: process.env.REDIS_HOST || "127.0.0.1",
  port: process.env.REDIS_PORT ? Number(process.env.REDIS_PORT) : 6379,
  password: process.env.REDIS_PASSWORD || undefined,
  db: process.env.REDIS_DB ? Number(process.env.REDIS_DB) : 0,

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
const KEY_BESTZ_PREFIX = "bestz:"; // ZSET: bestz:<SYMBOL>

// TTL (optional) - recommended 0 for realtime
const SNAP_TTL_SECONDS = process.env.REDIS_SNAP_TTL ? Number(process.env.REDIS_SNAP_TTL) : 0;
const META_TTL_SECONDS = process.env.REDIS_META_TTL ? Number(process.env.REDIS_META_TTL) : 0;

// ===================== SINGLETON =====================
let _redis;
function getRedis() {
  if (_redis) return _redis;
  _redis = new Redis(DEFAULT_REDIS_CONFIG);
  _redis.on("error", (err) => console.error("[REDIS] error:", err?.message || err));
  return _redis;
}

// ===================== NORMALIZE =====================
function normalizeBrokerKey(broker) {
  // broker có thể là "2 ABC" hoặc "AB", hoặc đã là "2-abc"
  if (!broker) return "";
  return String(broker)
    .trim()
    .toLowerCase()
    .replace(/[\s_]+/g, "-")        // space/_ => "-"
    .replace(/-+/g, "-")            // nhiều "-" => 1 "-"
    .replace(/[^a-z0-9-]/g, "")     // bỏ ký tự lạ
    .replace(/^-+|-+$/g, "");       // trim "-"
}
function normalizeBroker(broker) {
  return normalizeBrokerKey(broker);
}
function normalizeSymbol(symbol) {
  return String(symbol || "").toUpperCase().trim();
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
  const symbol = normalizeSymbol(mapKey.slice(idx + 1));
  if (!broker || !symbol) return null;
  return { broker, symbol };
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
function bestzKey(symbol) {
  return `${KEY_BESTZ_PREFIX}${normalizeSymbol(symbol)}`;
}
function keyChartOHLC(broker_, symbol) {
  return `chart:ohlc:${String(broker_ || "").trim()}:${normalizeSymbol(symbol)}`;
}
function isTradeTrue(v) {
  return String(v || "").toUpperCase() === "TRUE";
}
function toNumberOrNull(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

// ---- stringify helpers (fix [object Object]) ----
function safeJSONStringify(v) {
  try {
    return JSON.stringify(v);
  } catch {
    return "";
  }
}
function normalizeTimeTradeField(v) {
  // MT4 có thể gửi string, array, object
  if (v == null) return "";
  if (typeof v === "string") return v; // có thể là JSON string rồi
  if (Array.isArray(v) || typeof v === "object") return safeJSONStringify(v);
  return String(v);
}

// ===================== OHLC =====================
function normalizeOHLCArray(ohlc = []) {
  if (!Array.isArray(ohlc)) return [];
  const out = [];
  for (const c of ohlc) {
    if (!c) continue;
    const time = String(c.time || "").trim();
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

async function saveOHLC_SNAP_ArrayOnly(payload, opts = {}) {
  const { ttlSec = 0, maxCandles = 600 } = opts;

  const msg = Array.isArray(payload) ? payload[0] : payload;
  if (!msg || msg.Type !== "OHLC_SNAP") return { ok: false, reason: "NOT_OHLC_SNAP" };

  const data = msg.data || {};
  const broker_ = normalizeBrokerKey(String(data.broker_ || "").trim());
  const symbols = Array.isArray(data.Symbols) ? data.Symbols : [];
  if (!broker_ || symbols.length === 0) return { ok: false, reason: "INVALID_DATA" };

  const redis = getRedis();
  const pipe = redis.pipeline();

  let saved = 0;
  for (const s of symbols) {
    const symbol = normalizeSymbol(s?.symbol);
    if (!symbol) continue;

    let ohlcArr = normalizeOHLCArray(s?.ohlc);
    if (maxCandles > 0 && ohlcArr.length > maxCandles) ohlcArr = ohlcArr.slice(-maxCandles);

    const key = keyChartOHLC(broker_, symbol);
    pipe.set(key, JSON.stringify(ohlcArr));
    if (ttlSec && ttlSec > 0) pipe.expire(key, ttlSec);
    saved++;
  }

  if (saved === 0) return { ok: false, reason: "NO_VALID_SYMBOLS", broker_ };

  const res = await pipe.exec();
  for (const [err] of res) if (err) console.error("[OHLC_PIPE_ERR]", err);
  return { ok: true, broker_, saved };
}

// ===================== UPSERT SNAPSHOTS (NO-LOSS GUARD) =====================
/**
 * Guard strategy:
 * - Prefer MT4 monotonic `tud` if present (or payload.tud)
 * - Fallback to `receivedAt`
 * We read both: HGET tud + HGET receivedAt
 * Update only if (newTud > curTud) OR (no tud: newRecv > curRecv)
 *
 * ✅ Fix "phải đổi broker/index mới update":
 *   Nếu trước đây tud/receivedAt không tăng đúng, guard sẽ block.
 *   Với logic ưu tiên tud và fallback hợp lý, sẽ không bị kẹt.
 */
async function upsertSnapshotsAtomic(dataMap, options = {}) {
  const redis = getRedis();

  const entries = normalizeEntries(dataMap);
  if (!entries.length) return { updated: 0, skipped: 0, total: 0, mode: "FAST_HMSET_GUARD_V2" };

  const setMeta = options.setMeta !== undefined ? !!options.setMeta : true;
  const ttlSeconds = options.ttlSeconds !== undefined ? Number(options.ttlSeconds) : SNAP_TTL_SECONDS;

  const norm = [];
  for (const [mapKey, payloadRaw] of entries) {
    const payload = payloadRaw || {};
    const parsed = safeParseKey(mapKey);

    const rawBroker = payload.broker_ || payload.broker || (parsed ? parsed.broker : null);
    const broker = normalizeBroker(rawBroker);

    const symbol = normalizeSymbol(payload.symbol || (parsed ? parsed.symbol : null));
    if (!broker || !symbol) continue;

    // ✅ take tud if exists (should be monotonic per MT4 connection)
    const newTud = payload.tud != null ? Number(payload.tud) : null;

    // ✅ receivedAt fallback
    const recv = Number(payload.receivedAt || Date.now());
    payload.receivedAt = recv;

    // ✅ normalize critical fields that often become [object Object]
    payload.timetrade = normalizeTimeTradeField(payload.timetrade);

    // also keep symbol uppercase in hash
    payload.symbol = symbol;
    payload.broker_ = broker;

    norm.push({ broker, symbol, recv, newTud, payload });
  }

  if (!norm.length) return { updated: 0, skipped: 0, total: 0, mode: "FAST_HMSET_GUARD_V2" };

  // Phase A: read current tud + receivedAt
  const pipeA = redis.pipeline();
  for (const it of norm) {
    const key = snapKey(it.broker, it.symbol);
    pipeA.hmget(key, "tud", "receivedAt");
  }
  const resA = await pipeA.exec();

  // Phase B: write only newer
  const pipeB = redis.pipeline();

  let total = 0;
  let updated = 0;
  let skipped = 0;

  for (let i = 0; i < norm.length; i++) {
    const it = norm[i];
    const [err, vals] = resA[i] || [];
    const curTud = !err && vals ? Number(vals[0] || 0) : 0;
    const curRecv = !err && vals ? Number(vals[1] || 0) : 0;

    total++;

    // decide shouldWrite
    let shouldWrite = true;

    if (it.newTud != null && Number.isFinite(it.newTud) && it.newTud > 0) {
      // ✅ tud guard
      if (curTud && curTud >= it.newTud) shouldWrite = false;
    } else {
      // ✅ receivedAt guard
      if (curRecv && curRecv >= it.recv) shouldWrite = false;
    }

    if (!shouldWrite) {
      skipped++;
      continue;
    }

    // ✅ ensure sets exist (visibility)
    pipeB.sadd(KEY_BROKERS_SET, it.broker);
    pipeB.sadd(symbolsKey(it.broker), it.symbol);

    // ✅ snapshot write
    const key = snapKey(it.broker, it.symbol);

    // ensure strings in redis hash
    const snapObj = { ...it.payload };
    snapObj.receivedAt = String(it.recv);
    if (it.newTud != null && Number.isFinite(it.newTud)) snapObj.tud = String(it.newTud);

    // ✅ HARD FIX: timetrade must be JSON string
    snapObj.timetrade = normalizeTimeTradeField(snapObj.timetrade);

    pipeB.hmset(key, snapObj);

    if (ttlSeconds && ttlSeconds > 0) pipeB.expire(key, ttlSeconds);

    // ✅ bestz maintenance
    const idx = toNumberOrNull(snapObj.indexBroker ?? snapObj.index);
    if (idx !== null && isTradeTrue(snapObj.trade)) pipeB.zadd(bestzKey(it.symbol), idx, it.broker);
    else pipeB.zrem(bestzKey(it.symbol), it.broker);

    // ✅ meta
    if (setMeta) {
      const p = snapObj;
      const meta = {
        index: p.index != null ? String(p.index) : (p.indexBroker != null ? String(p.indexBroker) : ""),
        broker: p.broker_display != null ? String(p.broker_display) : String(p.broker || ""),
        version: p.version != null ? String(p.version) : "",
        totalsymbol: p.totalsymbol != null ? String(p.totalsymbol) : "",
        timecurent: p.timecurent != null ? String(p.timecurent) : "",
        status: p.status != null ? String(p.status) : "",
        typeaccount: p.typeaccount != null ? String(p.typeaccount) : "",
        port: p.port != null ? String(p.port) : "",
        last_reset: p.last_reset != null ? String(p.last_reset) : "",
        timecurrent: p.timecurrent != null ? String(p.timecurrent) : "",
        timeUpdated: p.timeUpdated != null ? String(p.timeUpdated) : "",
        receivedAt: String(it.recv),
      };
      pipeB.hmset(brokerMetaKey(it.broker), meta);
      if (META_TTL_SECONDS && META_TTL_SECONDS > 0) pipeB.expire(brokerMetaKey(it.broker), META_TTL_SECONDS);
    }

    updated++;
  }

  if (updated === 0) return { updated: 0, skipped, total, mode: "FAST_HMSET_GUARD_V2" };

  const resB = await pipeB.exec();
  for (const [e] of resB) if (e) console.error("[REDIS_PIPE_ERR]", e);

  return { updated, skipped, total, mode: "FAST_HMSET_GUARD_V2" };
}

// ===================== READ HELPERS =====================
async function getAllBrokers() {
  const redis = getRedis();

  // 1. Lấy danh sách broker
  const brokers = await redis.smembers("brokers");
  if (!brokers.length) return [];

  // 2. Lấy index của từng broker
  const pipe = redis.pipeline();
  for (const b of brokers) {
    pipe.hget(`broker_meta:${b}`, "index");
  }

  const res = await pipe.exec();

  // 3. Ghép broker + index
  const list = brokers.map((broker, i) => {
    const idx = Number(res[i]?.[1]);
    return {
      broker,
      index: Number.isFinite(idx) ? idx : 999999, // broker không có index -> đẩy xuống cuối
    };
  });

  // 4. Sort theo index tăng dần
  list.sort((a, b) => a.index - b.index);

  // 5. Trả về đúng format bạn cần
  return list.map(item => item.broker);
}

async function getAllBrokers_TRUE() {
  const redis = getRedis();

  // 1. Lấy danh sách broker
  const brokers = await redis.smembers("brokers");
  if (!brokers.length) return [];

  // 2. Lấy status + index của từng broker
  const pipe = redis.pipeline();
  for (const b of brokers) {
    pipe.hmget(`broker_meta:${b}`, "status", "index");
  }

  const res = await pipe.exec();

  // 3. Ghép broker + index (bỏ status = Disconnect)
  const list = [];

  for (let i = 0; i < brokers.length; i++) {
    const broker = brokers[i];
    const metaArr = res[i]?.[1] || [];

    const statusRaw = metaArr[0];
    const indexRaw = metaArr[1];

    // ✅ bỏ broker Disconnect
    const statusStr = String(statusRaw || "").trim().toLowerCase();
    if (statusStr === "disconnect") continue;

    const idx = Number(indexRaw);

    list.push({
      broker,
      index: Number.isFinite(idx) ? idx : 999999, // không có index -> đẩy xuống cuối
    });
  }

  // 4. Sort theo index tăng dần
  list.sort((a, b) => a.index - b.index);

  // 5. Trả về đúng format bạn cần
  return list.map(item => item.broker);
}


async function getSymbolsByBroker(broker) {
  const redis = getRedis();
  const b = normalizeBroker(broker);
  if (!b) return [];
  return await redis.smembers(symbolsKey(b));
}

async function getSnapshot(broker, symbol) {
  const redis = getRedis();
  const b = normalizeBroker(broker);
  const s = normalizeSymbol(symbol);
  if (!b || !s) return {};
  return await redis.hgetall(snapKey(b, s));
}

function tryParseJSON(v) {
  if (typeof v !== "string") return v;
  const t = v.trim();
  if (!t) return v;
  if (!(t.startsWith("[") || t.startsWith("{"))) return v;
  try {
    return JSON.parse(t);
  } catch {
    return v;
  }
}

async function getAllPricesByBroker(broker) {
  const redis = getRedis();
  const brokerKey = normalizeBroker(broker);
  if (!brokerKey) return [];

  const symbols = await redis.smembers(`symbols:${brokerKey}`);
  if (!symbols || !symbols.length) return [];

  const pipe = redis.pipeline();
  for (const sym of symbols) pipe.hgetall(`snap:${brokerKey}:${sym}`);
  const res = await pipe.exec();

  const out = [];
  for (let i = 0; i < symbols.length; i++) {
    const [err, data] = res[i] || [];
    if (err || !data || Object.keys(data).length === 0) continue;

    out.push({
      symbol: symbols[i],
      broker_: data.broker_ || brokerKey,
      broker: data.broker || broker,

      bid: Number(data.bid) || 0,
      ask: Number(data.ask) || 0,
      spread: Number(data.spread) || 0,

      bid_mdf: data.bid_mdf != null ? Number(data.bid_mdf) : null,
      ask_mdf: data.ask_mdf != null ? Number(data.ask_mdf) : null,
      spread_mdf: data.spread_mdf != null ? Number(data.spread_mdf) : null,
      digit: data.digit ?? "",

      // ✅ FIX: trả đủ time fields
      timedelay: data.timedelay ?? "",
      timecurrent: data.timecurrent ?? "",
      timeUpdated: data.timeUpdated ?? "",
      receivedAt: data.receivedAt ?? "",
      tud: data.tud ?? "",

      // ✅ FIX: timetrade parse đúng
      timetrade: tryParseJSON(data.timetrade),

      index: data.indexBroker ?? data.index ?? "",
      trade: data.trade ?? "",
      status: data.status ?? "",
    });
  }

  // sort by index if exists
  out.sort((a, b) => Number(a.index || 999999999) - Number(b.index || 999999999));
  return out;
}

async function getBrokerMeta(broker) {
  const redis = getRedis();
  const b = normalizeBroker(broker);
  if (!b) return {};
  return await redis.hgetall(brokerMetaKey(b));
}

async function getSymbolAcrossBrokers(symbol) {
  const redis = getRedis();
  const sym = normalizeSymbol(symbol);
  if (!sym) return {};

  const brokers = await redis.smembers(KEY_BROKERS_SET);
  if (!brokers.length) return {};

  const pipe = redis.pipeline();
  for (const b of brokers) pipe.hgetall(snapKey(b, sym));
  const res = await pipe.exec();

  const out = {};
  brokers.forEach((b, i) => {
    const [err, hash] = res[i] || [];
    if (!err && hash && Object.keys(hash).length) out[`${b}:${sym}`] = hash;
  });
  return out;
}

async function getBrokerResetting() {
  const redis = getRedis();
  const brokerList = await redis.smembers(KEY_BROKERS_SET);
  if (!brokerList || brokerList.length === 0) return [];

  const pipe = redis.pipeline();
  for (const b of brokerList) pipe.hgetall(`broker_meta:${b}`);
  const res = await pipe.exec();

  const brokers = [];
  for (let i = 0; i < brokerList.length; i++) {
    const broker_ = brokerList[i];
    const [err, meta] = res[i] || [];
    if (err) continue;

    const m = meta || {};
    const idx = (m.indexBroker != null ? m.indexBroker : m.index);

    brokers.push({
      broker_: broker_,
      broker: m.broker || broker_.toUpperCase(),
      status: m.status ?? "",
      index: idx ?? "",
      indexBroker: m.indexBroker ?? "",
      totalsymbol: m.totalsymbol ?? "",
      timeUpdated: m.timeUpdated ?? "",
      timecurent: m.timecurent ?? "",
      typeaccount: m.typeaccount ?? "",
      port: m.port ?? "",
    });
  }

  // giữ logic cũ: status !== "True" thì coi là resetting / not ready
  return brokers
    .filter(b => String(b.status) !== "True")
    .sort((a, b) => Number(a.index || a.indexBroker || 999999999) - Number(b.index || b.indexBroker || 999999999));
}

async function getMultipleSymbolDetails_RedisH(symbols = []) {
  const redis = getRedis(); // ✅ dùng redis client hiện tại của bạn (RedisH.getRedis() hoặc helper2.getRedis())
  if (!Array.isArray(symbols) || symbols.length === 0) return new Map();

  // normalize symbol list (UI/analysis thường dùng UPPER)
  const symbolsUpper = symbols
    .map(s => String(s || '').trim().toUpperCase())
    .filter(Boolean);

  if (symbolsUpper.length === 0) return new Map();

  // lấy toàn bộ brokers 1 lần
  const brokers = await redis.smembers("brokers");
  if (!brokers || brokers.length === 0) return new Map();
 
  // pipeline hgetall snap:<broker>:<symbol>
  const pipe = redis.pipeline();
  const idxMap = []; // map index -> { sym, broker_ }

  for (const sym of symbolsUpper) {
    for (const broker_ of brokers) {
      idxMap.push({ sym, broker_ });
      pipe.hgetall(`snap:${broker_}:${sym}`);
       console.log("BROKERS FOUND:", brokers);
    }
  }

  const res = await pipe.exec();

  // ✅ output Map: sym -> array snapshots
  const out = new Map();

  for (let i = 0; i < res.length; i++) {
    const [err, h] = res[i] || [];
    if (err || !h || Object.keys(h).length === 0) continue;

    const { sym, broker_ } = idxMap[i];

    const arr = out.get(sym) || [];
    console.log("SNAPSHOT FOUND:", out);
   arr.push({
  broker_: h.broker_ ?? broker_,
  broker: h.broker ?? "",
  index: h.index ?? "",
  symbol: h.symbol ?? sym,

  bid: h.bid ?? "",
  ask: h.ask ?? "",
  spread: h.spread ?? "",

  bid_mdf: h.bid_mdf ?? "",
  ask_mdf: h.ask_mdf ?? "",
  spread_mdf: h.spread_mdf ?? "",

  digit: h.digit ?? "",
  trade: h.trade ?? "",
  status: h.status ?? "",

  typeaccount: h.typeaccount ?? "",   // ✅ THÊM DÒNG NÀY

  timeUpdated: h.timeUpdated ?? "",
  receivedAt: h.receivedAt ?? "",
});

    out.set(sym, arr);
  }

  return out;
}
function normSymbol(sym) {
  return String(sym || "").trim().toUpperCase();
}
function numIndex(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n : 999999999;
}

function isTruthyStatus(v) {
  if (v === true) return true;
  if (v === 1) return true;
  const s = String(v ?? "").trim().toLowerCase();
  return s === "true" || s === "1" || s === "yes" || s === "on";
}

function safeParseTimetrade(tt) {
  // tt có thể: array | string json | null
  if (Array.isArray(tt)) return tt;

  if (typeof tt === "string") {
    const s = tt.trim();
    if (!s) return [];
    try {
      const parsed = JSON.parse(s);
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  }

  return [];
}

function hasActiveTradeWindow(timetrade) {
  const arr = safeParseTimetrade(timetrade);
  if (!arr.length) return false;
  // ✅ chỉ cần có 1 item status=true là hợp lệ
  return arr.some((it) => isTruthyStatus(it?.status));
}

async function getMultipleSymbolAcrossBrokersWithMetaFast(symbols, brokers, redis) {
  const SYMS = Array.isArray(symbols) ? symbols.map(normSymbol).filter(Boolean) : [];
  if (SYMS.length === 0) return new Map();

  const BRKS = Array.isArray(brokers) ? brokers.map(normalizeBrokerKey).filter(Boolean) : [];
  if (BRKS.length === 0) return new Map();

  const pipe = redis.pipeline();

  // ---- A) meta: 3 field / broker (1 lần)
  for (const b of BRKS) {
    pipe.hmget(`broker_meta:${b}`, "typeaccount", "status", "timecurent");
  }

  // ---- B) snaps: broker × symbol
  for (const sym of SYMS) {
    for (const b of BRKS) {
      pipe.hgetall(`snap:${b}:${sym}`);
    }
  }

  const res = await pipe.exec();

  // ---- parse meta
  const metaMap = new Map(); // broker_ -> {typeaccount, status, timecurent_a}
  for (let i = 0; i < BRKS.length; i++) {
    const b = BRKS[i];
    const [err, arr] = res[i] || [];
    const [typeaccount, status, timecurent] = (!err && Array.isArray(arr)) ? arr : [];
    metaMap.set(b, {
      typeaccount: typeaccount ?? "",
      status: status ?? "",
      timecurent_a: timecurent ?? "",
    });
  }

  // ---- init output
  const outMap = new Map();
  for (const sym of SYMS) outMap.set(sym, []);

  // ---- parse snaps
  const base = BRKS.length; // meta offset
  let k = 0;

  for (const sym of SYMS) {
    const arr = outMap.get(sym);

    for (const b of BRKS) {
      const idx = base + k;
      k++;

      const [err, h] = res[idx] || [];
      if (err || !h || Object.keys(h).length === 0) continue;

      const m = metaMap.get(b) || { typeaccount: "", status: "", timecurent_a: "" };

      // ✅ lấy index: ưu tiên từ snap
      const index = h.index ?? h.indexBroker ?? "";

      // ✅ filter: timedelay + trade + meta.status (giữ logic của bạn)
      // NOTE: hgetall trả về string => Number(...) để chắc chắn
      if (Number(h.timedelay) < -1800) continue;
      if (String(h.trade || "").toUpperCase() !== "TRUE") continue;
      if (String(m.status || "") !== "True") continue;

      // ✅ NEW: timetrade phải có ít nhất 1 item status=true
      // timetrade có thể nằm trong h.timetrade (hoặc bạn đổi key tại đây nếu khác)
      if (!hasActiveTradeWindow(h.timetrade)) continue;

      arr.push({
        ...h,
        broker_: b,
        typeaccount: m.typeaccount ?? h.typeaccount ?? "",
        Status: m.status ?? h.status ?? "",
        timecurent_broker: m.timecurent_a ?? h.timecurrent ?? "",
        index,
      });
    }

    // sort theo index
    arr.sort((a, b) => numIndex(a.index) - numIndex(b.index));
  }

  return outMap;
}



async function getBestBrokerBySymbol(symbol) {
  const redis = getRedis();
  const sym = normalizeSymbol(symbol);
  if (!sym) return null;
  const arr = await redis.zrange(bestzKey(sym), 0, 0);
  return arr && arr[0] ? arr[0] : null;
}

// Dùng trong redis.helper2.js hoặc file query import getRedis(), brokerMetaKey(), snapKey()
// Nếu bạn đặt trong redis.helper2.js thì có sẵn getRedis() và key builders.

async function getBestBrokerDataBySymbol(symbol, opts = {}) {
  const {
    requireTradeTrue = false,     // nếu muốn lọc trade === "TRUE"
    minTimedelay = null,          // ví dụ -1800
    requireBidAsk = true,         // bắt buộc có bid & ask
  } = opts;

  const redis = getRedis();
  const sym = String(symbol || "").toUpperCase().trim();
  if (!sym) return null;

  // 1) Lấy brokers
  const brokers = await redis.smembers("brokers");
  if (!brokers || brokers.length === 0) return null;

  // 2) Pipeline: hgetall snap + hmget meta(status,index,broker,typeaccount,...) (1 pipe là nhanh nhất)
  const pipe = redis.pipeline();
  for (const b of brokers) {
    pipe.hgetall(`snap:${b}:${sym}`);
    // hmget để lấy nhiều field meta 1 lần
    pipe.hmget(`broker_meta:${b}`, "status", "index", "broker", "typeaccount", "auto_trade");
  }
  const res = await pipe.exec();

  let bestTrue = null;   // best trong nhóm status True
  let bestAll = null;    // best trong tất cả

  function pickBetter(curBest, cand) {
    if (!curBest) return cand;
    // index nhỏ hơn thắng
    if (cand.index < curBest.index) return cand;
    return curBest;
  }

  for (let i = 0; i < brokers.length; i++) {
    const broker_ = brokers[i];

    const snap = res[i * 2]?.[1];
    const metaArr = res[i * 2 + 1]?.[1]; // [status,index,broker,typeaccount,auto_trade]

    if (!snap || Object.keys(snap).length === 0) continue;

    // require bid/ask
    if (requireBidAsk) {
      const bid = Number(snap.bid);
      const ask = Number(snap.ask);
      if (!Number.isFinite(bid) || !Number.isFinite(ask) || bid <= 0 || ask <= 0) continue;
    }

    // lọc trade
    if (requireTradeTrue) {
      if (String(snap.trade || "").toUpperCase() !== "TRUE") continue;
    } 


    // lọc timedelay
    if (minTimedelay !== null) {
      const td = Number(snap.timedelay);
      if (Number.isFinite(td) && td < minTimedelay) continue;
    }

    const status = String(metaArr?.[0] ?? snap.status ?? "");
    if(status !== "True"){
      continue;
    }
    const index = Number(metaArr?.[1] ?? snap.index ?? snap.indexBroker);
    const indexNum = Number.isFinite(index) ? index : 999999999;

    const brokerDisplay = String(metaArr?.[2] ?? snap.broker ?? broker_).toUpperCase();
    const typeaccount = String(metaArr?.[3] ?? snap.typeaccount ?? "");
    const auto_trade = String(metaArr?.[4] ?? snap.auto_trade ?? "");

    // build candidate chuẩn output UI
    const candidate = {
      broker_,
      brokerDisplay,
      index: indexNum,
      status,
      typeaccount,
      auto_trade,
      snap: {
        ...snap,
        broker_: broker_,
        broker: brokerDisplay,
        typeaccount,
        auto_trade,
        Status: status, // ✅ field UI bạn hay dùng
      },
    };

    // bestAll
    bestAll = pickBetter(bestAll, candidate);

    // bestTrue
    if (String(status) === "True") {
      bestTrue = pickBetter(bestTrue, candidate);
    }
  }

  const best = bestTrue || bestAll;
  if (!best) return null;

  // 3) lấy OHLC của broker best (key chart:ohlc:<broker_>:<SYMBOL>)
  let ohlc = [];
  try {
    const raw = await redis.get(`chart:ohlc:${best.broker_}:${sym}`);
    if (raw) ohlc = JSON.parse(raw);
  } catch {}

  // 4) return đúng format bạn cần
  return {
    title: `${sym} | ${best.broker_} | Bid&Ask`,
    broker_: best.broker_,
    brokerDisplay: best.broker_,
    ohlc,
    snap: best.snap,
    note: best.snap ? undefined : "NO_PRICE_SNAP",
  };
}



// ===================== ADMIN / CLEANUP =====================
async function deleteByPattern(pattern) {
  if (!pattern) return { deleted: 0 };

  const redis = getRedis();
  let cursor = "0";
  let totalDeleted = 0;

  do {
    const [next, keys] = await redis.scan(cursor, "MATCH", pattern, "COUNT", 1000);
    cursor = next;

    if (keys.length > 0) {
      const CHUNK = 1000;
      for (let i = 0; i < keys.length; i += CHUNK) {
        const part = keys.slice(i, i + CHUNK);
        await redis.del(...part);
        totalDeleted += part.length;
      }
    }
  } while (cursor !== "0");

  return { deleted: totalDeleted };
}

async function deleteBrokerCompletely(broker) {
  const redis = getRedis();
  const brokerKey = normalizeBroker(broker);
  if (!brokerKey) return { ok: false, reason: "NO_BROKER" };

  const symbols = await redis.smembers(`symbols:${brokerKey}`);
  const pipe = redis.pipeline();

  for (const sym of symbols) {
    pipe.del(`snap:${brokerKey}:${sym}`);
    pipe.del(`chart:ohlc:${brokerKey}:${sym}`);
    pipe.zrem(bestzKey(sym), brokerKey);
  }

  pipe.del(`symbols:${brokerKey}`);
  pipe.del(`broker_meta:${brokerKey}`);
  pipe.srem(KEY_BROKERS_SET, brokerKey);

  await pipe.exec();

  return { ok: true, broker: brokerKey, removedSymbols: symbols.length };
}

async function deleteBrokerAndRelatedIndex(broker_) {
  if (!broker_) return { ok: false, reason: "broker_required" };

  const redis = getRedis();
  const b = normalizeBroker(broker_);
  if (!b) return { ok: false, reason: "broker_invalid" };

  const meta = await redis.hgetall(`broker_meta:${b}`);
  const idx = String(meta?.index ?? meta?.indexBroker ?? "").trim();

  let targets = [b];

  if (idx) {
    const brokers = await redis.smembers(KEY_BROKERS_SET);
    if (brokers && brokers.length) {
      const pipe = redis.pipeline();
      for (const x of brokers) pipe.hget(`broker_meta:${x}`, "index");
      const res = await pipe.exec();

      const sameIndex = [];
      for (let i = 0; i < brokers.length; i++) {
        const x = brokers[i];
        const [err, v] = res[i] || [];
        if (err) continue;
        if (String(v ?? "").trim() === idx) sameIndex.push(x);
      }
      targets = Array.from(new Set([b, ...sameIndex]));
    }
  }

  const deletedBrokers = [];
  for (const t of targets) deletedBrokers.push(await deleteBrokerCompletely(t));

  return { ok: true, inputBroker: b, index: idx || "", targets, deletedBrokers };
}

async function setBrokerStatus(broker, status) {
  const redis = getRedis();
  const broker_ = normalizeBroker(broker);
  if (!broker_) throw new Error("broker is required");

  const statusStr = String(status);
  await redis.hset(`broker_meta:${broker_}`, "status", statusStr);

  const symbols = await redis.smembers(`symbols:${broker_}`);
  if (!symbols || symbols.length === 0) return { ok: true, broker: broker_, updated: 0, note: "No symbols found" };

  const pipe = redis.pipeline();
  for (const sym of symbols) pipe.hset(`snap:${broker_}:${sym}`, "status", statusStr);
  await pipe.exec();

  return { ok: true, broker: broker_, updated: symbols.length, status: statusStr };
}

async function deleteSymbol(broker, symbol) {
  const redis = getRedis();
  const b = normalizeBroker(broker);
  const s = normalizeSymbol(symbol);
  if (!b || !s) return { deleted: 0 };

  const pipe = redis.pipeline();
  pipe.del(snapKey(b, s));
  pipe.del(keyChartOHLC(b, s));
  pipe.srem(symbolsKey(b), s);
  pipe.zrem(bestzKey(s), b);
  const res = await pipe.exec();

  const deleted = !res[0]?.[0] ? Number(res[0]?.[1] || 0) : 0;
  return { deleted };
}

async function deleteBroker(broker) {
  return deleteBrokerCompletely(broker);
}

async function flushAll() {
  const redis = getRedis();
  return await redis.flushdb();
}

async function flushAllRedis() {
  const redis = getRedis();
  await redis.flushdb();
  console.log("[REDIS] All data flushed");
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
  getSymbolAcrossBrokers,
  getBrokerMeta,
  getBestBrokerBySymbol,
  getAllPricesByBroker,
  getBrokerResetting,  
  getBestBrokerDataBySymbol,
  getAllBrokers_TRUE,

  // cleanup
  deleteSymbol,
  deleteBroker,
  flushAllRedis,
  flushAll,
  deleteByPattern,
  deleteBrokerCompletely,
  deleteBrokerAndRelatedIndex,
  getMultipleSymbolDetails_RedisH,
  getMultipleSymbolAcrossBrokersWithMetaFast
};
