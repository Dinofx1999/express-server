"use strict";

const Redis = require("ioredis");

const redis = new Redis({
  host: "127.0.0.1",
  port: 6379,
  db: 0,
});

// ========== HELPERS ==========
function normBroker(b) {
  return String(b || "").trim().toLowerCase();
}
function normSymbol(s) {
  return String(s || "").trim().toUpperCase();
}

// =====================================================
// 1) LẤY DANH SÁCH BROKER
// =====================================================
async function getAllBrokers() {
  return await redis.smembers("brokers");
}

// =====================================================
// 2) LẤY META 1 BROKER
// =====================================================
async function getBrokerMeta(broker) {
  const broker_ = normBroker(broker);
  if (!broker_) return null;

  const meta = await redis.hgetall(`broker_meta:${broker_}`);
  return Object.keys(meta).length ? meta : null;
}

// =====================================================
// 3) LẤY SYMBOL CỦA 1 BROKER
// =====================================================
async function getSymbolsByBroker(broker) {
  const broker_ = normBroker(broker);
  if (!broker_) return [];

  return await redis.smembers(`symbols:${broker_}`);
}

// =====================================================
// 4) LẤY SNAPSHOT 1 SYMBOL CỦA 1 BROKER
// =====================================================
async function getPrice(broker, symbol) {
  const broker_ = normBroker(broker);
  const sym = normSymbol(symbol);
  if (!broker_ || !sym) return null;

  const data = await redis.hgetall(`snap:${broker_}:${sym}`);
  return Object.keys(data).length ? data : null;
}

// =====================================================
// 5) LẤY TOÀN BỘ SYMBOL CỦA 1 BROKER
// =====================================================
async function getAllPricesByBroker(broker) {
  const broker_ = String(broker || "").trim().toLowerCase();
  if (!broker_) return [];

  // 1) Lấy danh sách symbol của broker
  const symbols = await redis.smembers(`symbols:${broker_}`);
  if (!symbols || symbols.length === 0) return [];

  // 2) Pipeline lấy snapshot từng symbol
  const pipe = redis.pipeline();
  symbols.forEach(sym => {
    pipe.hgetall(`snap:${broker_}:${sym}`);
  });

  const res = await pipe.exec();

  // 3) Build mảng kết quả
  const out = [];
  symbols.forEach((symbol, i) => {
    const [err, data] = res[i] || [];
    if (err || !data || Object.keys(data).length === 0) return;

    out.push({
      broker_: broker_,
      symbol,
      ...parseSnapshot(data)   // parse string → number/array
    });
  });

  return out;
}


// =====================================================
// 6) LẤY 1 SYMBOL TRÊN TẤT CẢ BROKER
// =====================================================
async function getSymbolAcrossBrokers(symbol) {
  const sym = normSymbol(symbol);
  if (!sym) return [];

  const brokers = await redis.smembers("brokers"); // broker_ lowercase
  if (!brokers.length) return [];

  // 1) lấy snap cho từng broker
  const pipe1 = redis.pipeline();
  brokers.forEach(b => pipe1.hgetall(`snap:${b}:${sym}`));
  const res1 = await pipe1.exec();

  // 2) lấy status meta cho từng broker (nhẹ)
  const pipe2 = redis.pipeline();
  brokers.forEach(b => pipe2.hget(`broker_meta:${b}`, "status"));
  const res2 = await pipe2.exec();

  const out = [];
  brokers.forEach((broker_, i) => {
    const [e1, snapHash] = res1[i] || [];
    if (e1 || !snapHash || Object.keys(snapHash).length === 0) return;

    const snap = parseSnapshot(snapHash);
    if (!snap) return;

    const [e2, status] = res2[i] || [];
    out.push({
      broker_,
                       // key chuẩn lowercase
      Status: e2 ? "" : (status ?? ""), // lấy từ broker_meta
      ...snap                  // bid/ask/spread/...
    });
  });

  return out;
}


// =====================================================
// 7) LẤY TẤT CẢ SNAPSHOT (CẨN THẬN – NẶNG)
// =====================================================

function parseSnapshot(hash) {
  if (!hash || typeof hash !== "object") return null;
  if (Object.keys(hash).length === 0) return null;

  const toNum = (v) => {
    if (v === null || v === undefined) return null;
    const n = Number(v);
    return Number.isFinite(n) ? n : v;
  };

  const out = { ...hash };
  // parse number fields
  [
    "bid","ask","bid_mdf","ask_mdf",
    "spread","spread_mdf",
    "longcandle","longcandle_mdf",
    "digit","timedelay","receivedAt"
  ].forEach(f => {
    if (out[f] !== undefined) out[f] = toNum(out[f]);
  });

  // normalize null
  if (out.broker_sync === "null" || out.broker_sync === "NULL") {
    out.broker_sync = null;
  }

  // parse timetrade JSON
  if (typeof out.timetrade === "string") {
    try { out.timetrade = JSON.parse(out.timetrade); }
    catch { /* giữ string nếu lỗi */ }
  }
  return out;
}
async function getAllUniqueSymbols() {
  // 1) Lấy danh sách broker_
  const brokers = await redis.smembers("brokers");
  if (!brokers || brokers.length === 0) return [];

  // 2) Pipeline lấy symbols của từng broker
  const pipe = redis.pipeline();
  brokers.forEach(broker_ => {
    pipe.smembers(`symbols:${broker_}`);
  });

  const res = await pipe.exec();

  // 3) Gom + loại trùng
  const set = new Set();

  res.forEach(([err, symbols]) => {
    if (err || !Array.isArray(symbols)) return;
    symbols.forEach(sym => set.add(sym));
  });

  // 4) Trả về mảng
  return Array.from(set);
}

async function getAllBrokerMetaArray() {
  // 1. Lấy danh sách broker_ (lowercase)
  const brokers = await redis.smembers("brokers");
  if (!brokers.length) return [];

  // 2. Pipeline lấy broker_meta:<broker_>
  const pipe = redis.pipeline();
  brokers.forEach(broker_ => {
    pipe.hgetall(`broker_meta:${broker_}`);
  });

  const res = await pipe.exec();

  // 3. Build mảng kết quả
  const out = [];
  brokers.forEach((broker_, i) => {
    const [err, meta] = res[i] || [];
    if (err || !meta || Object.keys(meta).length === 0) return;
    out.push({
      index: meta.index ?? meta.indexBroker ?? "",
      broker: meta.broker ?? "",        
      broker_: meta.broker_ ?? "",        // tên hiển thị (AB, B…)
      version: meta.version ?? "",
      totalsymbol: meta.totalsymbol ?? "",
      timecurent: meta.timecurent ?? "",
      status: meta.status ?? "",
      timeUpdated: meta.timeUpdated ?? "",
      port: meta.port ?? "",
      typeaccount: meta.typeaccount ?? ""
    });
  });

  return out;
}


async function getBestSymbolByIndex(symbol) {
  const sym = String(symbol || "").toUpperCase();
  if (!sym) return null;

  const brokers = await redis.smembers("brokers");
  if (!brokers.length) return null;

  const pipe = redis.pipeline();

  // Lấy snapshot + status
  brokers.forEach(b => {
    pipe.hgetall(`snap:${b}:${sym}`);
    pipe.hget(`broker_meta:${b}`, "status");
    pipe.hget(`broker_meta:${b}`, "index");
  });

  const res = await pipe.exec();

  let best = null;

  for (let i = 0; i < brokers.length; i++) {
    const snap = res[i * 3]?.[1];
    const status = res[i * 3 + 1]?.[1];
    const index = Number(res[i * 3 + 2]?.[1]);

    if (!snap || !snap.bid || !snap.ask) continue;

    const candidate = {
      broker_: brokers[i],
      index: isNaN(index) ? Infinity : index,
      status: status || "",
      ...parseSnapshot(snap)
    };

    // Ưu tiên broker status = TRUE
    if (!best) {
      best = candidate;
      continue;
    }

    if (candidate.status === "True" && best.status !== "True") {
      best = candidate;
      continue;
    }

    if (candidate.status === best.status && candidate.index < best.index) {
      best = candidate;
    }
  }

  return best;
}



async function getAllSnapshots() {
  const brokers = await getAllBrokers();
  const out = {};

  for (const b of brokers) {
    out[b] = await getAllPricesByBroker(b);
  }

  return out;
}

// =====================================================
// EXPORT
// =====================================================
module.exports = {
  getAllBrokers,
  getBrokerMeta,
  getSymbolsByBroker,
  getPrice,
  getAllPricesByBroker,
  getSymbolAcrossBrokers,
  getAllSnapshots,
  getAllBrokerMetaArray,
  getAllUniqueSymbols,
  getBestSymbolByIndex
};
