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
  const redis = getRedis();
  if (!broker) return [];

  const brokerKey = broker.toLowerCase();
  const symbols = await redis.smembers(`symbols:${brokerKey}`);
  if (!symbols || !symbols.length) return [];

  const pipe = redis.pipeline();
  for (const sym of symbols) {
    pipe.hgetall(`snap:${brokerKey}:${sym}`);
  }

  const res = await pipe.exec();

  const out = [];

  for (let i = 0; i < symbols.length; i++) {
    const [err, data] = res[i] || [];
    if (err || !data || Object.keys(data).length === 0) continue;

    out.push({
      symbol: symbols[i],
      broker: data.broker || broker,
      bid: Number(data.bid) || 0,
      ask: Number(data.ask) || 0,
      spread: Number(data.spread) || 0,

      // ✅ QUAN TRỌNG – lấy đúng time
      time: data.timecurrent || data.timeUpdated || data.receivedAt || "",

      // giữ lại nếu cần
      bid_mdf: data.bid_mdf || null,
      ask_mdf: data.ask_mdf || null,
      index: data.index || "",
      trade: data.trade || "",
      status: data.status || "",
    });
  }

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

    // ✅ index dùng để sort (ưu tiên indexBroker)
    const idxRaw = snap.indexBroker ?? snap.index ?? snapHash.indexBroker ?? snapHash.index ?? "";
    const idxNum = Number(idxRaw);

    out.push({
      broker_,                           // key chuẩn lowercase
      Status: e2 ? "" : (status ?? ""),  // lấy từ broker_meta
      ...snap,                           // bid/ask/spread/...
      index: idxRaw,                     // giữ để UI dùng
      indexNum: Number.isFinite(idxNum) ? idxNum : Number.MAX_SAFE_INTEGER, // dùng sort
    });
  });

  // ✅ sort theo index tăng dần
  out.sort((a, b) => (a.indexNum ?? 999999999) - (b.indexNum ?? 999999999));

  // (tuỳ bạn) nếu không muốn trả indexNum ra ngoài:
  return out.map(({ indexNum, ...rest }) => rest);
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
  // 1. Lấy danh sách broker
  const brokers = await redis.smembers("brokers");
  if (!brokers || brokers.length === 0) return [];

  // 2. Lấy meta của từng broker
  const pipe = redis.pipeline();
  for (const broker_ of brokers) {
    pipe.hgetall(`broker_meta:${broker_}`);
  }

  const res = await pipe.exec();

  const result = [];

  brokers.forEach((broker_, i) => {
    const [err, meta] = res[i] || [];
    if (err || !meta || Object.keys(meta).length === 0) return;

    const indexVal = meta.index ?? meta.indexBroker ?? '';

    result.push({
      broker_: broker_,
      broker: meta.broker ?? '',
      index: indexVal,
      indexNum: Number(indexVal), // dùng để sort
      version: meta.version ?? '',
      totalsymbol: meta.totalsymbol ?? '',
      timecurent: meta.timecurent ?? '',
      status: meta.status ?? '',
      typeaccount: meta.typeaccount ?? '',
      port: meta.port ?? '',
      timeUpdated: meta.timeUpdated ?? '',
    });
  });

  // 3. Sort theo index tăng dần (ưu tiên số hợp lệ)
  result.sort((a, b) => {
    const aIdx = isNaN(a.indexNum) ? Number.MAX_SAFE_INTEGER : a.indexNum;
    const bIdx = isNaN(b.indexNum) ? Number.MAX_SAFE_INTEGER : b.indexNum;
    return aIdx - bIdx;
  });

  return result;
}



async function getBestSymbolByIndex(symbol, opts = {}) {
  const sym = String(symbol || "").toUpperCase().trim();
  if (!sym) return null;

  const { staleMs = 2000 } = opts; // bỏ stale nếu bạn không muốn lọc

  const brokers = await redis.smembers("brokers");
  if (!brokers.length) return null;

  const pipe = redis.pipeline();

  brokers.forEach((b) => {
    pipe.hgetall(`snap:${b}:${sym}`);
    pipe.hmget(`broker_meta:${b}`, "status", "index"); // gộp 1 lệnh cho nhẹ
  });

  const res = await pipe.exec();

  let best = null;
  const now = Date.now();

  for (let i = 0; i < brokers.length; i++) {
    const broker_ = brokers[i];

    const snap = res[i * 2]?.[1];
    const metaArr = res[i * 2 + 1]?.[1]; // [status, index]

    if (!snap || !snap.bid || !snap.ask) continue;

    const metaStatus = metaArr?.[0];
    const metaIndex = metaArr?.[1];

    // ✅ status: ưu tiên snap.status, fallback meta.status
    const status = String(snap.status ?? metaStatus ?? "");

    // ✅ index: ưu tiên snap.indexBroker/snap.index, fallback meta.index
    const idxRaw = snap.indexBroker ?? snap.index ?? metaIndex;
    const index = Number(idxRaw);
    const indexNum = Number.isFinite(index) ? index : Infinity;

    // ✅ stale guard (nếu receivedAt quá cũ thì bỏ qua)
    const recv = Number(snap.receivedAt || 0);
    if (staleMs > 0 && recv > 0 && (now - recv) > staleMs) continue;

    const candidate = {
      broker_,
      index: indexNum,
      status,
      ...parseSnapshot(snap),
      // đảm bảo có symbol
      symbol: sym,
    };

    if (!best) {
      best = candidate;
      continue;
    }

    // Ưu tiên status True
    const candTrue = candidate.status === "True";
    const bestTrue = best.status === "True";

    if (candTrue && !bestTrue) {
      best = candidate;
      continue;
    }

    // cùng status -> index nhỏ hơn thắng
    if (candTrue === bestTrue && candidate.index < best.index) {
      best = candidate;
    }
  }

  return best;
}

// async function getSymbolOfMinIndexBroker(symbol, opts = {}) {
//   const sym = String(symbol || "").toUpperCase().trim();
//   if (!sym) return null;

//   const { staleMs = 0 } = opts; // nếu muốn lọc dữ liệu cũ: vd 2000

//   const brokers = await redis.smembers("brokers");
//   if (!brokers.length) return null;

//   const pipe = redis.pipeline();

//   // 1) lấy snap + meta.index (fallback)
//   for (const b of brokers) {
//     pipe.hgetall(`snap:${b}:${sym}`);
//     pipe.hmget(`broker_meta:${b}`, "status", "index"); 
//   }

//   const res = await pipe.exec();
//   console.log("RES LENGTH: ", res);
//   let best = null;
//   const now = Date.now();

//   for (let i = 0; i < brokers.length; i++) {
//     const broker_ = brokers[i];

//     const snap = res[i * 2]?.[1];
//     const metaIndex = res[i * 2 + 1]?.[1];
//     if (!snap || !snap.bid || !snap.ask) continue;
//     if(snap.timedelay && Number(snap.timedelay) < -1800)  continue; // bỏ timedelay > 30p

//     // ✅ index ưu tiên từ snap (chuẩn nhất), fallback meta
//     const idxRaw = snap.indexBroker ?? snap.index ?? metaIndex;
//     const idx = Number(idxRaw);
//     const indexNum = Number.isFinite(idx) ? idx : Infinity;

//     // (optional) bỏ snap quá cũ
//     const recv = Number(snap.receivedAt || 0);
//     if (staleMs > 0 && recv > 0 && (now - recv) > staleMs) continue;

//     const candidate = {
//       broker_,
//       index: indexNum,
//       ...snap,
//     };

//     if (!best || candidate.index < best.index) best = candidate;
//   }

//   return best; // best.symbol chính là symbol của broker index nhỏ nhất
// }

async function getSymbolOfMinIndexBroker(symbol, opts = {}) {
  const sym = String(symbol || "").toUpperCase().trim();
  if (!sym) return null;

  const { staleMs = 0 } = opts;

  const brokers = await redis.smembers("brokers");
  if (!brokers || !brokers.length) return null;

  const pipe = redis.pipeline();

  for (const b of brokers) {
    pipe.hgetall(`snap:${b}:${sym}`);
    pipe.hmget(`broker_meta:${b}`, "status", "index");
  }

  const res = await pipe.exec();

  let best = null;
  const now = Date.now();

  for (let i = 0; i < brokers.length; i++) {
    const broker_ = brokers[i];

    const snap = res[i * 2]?.[1];
    const metaArr = res[i * 2 + 1]?.[1] || [];
    const metaStatus = metaArr[0];
    const metaIndex = metaArr[1];

    // phải có snap + bid/ask
    if (!snap || snap.bid == null || snap.ask == null) continue;

    // bỏ timedelay quá 30p (giữ đúng logic bạn)
    if (snap.timedelay && Number(snap.timedelay) < -1800) continue;

    // ✅ status: ưu tiên snap, fallback meta
    const statusRaw = snap.status ?? metaStatus;


    // ✅ chỉ loại khi Disconnect (không phân biệt hoa/thường + trim)
    const statusStr = String(statusRaw ?? "").trim().toLowerCase();
    console.log("BROKER STATUS: ", broker_, statusStr);
    if (statusStr === "disconnect") continue;

    // ✅ index ưu tiên snap → meta
    const idxRaw = snap.indexBroker ?? snap.index ?? metaIndex;
    const idxNum = Number(idxRaw);
    const indexNum = Number.isFinite(idxNum) ? idxNum : Infinity;

    // optional: bỏ snap quá cũ
    const recv = Number(snap.receivedAt || 0);
    if (staleMs > 0 && recv > 0 && now - recv > staleMs) continue;

    const candidate = {
      broker_,
      index: indexNum,
      status: statusRaw, // ✅ trả ra status
      ...snap,
    };

    if (!best || candidate.index < best.index) best = candidate;
  }

  return best; // broker index nhỏ nhất + status != Disconnect
}



// async function getSymbolOfMinIndexBroker(symbol, opts = {}) {
//   const sym = String(symbol || "").toUpperCase().trim();
//   if (!sym) return null;

//   const { staleMs = 0 } = opts;

//   const brokers = await redis.smembers("brokers");
//   if (!brokers.length) return null;

//   const pipe = redis.pipeline();

//   // 1) lấy snap + meta.index (fallback)
//   for (const b of brokers) {
//     pipe.hgetall(`snap:${b}:${sym}`);
//     pipe.hget(`broker_meta:${b}`, "index");
//   }

//   const res = await pipe.exec();

//   let best = null;
//   const now = Date.now();

//   for (let i = 0; i < brokers.length; i++) {
//     const broker_ = brokers[i];

//     const snap = res[i * 2]?.[1];
//     const metaIndex = res[i * 2 + 1]?.[1];
//     if (!snap || !snap.bid || !snap.ask) continue;

//     // ✅ NEW: chỉ lấy broker status === "True"
//     if (
//       snap.status !== "True" &&
//       snap.status !== true &&
//       snap.status !== 1
//     ) {
//       continue;
//     }

//     // bỏ timedelay > 30 phút
//     if (snap.timedelay && Number(snap.timedelay) < -1800) continue;

//     // index ưu tiên từ snap, fallback meta
//     const idxRaw = snap.indexBroker ?? snap.index ?? metaIndex;
//     const idx = Number(idxRaw);
//     const indexNum = Number.isFinite(idx) ? idx : Infinity;

//     // (optional) bỏ snap quá cũ
//     const recv = Number(snap.receivedAt || 0);
//     if (staleMs > 0 && recv > 0 && (now - recv) > staleMs) continue;

//     const candidate = {
//       broker_,
//       index: indexNum,
//       ...snap,
//     };

//     if (!best || candidate.index < best.index) {
//       best = candidate;
//     }
//   }

//   return best; // broker có index nhỏ nhất + status True
// }







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
  getBestSymbolByIndex,
  getSymbolOfMinIndexBroker,
};
