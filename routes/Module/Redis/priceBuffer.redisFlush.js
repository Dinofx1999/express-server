"use strict";

// Bạn đang dùng redis.helper2.js (Redis 3.0 compatible)
const Redis2 = require("./redis.helper2");

// ===================== BUFFERS =====================
/**
 * priceBuffer key: `${broker_}:${SYMBOL}`  (broker_ lowercase)
 * value: priceInfo
 */
const priceBuffer = new Map();

/**
 * priceBufferBySymbol: SYMBOL -> Map(broker_ -> priceInfo)
 * để query "1 symbol tất cả broker"
 */
const priceBufferBySymbol = new Map();

/**
 * metaBuffer: broker_ -> meta object
 * lưu các field bạn yêu cầu: index,broker,version,totalsymbol,timecurent,status,timeUpdated,port,typeaccount
 */
const metaBuffer = new Map();

/**
 * active brokers (broker_ lowercase)
 */
const activeBrokers = new Set();

// ===================== NORMALIZE HELPERS =====================
function normalizeBrokerUpper(b) {
  return String(b || "UNKNOWN").trim().toUpperCase();
}
function normalizeBrokerKeyLower(broker_, brokerUpperFallback) {
  // Ưu tiên broker_ từ MT4 (đã lowercase), nếu thiếu thì lower(broker)
  const v = (broker_ || brokerUpperFallback || "unknown").toString().trim();
  return v.toLowerCase();
}
function normalizeSymbol(sym) {
  return String(sym || "").trim().toUpperCase();
}

// ===================== PARSE HELPERS =====================
function parseNum(val, fallback = 0) {
  const n = parseFloat(val);
  return Number.isFinite(n) ? n : fallback;
}
function parseIntSafe(val, fallback = 0) {
  const n = parseInt(val, 10);
  return Number.isFinite(n) ? n : fallback;
}

// ===================== 1) UPDATE BUFFER FROM MT4 =====================
function updatePriceBufferFromMT4(rawData) {
  if (!rawData || typeof rawData !== "object") {
    console.warn("[PRICE_SNAP] Invalid rawData received");
    return;
  }

  try {
    const brokerUpper = normalizeBrokerUpper(rawData.broker);
    const brokerKey = normalizeBrokerKeyLower(rawData.broker_, brokerUpper); // key chuẩn hoá để lưu redis
    const indexBroker = rawData.index ?? rawData.indexBroker ?? 0;
    const port = rawData.port ?? "unknown";

    // rawData có field timecurent (đúng theo MT4 gửi)
    const timecurent = rawData.timecurent || null;

    // bạn muốn dùng field timeUpdated (string) từ MT4
    const timeUpdated = rawData.timeUpdated || null;

    const meta = {
      index: String(indexBroker ?? ""),
      broker: brokerUpper, // hiển thị broker dạng upper cho dễ đọc
      version: String(rawData.version ?? ""),
      totalsymbol: String(rawData.totalsymbol ?? ""),
      timecurent: String(timecurent ?? ""),
      status: String(rawData.status ?? ""),
      timeUpdated: String(timeUpdated ?? ""),
      port: String(port ?? ""),
      typeaccount: String(rawData.typeaccount ?? ""),
      // thêm vài field hữu ích (không bắt buộc)
      auto_trade: String(rawData.auto_trade ?? ""),
      batch: String(rawData.batch ?? ""),
    };

    // active broker theo key lowercase
    activeBrokers.add(brokerKey);

    // buffer meta theo brokerKey
    metaBuffer.set(brokerKey, meta);

    if (!Array.isArray(rawData.Symbols) || rawData.Symbols.length === 0) {
      // console.log(`[PRICE_SNAP] No symbols from ${brokerUpper} (port ${port})`);
      return;
    }

    const receivedAt = Date.now(); // thời gian server nhận batch này

    let updatedCount = 0;

    for (const symData of rawData.Symbols) {
      const symbol = normalizeSymbol(symData.symbol);
      const symbol_raw = normalizeSymbol(symData.symbol_raw) || symbol;
      if (!symbol) continue;

      const key = `${brokerKey}:${symbol}`;

      const priceInfo = {
        // Giá
        bid: parseNum(symData.bid),
        ask: parseNum(symData.ask),
        bid_mdf: symData.bid_mdf != null ? parseNum(symData.bid_mdf) : null,
        ask_mdf: symData.ask_mdf != null ? parseNum(symData.ask_mdf) : null,

        // Spread & Candle
        spread: parseIntSafe(symData.spread),
        spread_mdf: parseIntSafe(symData.spread_mdf),
        longcandle: parseIntSafe(symData.longcandle),
        longcandle_mdf: parseIntSafe(symData.longcandle_mdf),

        // Digit & trạng thái
        digit: parseIntSafe(symData.digit, 5),
        trade: String(symData.trade ?? "false"),
        timedelay: parseIntSafe(symData.timedelay),
        broker_sync: symData.broker_sync === "NULL" ? null : (symData.broker_sync || null),
        last_reset: symData.last_reset || null,

        // Thời gian
        timecurrent: symData.timecurrent || null,
        timetrade: Array.isArray(symData.timetrade) ? symData.timetrade : [],

        // Metadata
        broker: brokerUpper,       // display
        broker_: brokerKey,        // key for redis
        indexBroker: String(indexBroker ?? ""),
        symbol,
        symbol_raw,
        port: String(port ?? ""),
        timeUpdated: String(timeUpdated ?? ""), // từ rawData
        receivedAt, // dùng cho atomic upsert
      };

      // main buffer
      priceBuffer.set(key, priceInfo);

      // by symbol buffer
      if (!priceBufferBySymbol.has(symbol)) priceBufferBySymbol.set(symbol, new Map());
      priceBufferBySymbol.get(symbol).set(brokerKey, priceInfo);

      updatedCount++;
    }

    // console.log(`[PRICE_SNAP] Updated ${updatedCount}/${rawData.Symbols.length} from ${brokerUpper} (${brokerKey})`);

  } catch (err) {
    console.error("[PRICE_SNAP] Error updating price buffer:", err.message);
    console.error(err.stack);
  }
}

// ===================== 2) EXPORT META FUNCTION (bạn yêu cầu) =====================
/**
 * Trả về meta của 1 broker (theo brokerKey lowercase, ví dụ "ab", "b")
 * hoặc nếu không truyền brokerKey thì trả về meta của tất cả brokers.
 */
function getBrokerMetaView(brokerKey) {
  if (brokerKey) {
    const key = String(brokerKey).trim().toLowerCase();
    return metaBuffer.get(key) || null;
  }

  const out = [];
  for (const [key, meta] of metaBuffer.entries()) {
    out.push({ broker_: key, ...meta });
  }
  return out;
}

// ===================== 3) FLUSH TO REDIS VIA INTERVAL =====================
// chuyển priceBuffer -> Map theo format "broker_:SYMBOL" đúng schema redis.helper2.js
function buildMapForRedisFlush() {
  const m = new Map();
  for (const [k, v] of priceBuffer.entries()) {
    // k đang là "broker_:SYMBOL"
    m.set(k, v);
  }
  return m;
}

async function flushBuffersToRedis() {
  try {
    if (priceBuffer.size === 0 && metaBuffer.size === 0) return;

    // 3.1 flush snapshots (atomic)
    if (priceBuffer.size > 0) {
      const mapToFlush = buildMapForRedisFlush();
      const rs = await Redis2.upsertSnapshotsAtomic(mapToFlush, { setMeta: false });

      // flush xong thì clear để tránh ghi lại liên tục
      priceBuffer.clear();

      // log nhẹ
      // console.log("[REDIS_FLUSH] snapshots", rs);
    }

    // 3.2 flush meta riêng (nếu bạn muốn lưu meta dạng riêng ngoài broker_meta của helper)
    // Hiện tại helper đã set broker_meta:<broker_> rồi, nên metaBuffer có thể clear luôn:
    if (metaBuffer.size > 0) metaBuffer.clear();

  } catch (err) {
    console.error("[REDIS_FLUSH] error:", err.message);
  }
}

let _flushTimer = null;

/**
 * Start interval flush.
 * @param {number} ms interval milliseconds (vd 300, 500, 1000)
 */
function startRedisFlushInterval(ms = 500) {
  if (_flushTimer) clearInterval(_flushTimer);

  _flushTimer = setInterval(() => {
    // chạy async không block tick
    flushBuffersToRedis();
  }, ms);

  console.log("[REDIS_FLUSH] interval started:", ms, "ms");
}

function stopRedisFlushInterval() {
  if (_flushTimer) clearInterval(_flushTimer);
  _flushTimer = null;
}

// ===================== EXPORTS =====================
module.exports = {
  // buffers & updater
  updatePriceBufferFromMT4,

  // meta export
  getBrokerMetaView,

  // flush interval
  startRedisFlushInterval,
  stopRedisFlushInterval,

  // (optional) expose for debug
  _buffers: {
    priceBuffer,
    priceBufferBySymbol,
    metaBuffer,
    activeBrokers,
  },
};
