const RedisH = require('../Redis/redis.helper'); 

function keyChartOHLC(broker_, symbol) {
  return `chart:ohlc:${String(broker_ || '').trim()}:${String(symbol || '').toUpperCase().trim()}`;
}

function parseOHLCFromRaw(raw, r) {
  if (!raw) return [];

  // 1) helper2 đang lưu JSON string
  try {
    const v = JSON.parse(raw);
    if (Array.isArray(v)) return v;
  } catch {}

  // 2) fallback: nếu trước đây bạn packValue (gz:...) thì thử unpack
  try {
    const cfg = r.__cfg || { compress: true };
    const v = RedisH.unpackValue(raw, cfg);
    if (Array.isArray(v)) return v;
  } catch {}

  return [];
}

async function getOHLC(broker_, symbol) {
  const r = RedisH.getRedis();
  const raw = await r.get(keyChartOHLC(broker_, symbol));
  return parseOHLCFromRaw(raw, r);
}

// cache OHLC
const _ohlcCache = new Map();
async function getOHLC_cached(broker_, symbol, cacheMs = 300) {
  const k = keyChartOHLC(broker_, symbol);
  const now = Date.now();
  const hit = _ohlcCache.get(k);
  if (hit && (now - hit.t) < cacheMs) return hit.v;
  const v = await getOHLC(broker_, symbol);
  _ohlcCache.set(k, { t: now, v });
  return v;
}

module.exports = {
  getOHLC,
  getOHLC_cached,
};