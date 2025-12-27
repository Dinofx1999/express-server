'use strict';

const WebSocket = require('ws');
const http = require('http');
require('dotenv').config();

const RedisH = require('../Redis/redis.helper');   // dùng cho unpack (nếu có)
const RedisH2 = require('../Redis/redis.helper2'); // helper2 OPT v2 (Option 1)
const { log, colors } = require('../Helpers/Log');
const { formatString } = require('../Helpers/text.format');

// RedisH dùng chung client cũ (nếu bạn còn pack/unpack)
RedisH.initRedis({ host: '127.0.0.1', port: 6379, db: 0, compress: true });

// ---------- helpers ----------
function parseQuery(url = '') {
  const out = {};
  const i = url.indexOf('?');
  if (i === -1) return out;
  const qs = url.slice(i + 1);
  for (const pair of qs.split('&')) {
    if (!pair) continue;
    const [k, v] = pair.split('=');
    if (!k) continue;
    out[String(k)] = decodeURIComponent(v || '');
  }
  return out;
}

function getSymbolFromPath(url = '') {
  const path = url.split('?')[0] || '';
  const sym = path.replace('/', '').trim();
  return sym ? sym.toUpperCase() : '';
}

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
setInterval(() => {
  const now = Date.now();
  for (const [k, v] of _ohlcCache.entries()) {
    if (!v || (now - v.t) > 10_000) _ohlcCache.delete(k);
  }
}, 10_000).unref?.();

function cleanObj(obj) {
  const out = {};
  for (const [k, v] of Object.entries(obj || {})) {
    if (v === undefined) continue;
    out[k] = v;
  }
  return out;
}

// MT4 trade field: "TRUE" / "true" / true / 1...
function truthyTrade(x) {
  if (x === true) return true;
  const s = String(x || '').trim().toLowerCase();
  return s === 'true' || s === '1' || s === 'yes';
}

function buildChart({ chartId, title, broker_, brokerDisplay, ohlc, snap, note }) {
  const s = snap || {};
  // ưu tiên indexBroker (đúng data bạn show)
  const idx = s.indexBroker ?? s.index ?? '';

  return cleanObj({
    chartId,
    title,
    Broker: brokerDisplay || (broker_ ? broker_.toUpperCase() : ''),
    Broker_: broker_ || '',

    Index: idx,
    Status: s.status ?? '',
    Auto_Trade: s.auto_trade ?? '',
    Typeaccount: s.typeaccount ?? '',
    timecurent: s.timecurent ?? '',
    timeUpdated: s.timeUpdated ?? '',

    symbol: s.symbol || '',
    digit: s.digit || '',
    spread: s.spread || '',
    bid: s.bid || '',
    ask: s.ask || '',

    bid_mdf: s.bid_mdf || '',
    ask_mdf: s.ask_mdf || '',
    spread_mdf: s.spread_mdf || '',

    trade: s.trade,

    ohlc: Array.isArray(ohlc) ? ohlc : [],
    note,
  });
}

async function getPriceSnap(broker_, symbol) {
  const hash = await RedisH2.getSnapshot(broker_, symbol);
  return hash && Object.keys(hash).length ? hash : null;
}

// ✅ Option 1: O(1) best broker from bestz:<SYMBOL>
async function getBestBrokerFast(symbol) {
  // returns broker_ lowercase or null
  return await RedisH2.getBestBrokerBySymbol(symbol);
}

// ---------- main ----------
function setupWebSocketServer(port) {
  const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('WS CHART RUNNING\n');
  });

  const wss = new WebSocket.Server({
    server,
    maxPayload: 1000 * 1024 * 1024,
    perMessageDeflate: false,
    handshakeTimeout: 10000,
  });

  wss.on('connection', (ws, req) => {
    const symbol = getSymbolFromPath(req.url);
    const query = parseQuery(req.url);

    const brokerRaw = String(query.broker || '').trim(); // user send B / AB / ...
    const broker_ = formatString ? formatString(brokerRaw) : brokerRaw.toLowerCase(); // => b / ab

    if (!symbol || !broker_) {
      ws.send(JSON.stringify({ type: 'ERROR', message: 'Use: ws://IP:PORT/BTCUSD?broker=B' }));
      ws.close();
      return;
    }

    log(colors.green, 'WS CHART', colors.reset, `OPEN ${symbol} broker=${broker_}`);

    let inFlight = false;
    let lastSent = '';

    const timer = setInterval(async () => {
      if (ws.readyState !== WebSocket.OPEN) return;
      if (inFlight) return;
      inFlight = true;

      try {
        // ========= Chart 1 (broker request) =========
        const [ohlc1, snap1] = await Promise.all([
          getOHLC_cached(broker_, symbol, 300),
          getPriceSnap(broker_, symbol),
        ]);

        const c1 = buildChart({
          chartId: 'c1',
          title: `${symbol} | ${broker_} | Bid&Ask`,
          broker_,
          brokerDisplay: broker_,
          ohlc: ohlc1,
          snap: snap1,
          note: snap1 ? undefined : 'NO_PRICE_SNAP',
        });

        // ========= Chart 2 (best broker min-index trade TRUE) =========
        const bestBroker_ = await getBestBrokerFast(symbol);

        let c2, c3;

        if (!bestBroker_) {
          c2 = buildChart({
            chartId: 'c2',
            title: `${symbol} | MIN-INDEX | Bid&Ask`,
            broker_: '',
            brokerDisplay: 'MIN-INDEX',
            ohlc: [],
            snap: null,
            note: 'NO_MIN_TRADE_TRUE',
          });

          c3 = buildChart({
            chartId: 'c3',
            title: `${symbol} | MIN-INDEX | bid_mdf&ask_mdf`,
            broker_: '',
            brokerDisplay: 'MIN-INDEX',
            ohlc: [],
            snap: null,
            note: 'NO_MIN_TRADE_TRUE',
          });
        } else {
          // load best broker data
          const [ohlc2, snap2] = await Promise.all([
            getOHLC_cached(bestBroker_, symbol, 300),
            getPriceSnap(bestBroker_, symbol),
          ]);

          // Nếu bestz có nhưng snap2 chưa có => fallback hiển thị NO_SNAP
          c2 = buildChart({
            chartId: 'c2',
            title: `${symbol} | ${bestBroker_} | Bid&Ask`,
            broker_: bestBroker_,
            brokerDisplay: bestBroker_,
            ohlc: ohlc2,
            snap: snap2,
            note: snap2 ? undefined : 'NO_PRICE_SNAP_BEST',
          });

          // ========= Chart 3: OHLC of c2, price = mdf, digit = digit of c2 =========
          const s2 = snap2 || {};

          const snap3 = snap2
            ? {
                ...s2,
                // giá mdf
                bid: s2.bid_mdf ?? s2.bid,
                ask: s2.ask_mdf ?? s2.ask,
                spread: s2.spread_mdf ?? s2.spread,
                // digit vẫn là digit của chart2
                digit: s2.digit ?? '',
              }
            : null;

          c3 = buildChart({
            chartId: 'c3',
            title: `${symbol} | ${bestBroker_} | bid_mdf&ask_mdf`,
            broker_: bestBroker_,
            brokerDisplay: bestBroker_,
            ohlc: ohlc2,
            snap: snap3,
            note: snap3 ? undefined : 'NO_PRICE_SNAP_BEST',
          });
        }

        // ========= Final =========
        const payload = {
          type: 'CHART_INIT_3',
          symbol,
          broker: broker_,
          charts: [c1, c2, c3],
        };

        const s = JSON.stringify(payload);
        if (s !== lastSent) {
          lastSent = s;
          ws.send(s);
        }
      } catch (e) {
        console.error('[WS_CHART] loop error:', e?.message || e);
      } finally {
        inFlight = false;
      }
    }, 60);

    ws.on('close', () => {
      clearInterval(timer);
      log(colors.red, 'WS CHART', colors.reset, `CLOSE ${symbol} broker=${broker_}`);
    });

    ws.on('error', () => {
      clearInterval(timer);
    });
  });

  server.listen(port, () => {
    log(colors.green, 'WS CHART', colors.reset, `ws://IP:${port}/BTCUSD?broker=B`);
  });

  return wss;
}

module.exports = setupWebSocketServer;
