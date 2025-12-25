'use strict';

const WebSocket = require('ws');
const http = require('http');
require('dotenv').config();

const RedisH = require('../Redis/redis.helper');
const RedisH2 = require('../Redis/redis.helper2'); // <- file helper2 bạn đang dùng (đường dẫn sửa cho đúng)
const { log, colors } = require('../Helpers/Log');
const { formatString } = require('../Helpers/text.format');

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
  try {
    const cfg = r.__cfg || { compress: true };
    const v = RedisH.unpackValue(raw, cfg);
    if (Array.isArray(v)) return v;
  } catch {}
  try {
    const v = JSON.parse(raw);
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

function truthyTrade(x) {
  if (x === true) return true;
  const s = String(x || '').trim().toLowerCase();
  return s === 'true' || s === '1' || s === 'yes';
}

function numIndex(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n : 999999999;
}

async function getPriceSnap(broker_, symbol) {
  // helper2 schema: snap:<broker_>:<symbol> (HASH)
  const hash = await RedisH2.getSnapshot(broker_, symbol);
  return hash && Object.keys(hash).length ? hash : null;
}

async function getBestBrokerByMinIndexTradeTrue(symbol) {
  const brokers = await RedisH2.getAllBrokers();
  if (!brokers || brokers.length === 0) return null;

  const pipe = RedisH2.getRedis().pipeline();
  for (const b of brokers) pipe.hgetall(`snap:${b}:${symbol}`);
  const res = await pipe.exec();

  const candidates = [];
  for (let i = 0; i < brokers.length; i++) {
    const b = brokers[i];
    const [err, h] = res[i] || [];
    if (err || !h || Object.keys(h).length === 0) continue;

    // trade field may be 'TRUE'/'true'/boolean/etc
    if (!truthyTrade(h.trade)) continue;

    candidates.push({
      broker_: b,
      index: numIndex(h.index),
      snap: h,
    });
  }

  if (candidates.length === 0) return null;

  candidates.sort((a, b) => a.index - b.index);
  return candidates[0];
}

function buildChart({ chartId, title, broker_, brokerDisplay, ohlc, snap, note }) {
  const s = snap || {};
  return cleanObj({
    chartId,
    title,
    Broker: brokerDisplay || (broker_ ? broker_.toUpperCase() : ''),
    Broker_: broker_ || '',
    // from snap (price fields)
    symbol: s.symbol || '',
    digit: s.digit || s.Digit || '',
    spread: s.spread || s.Spread || '',
    bid: s.bid || s.Bid || '',
    ask: s.ask || s.Ask || '',
    bid_mdf: s.bid_mdf || '',
    ask_mdf: s.ask_mdf || '',
    spread_mdf: s.spread_mdf || '',
    trade: s.trade,
    Index: s.index || '',
    timeUpdated: s.timeUpdated || '',
    ohlc: Array.isArray(ohlc) ? ohlc : [],
    note,
  });
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

    const brokerRaw = String(query.broker || '').trim();
    const broker_ = formatString ? formatString(brokerRaw) : brokerRaw.toLowerCase();

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
        // Chart1
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

        // Chart2 (best broker)
        const best = await getBestBrokerByMinIndexTradeTrue(symbol);
        let c2, c3;

        if (!best) {
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
          const bestBroker_ = best.broker_;
          const [ohlc2] = await Promise.all([
            getOHLC_cached(bestBroker_, symbol, 300),
          ]);

          // snap best đã có sẵn trong best.snap
          c2 = buildChart({
            chartId: 'c2',
            title: `${symbol} | ${bestBroker_} | Bid&Ask`,
            broker_: bestBroker_,
            brokerDisplay: bestBroker_,
            ohlc: ohlc2,
            snap: best.snap,
          });

          // Chart3: OHLC = c2, price = mdf of c2, digit = digit of c2
          const s = best.snap || {};
          const snap3 = {
            ...s,
            bid: s.bid_mdf || s.bid,
            ask: s.ask_mdf || s.ask,
            spread: s.spread_mdf || s.spread,
            digit: s.digit || s.Digit || '',
          };

          c3 = buildChart({
            chartId: 'c3',
            title: `${symbol} | ${bestBroker_} | bid_mdf&ask_mdf`,
            broker_: bestBroker_,
            brokerDisplay: bestBroker_,
            ohlc: ohlc2,
            snap: snap3,
          });
        }

        const payload = { type: 'CHART_INIT_3', symbol, broker: broker_, charts: [c1, c2, c3] };
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
    }, 120);

    ws.on('close', () => clearInterval(timer));
    ws.on('error', () => clearInterval(timer));
  });

  server.listen(port, () => {
    log(colors.green, 'WS CHART', colors.reset, `ws://IP:${port}/BTCUSD?broker=B`);
  });

  return wss;
}

module.exports = setupWebSocketServer;
