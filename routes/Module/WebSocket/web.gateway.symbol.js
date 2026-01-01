'use strict';

const WebSocket = require('ws');
const http = require('http');
require('dotenv').config();
const { getTimeGMT7 } = require('../Helpers/time');
const { log, colors } = require('../Helpers/Log');
const { normSym } = require('../Helpers/text.format');

const RedisH2 = require('../Redis/redis.helper2');

// -------- cache ----------
const brokersCache = { t: 0, v: [] };
const symCache = new Map(); // sym -> {t,v}

function cacheGet(sym, ms) {
  const hit = symCache.get(sym);
  if (!hit) return null;
  if ((Date.now() - hit.t) > ms) return null;
  return hit.v;
}
function cacheSet(sym, v) {
  symCache.set(sym, { t: Date.now(), v });
}
setInterval(() => {
  const now = Date.now();
  for (const [k, it] of symCache.entries()) {
    if (!it || (now - it.t) > 2000) symCache.delete(k);
  }
}, 2000).unref?.();

// -------- sort helpers ----------
function numIndex(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n : 999999999;
}

function sortByIndex(arr) {
  if (!Array.isArray(arr)) return [];
  return arr.slice().sort((a, b) => {
    const ai = numIndex(a?.indexBroker ?? a?.index);
    const bi = numIndex(b?.indexBroker ?? b?.index);
    return ai - bi;
  });
}

// -------- redis helpers ----------
function snapKey(broker_, sym) {
  return `snap:${broker_}:${sym}`;
}

function brokerMetaKey(broker_) {
  return `broker_meta:${broker_}`;
}

function parseSnapshotHash(h, broker_) {
  if (!h || typeof h !== 'object' || Object.keys(h).length === 0) return null;
  return {
    broker_: h.broker_ ?? broker_ ?? '',
    broker: h.broker ?? '',
    indexBroker: h.indexBroker ?? h.index ?? '',
    symbol: h.symbol ?? '',
    symbol_raw: h.symbol_raw ?? '',
    bid: h.bid ?? '',
    ask: h.ask ?? '',
    spread: h.spread ?? '',
    bid_mdf: h.bid_mdf ?? '',
    ask_mdf: h.ask_mdf ?? '',
    spread_mdf: h.spread_mdf ?? '',
    digit: h.digit ?? '',
    trade: h.trade ?? '',
    timeUpdated: h.timeUpdated ?? '',
    receivedAt: h.receivedAt ?? '',
    timedelay: h.timedelay ?? '',
    longcandle: h.longcandle ?? '',
    longcandle_mdf: h.longcandle_mdf ?? '',
    broker_sync: h.broker_sync ?? '',
    last_reset: h.last_reset ?? '',
    timecurrent: h.timecurrent ?? '',
    typeaccount: h.typeaccount ?? '',
    auto_trade: h.auto_trade ?? '',
    timetrade: JSON.parse(h.timetrade ?? '[]'),
    port: h.port ?? '',
  };
}

/**
 * Lấy snaps cho 1 symbol + gắn Status từ broker_meta vào từng broker
 * Output: Array items, mỗi item có thêm field Status
 */
async function getSymbolAcrossBrokersFast(sym, brokers, redis) {
  if (!brokers || brokers.length === 0) return [];

  // 1) lấy snap all brokers
  const pipe1 = redis.pipeline();
  for (const b of brokers) pipe1.hgetall(snapKey(b, sym));
  const res1 = await pipe1.exec();

  // 2) lấy meta all brokers (status + auto_trade) - vẫn "nhẹ"
  const pipe2 = redis.pipeline();
  for (const b of brokers) {
    pipe2.hmget(brokerMetaKey(b), "status", "auto_trade");
  }
  const res2 = await pipe2.exec();
  
  const out = [];
  for (let i = 0; i < brokers.length; i++) {
    const broker_ = brokers[i];
    const [e1, h] = res1[i] || [];
    if (e1 || !h || Object.keys(h).length === 0) continue;
//  console.log(h);
    const snap = parseSnapshotHash(h, broker_);

    if (!snap) continue;

    // ✅ lọc timedelay + trade
    if (Number(snap.timedelay) < -1800) continue;
    if (String(snap.trade || "").toUpperCase() !== "TRUE") continue;

    const [e2, metaArr] = res2[i] || [];
    const status = !e2 && metaArr ? metaArr[0] : "";
    const auto_trade = !e2 && metaArr ? metaArr[1] : "";
  
    // ✅ field đúng UI
    snap.Status = status ?? "";
    snap.auto_trade = auto_trade ?? ""; // <-- thêm auto_trade

    out.push(snap);
  }

  return out;
}


// -------- main ----------
function setupWebSocketServer(port) {
  const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('WS WEB SYMBOL RUNNING\n');
  });

  const wss = new WebSocket.Server({
    server,
    maxPayload: 1000 * 1024 * 1024,
    perMessageDeflate: false,
    handshakeTimeout: 10000,
  });

  wss.on('error', (error) => {
    console.log(colors.red, 'WebSocket Server Error:', colors.reset, error?.message || error);
  });

  // ✅ Broadcast loop
  function startBroadcastLoop() {
    const INTERVAL = 60; // 120-200ms ok
    const redis = RedisH2.getRedis();

    async function broadcast() {
      const start = Date.now();
      try {
        // group clients by symbol
        const clientsBySymbol = new Map();
        wss.clients.forEach((ws) => {
          if (ws.readyState !== WebSocket.OPEN) return;
          if (!ws.symbol) return;

          const sym = String(ws.symbol).toUpperCase().trim();
          if (!sym) return;

          const arr = clientsBySymbol.get(sym);
          if (arr) arr.push(ws);
          else clientsBySymbol.set(sym, [ws]);
        });

        const symbols = Array.from(clientsBySymbol.keys());
        if (symbols.length === 0) return;

        // brokers cache 1s
        let brokers = brokersCache.v;
        if (!brokers || brokers.length === 0 || (Date.now() - brokersCache.t) > 1000) {
          brokers = await RedisH2.getAllBrokers(); // ['b','ab',...]
          brokersCache.t = Date.now();
          brokersCache.v = brokers;
        }

        const time = getTimeGMT7();

        for (const symRaw of symbols) {
          const sym = (normSym ? normSym(symRaw) : symRaw).toUpperCase();

          // short cache per symbol
          let dataArr = cacheGet(sym, 80);
          if (!dataArr) {
            dataArr = await getSymbolAcrossBrokersFast(sym, brokers, redis);
            cacheSet(sym, dataArr);
          }

          // ✅ sort output by indexBroker (asc)
          
          const sorted = sortByIndex(dataArr);

          // ✅ UI FORMAT: {time, data:[...]}
          const msg = JSON.stringify({ time, data: sorted });

          const clients = clientsBySymbol.get(symRaw) || [];
          for (const ws of clients) {
            if (ws.readyState !== WebSocket.OPEN) continue;

            // dedupe per client
            if (ws.__lastMsg === msg) continue;
            ws.__lastMsg = msg;
            ws.send(msg);
          }
        }
      } catch (e) {
        console.error('[WS_WEB_SYMBOL] broadcast error:', e?.message || e);
      } finally {
        const elapsed = Date.now() - start;
        setTimeout(broadcast, Math.max(0, INTERVAL - elapsed));
      }
    }

    broadcast();
  }

  startBroadcastLoop();

  wss.on('connection', (ws, req) => {
    // ✅ FIX: remove query, take only path
    const path = (req.url || '').split('?')[0] || '';
    const symbol = path.replace('/', '').toUpperCase().trim();
    ws.symbol = symbol;

    log(colors.green, 'WS WEB SYMBOL', colors.reset, `Client connected symbol=${symbol}`);

    ws.on('error', (err) => console.error('[WS_WEB_SYMBOL] ws error:', err?.message || err));
  });

  server.listen(port, () => {
    log(colors.green, 'WS WEB SYMBOL', colors.reset, `ws://${process.env.DOMAIN || 'IP'}:${port}/BTCUSD`);
  });

  return wss;
}

module.exports = setupWebSocketServer;
