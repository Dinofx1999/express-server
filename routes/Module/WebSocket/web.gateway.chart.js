'use strict';

const WebSocket = require('ws');
const http = require('http');
require('dotenv').config();

const RedisH = require('../Redis/redis.helper');
const { log, colors } = require('../Helpers/Log');
const { formatString } = require('../Helpers/text.format');

// ✅ Init Redis: PHẢI compress=true nếu data trong redis là "gz:...."
RedisH.initRedis({
  host: '127.0.0.1',
  port: 6379,
  db: 0,
  compress: true,
});

// ==============================
// Helpers
// ==============================
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

// Key OHLC đúng theo bạn đang lưu: chart:ohlc:<broker_>:<SYMBOL>
function keyChartOHLC(broker_, symbol) {
  return `chart:ohlc:${String(broker_ || '').trim()}:${String(symbol || '').toUpperCase().trim()}`;
}

// ✅ đọc OHLC: phải unpackValue (KHÔNG JSON.parse trực tiếp)
async function getOHLC(broker_, symbol) {
  const r = RedisH.getRedis();
  const cfg = r.__cfg;
  const raw = await r.get(keyChartOHLC(broker_, symbol));
  const val = RedisH.unpackValue(raw, cfg);
  return Array.isArray(val) ? val : [];
}

// Merge meta + symbolData (để output giống format bạn yêu cầu)
function buildItem(meta, symData, fallbackBroker_) {
  const m = meta || {};
  const d = symData && typeof symData === 'object' ? symData : {};

  return Object.assign(
    {
      Broker: m.broker || (m.broker_ ? String(m.broker_).toUpperCase() : ''),
      Broker_: m.broker_ || fallbackBroker_ || '',
      Status: m.status || '',
      Index: m.index || '',
      Auto_Trade: m.auto_trade || '',
      Typeaccount: m.typeaccount || '',
      timecurent: m.timecurent || '',
      timeUpdated: m.timeUpdated || '',
      symbol: d.symbol || '',
    },
    d
  );
}

function cleanObj(obj) {
  if (!obj || typeof obj !== 'object') return obj;
  const out = {};
  for (const [k, v] of Object.entries(obj)) {
    if (v === undefined) continue;
    out[k] = v;
  }
  return out;
}

function upper(s) {
  return String(s || '').toUpperCase().trim();
}

function isTradeTrue(symObj) {
  return upper(symObj?.trade) === 'TRUE';
}

function numIndex(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n : 999999999;
}

// ==============================
// Main
// ==============================
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

  wss.on('error', (error) => {
    console.log(colors.red, '[WS_CHART] Server error:', colors.reset, error?.message || error);
  });

  wss.on('connection', async (ws, req) => {
    try {
      const symbol = getSymbolFromPath(req.url);
      const query = parseQuery(req.url);

      const brokerRaw = String(query.broker || '').trim();   // ABC
      const broker_ = formatString(brokerRaw);               // abc

      if (!symbol || !broker_) {
        ws.send(JSON.stringify({
          type: 'ERROR',
          message: 'Invalid url. Use: ws://IP:PORT/EURUSD?broker=ABC',
          symbol,
          broker: brokerRaw,
        }));
        ws.close();
        return;
      }

      ws.__symbol = symbol;
      ws.__broker_ = broker_;
      log(colors.green, 'WS CHART', colors.reset, `OPEN ${symbol} broker=${broker_}`);

      ws.on('error', (err) => {
        console.error('[WS_CHART] ws error:', err?.message || err);
      });

      // ==============================
      // REALTIME LOOP (1 message type)
      // ==============================
      let lastSent = '';
      const intervalMs = 200; // realtime (bạn đang cần tốc độ)

      const timer = setInterval(async () => {
        if (ws.readyState !== WebSocket.OPEN) return;

        try {
          // ===========
          // C1: broker đang chọn
          // ===========
          const [meta1, sym1, ohlc1] = await Promise.all([
            RedisH.getBrokerMeta(broker_),
            RedisH.getSymbol(broker_, symbol),
            getOHLC(broker_, symbol),
          ]);

          // nếu broker đang chọn không có symbol
          if (!meta1 || !sym1) {
            const payload = {
              type: 'CHART_INIT_3',
              symbol,
              broker: broker_,
              charts: [
                { chartId: 'c1', title: `${symbol} | ${brokerRaw} | Bid&Ask`, Broker: brokerRaw, Broker_: broker_, ohlc: [], note: 'NO_SYMBOL' },
                { chartId: 'c2', title: `${symbol} | MIN-INDEX | Bid&Ask`, ohlc: [], note: 'NO_MIN' },
                { chartId: 'c3', title: `${symbol} | MIN-INDEX | bid_mdf&ask_mdf`, ohlc: [], note: 'NO_MIN' },
              ],
            };
            const s = JSON.stringify(payload);
            if (s !== lastSent) {
              lastSent = s;
              ws.send(s);
            }
            return;
          }

          const c1 = buildItem(meta1, sym1, broker_);
          c1.ohlc = Array.isArray(ohlc1) ? ohlc1 : [];

          // ===========
          // C2: min-index (trade TRUE)
          // ===========
          // getBestSymbolFast của bạn: min-index + trade TRUE
          const best = await RedisH.getBestSymbolFast(symbol);
          if (!best || !best.Broker_) {
            const payload = {
              type: 'CHART_INIT_3',
              symbol,
              broker: broker_,
              charts: [
                cleanObj({ chartId: 'c1', title: `${symbol} | ${c1.Broker} | Bid&Ask`, ...c1 }),
                { chartId: 'c2', title: `${symbol} | MIN-INDEX | Bid&Ask`, ohlc: [], note: 'NO_MIN' },
                { chartId: 'c3', title: `${symbol} | MIN-INDEX | bid_mdf&ask_mdf`, ohlc: [], note: 'NO_MIN' },
              ],
            };
            const s = JSON.stringify(payload);
            if (s !== lastSent) {
              lastSent = s;
              ws.send(s);
            }
            return;
          }

          const minBroker_ = best.Broker_;

          const [meta2, sym2, ohlc2] = await Promise.all([
            RedisH.getBrokerMeta(minBroker_),
            RedisH.getSymbol(minBroker_, symbol),
            getOHLC(minBroker_, symbol),
          ]);

          // nếu broker min-index thiếu symbol (hiếm): vẫn build nhưng rỗng
          const c2 = buildItem(meta2 || {}, sym2 || {}, minBroker_);
          c2.ohlc = Array.isArray(ohlc2) ? ohlc2 : [];

          // ===========
          // C3: OHLC giống C2, nhưng GIÁ = bid_mdf/ask_mdf của C1
          // ===========
          const c3 = { ...c2 };
          // dùng giá mdf của chart 1, fallback sang bid/ask nếu thiếu
          c3.bid = (c1.bid_mdf ?? c1.bid ?? c3.bid);
          c3.ask = (c1.ask_mdf ?? c1.ask ?? c3.ask);

          // ===========
          // FINAL PAYLOAD (chỉ 1 loại message)
          // ===========
          const payload = {
            type: 'CHART_INIT_3',
            symbol,
            broker: broker_,
            charts: [
              cleanObj({
                chartId: 'c1',
                title: `${symbol} | ${c1.Broker} | Bid&Ask`,
                ...c1,
              }),
              cleanObj({
                chartId: 'c2',
                title: `${symbol} | ${c2.Broker} | Bid&Ask`,
                ...c2,
              }),
              cleanObj({
                chartId: 'c3',
                title: `${symbol} | ${c2.Broker} | bid_mdf&ask_mdf`,
                ...c3,
              }),
            ],
          };

          const s = JSON.stringify(payload);
          if (s !== lastSent) {
            lastSent = s;
            ws.send(s);
          }

        } catch (err) {
          console.error('[WS_CHART] loop error:', err?.message || err);
        }
      }, intervalMs);

      ws.on('close', () => {
        clearInterval(timer);
        log(colors.red, 'WS CHART', colors.reset, `CLOSE ${symbol} broker=${broker_}`);
      });
    } catch (e) {
      console.error('[WS_CHART] connection error:', e?.message || e);
      try { ws.close(); } catch {}
    }
  });

  server.listen(port, () => {
    log(colors.green, 'WS CHART', colors.reset, `ws://IP:${port}/EURUSD?broker=ABC`);
  });

  return wss;
}

module.exports = setupWebSocketServer;
