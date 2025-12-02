const { getSymbolInfo } = require('../Jobs/Func.helper');
const Redis = require('../Redis/clientRedis');
const { Analysis } = require('../Jobs/Analysis');
const { connectMongoDB } = require('../../Database/mongodb');
const { getAllSymbolConfigs } = require('../../Database/symbol-config.helper');
const { colors } = require('../Helpers/Log');

let ConfigSymbol = [];
let symbolConfigMap = new Map(); // Cache config dạng Map để lookup O(1)

async function startJob() {
  console.log(`[JOB ${process.pid}] Analysis booting...`);

  try {
    await connectMongoDB();
    console.log(`[JOB ${process.pid}] MongoDB connected.`);
  } catch (error) {
    console.error(`[JOB ${process.pid}] MongoDB connection failed:`, error.message);
    return;
  }

  try {
    await refreshConfig();
    console.log(`[JOB ${process.pid}] Loaded ${ConfigSymbol.length} symbol configs.`);
  } catch (error) {
    console.error(`[JOB ${process.pid}] Failed to load configs:`, error.message);
  }

  runAnalysisLoop();
  runConfigRefreshLoop();

  console.log(`[JOB ${process.pid}] Analysis ready.`);
}

// Cache config vào Map để lookup nhanh O(1)
async function refreshConfig() {
  ConfigSymbol = await getAllSymbolConfigs();
  symbolConfigMap.clear();
  for (const config of ConfigSymbol) {
    const key = String(config.symbol || config.name || '').trim().toUpperCase();
    if (key) symbolConfigMap.set(key, config);
  }
}

function runAnalysisLoop() {
  const interval = Number(process.env.CRON_INTERVAL_ANALYZE || 500);
  const TIMEOUT_WARN = 1000; // Cảnh báo nếu > 1 giây

  async function tick() {
    const startTime = Date.now();

    try {
      const ALL_Symbol = await Redis.getAllUniqueSymbols();

      // Chạy HOÀN TOÀN song song - không chia batch
      await Promise.all(
        ALL_Symbol.map(async (symbol) => {
          try {
            const sym = String(symbol).trim().toUpperCase();
            if (!sym) return;

            // Lookup từ Map O(1) thay vì search O(n)
            const symbolConfig = symbolConfigMap.get(sym) || getSymbolInfo(ConfigSymbol, sym);
            
            const priceData = await Redis.getSymbolDetails(sym);
            if (!priceData || priceData.length <= 1) return;

            await Analysis(priceData, sym, symbolConfig);
          } catch (err) {
            // Lỗi 1 symbol không ảnh hưởng symbol khác
            console.error(`[Analysis] Error ${symbol}:`, err.message);
          }
        })
      );

      const elapsed = Date.now() - startTime;

      // Monitoring
      if (elapsed > TIMEOUT_WARN) {
        console.log(
          colors.red, `⚠️ JOB ANALYSIS`, colors.reset,
          `SLOW: ${elapsed}ms (${ALL_Symbol.length} symbols) - Exceeded 1s limit!`
        );
      } else {
        console.log(
          colors.green, `✓ JOB ANALYSIS`, colors.reset,
          `${elapsed}ms (${ALL_Symbol.length} symbols)`
        );
      }
    } catch (error) {
      console.error(`[JOB ${process.pid}] Analysis error:`, error.message);
    }

    const elapsed = Date.now() - startTime;
    const nextDelay = Math.max(0, interval - elapsed);
    setTimeout(tick, nextDelay);
  }

  tick();
}

function runConfigRefreshLoop() {
  const refreshInterval = Number(process.env.CONFIG_REFRESH_INTERVAL || 10000);

  async function refreshTick() {
    try {
      await refreshConfig();
    } catch (error) {
      console.error(`[JOB ${process.pid}] Refresh config error:`, error.message);
    }
    setTimeout(refreshTick, refreshInterval);
  }

  refreshTick();
}

module.exports = startJob;