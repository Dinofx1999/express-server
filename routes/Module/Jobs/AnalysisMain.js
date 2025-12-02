const { getSymbolInfo } = require('../Jobs/Func.helper');
const Redis = require('../Redis/clientRedis');
const { Analysis } = require('../Jobs/Analysis');
const { connectMongoDB } = require('../../Database/mongodb');
const { getAllSymbolConfigs } = require('../../Database/symbol-config.helper');
const { colors } = require('../Helpers/Log');

let ConfigSymbol = [];
let symbolConfigMap = new Map();

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
  const TIMEOUT_WARN = 1000;

  async function tick() {
    const startTime = Date.now();

    try {
      // 1️⃣ Lấy danh sách symbols - 1 call
      const ALL_Symbol = await Redis.getAllUniqueSymbols();
      
      // Chuẩn hóa symbols
      const symbols = ALL_Symbol
        .map(s => String(s).trim().toUpperCase())
        .filter(Boolean);

      // 2️⃣ Lấy TẤT CẢ price data 1 lần - 1 call (thay vì 272 calls!)
      const priceDataMap = await Redis.getMultipleSymbolDetails(symbols);

      // 3️⃣ Phân tích song song
      await Promise.all(
        symbols.map(async (sym) => {
          try {
            const symbolConfig = symbolConfigMap.get(sym);
            const priceData = priceDataMap.get(sym);

            if (!priceData || priceData.length <= 1) return;
            console.log(priceData);
            await Analysis(priceData, sym, symbolConfig);
          } catch (err) {
            console.error(`[Analysis] Error ${sym}:`, err.message);
          }
        })
      );

      const elapsed = Date.now() - startTime;

      if (elapsed > TIMEOUT_WARN) {
        console.log(
          colors.red, `⚠️ JOB ANALYSIS`, colors.reset,
          `SLOW: ${elapsed}ms (${symbols.length} symbols)`
        );
      } else {
        console.log(
          colors.green, `✓ JOB ANALYSIS`, colors.reset,
          `${elapsed}ms (${symbols.length} symbols)`
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