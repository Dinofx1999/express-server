const { getSymbolInfo } = require('../Jobs/Func.helper');
const Redis = require('../Redis/clientRedis');
const { Analysis } = require('../Jobs/Analysis');
const { Analysis_Type2 } = require('../Jobs/Analysis_Type2');
const { connectMongoDB } = require('../../Database/mongodb');
const { getAllSymbolConfigs } = require('../../Database/symbol-config.helper');
const { colors } = require('../Helpers/Log');

let ConfigSymbol = [];
let symbolConfigMap = new Map();

async function startJob() {
  console.log(`[JOB ${process.pid}] Analysis booting...`);

  // Kết nối MongoDB
  try {
    await connectMongoDB();
    console.log(`[JOB ${process.pid}] MongoDB connected.`);
  } catch (error) {
    console.error(`[JOB ${process.pid}] MongoDB connection failed:`, error.message);
    return;
  }

  // Load config lần đầu
  try {
    await refreshConfig();
    console.log(`[JOB ${process.pid}] Loaded ${ConfigSymbol.length} symbol configs.`);
  } catch (error) {
    console.error(`[JOB ${process.pid}] Failed to load configs:`, error.message);
  }

  // Bắt đầu các loops
  runAnalysisLoop();
  runConfigRefreshLoop();

  console.log(`[JOB ${process.pid}] Analysis ready.`);
}

// Cache config vào Map để lookup O(1)
async function refreshConfig() {
  ConfigSymbol = await getAllSymbolConfigs();
  symbolConfigMap.clear();
  for (const config of ConfigSymbol) {
    const key = String(config.symbol || config.name || '').trim().toUpperCase();
    if (key) symbolConfigMap.set(key, config);
  }
}

// Loop chính để phân tích
function runAnalysisLoop() {
  const interval = Number(process.env.CRON_INTERVAL_ANALYZE || 500);
  const TIMEOUT_WARN = 1000;

  async function tick() {
    const startTime = Date.now();
    const configAdmin = await Redis.getConfigAdmin();
    console.log(configAdmin);
    const Delay_Stop = configAdmin.Delay_Stop || 10;
    const Spread_Plus = configAdmin.SpreadPlus || 1.2;
    try {
      // 1️⃣ Lấy danh sách symbols
      const ALL_Symbol = await Redis.getAllUniqueSymbols();

      // Chuẩn hóa symbols
      const symbols = ALL_Symbol
        .map(s => String(s).trim().toUpperCase())
        .filter(Boolean);

      // 2️⃣ Lấy TẤT CẢ price data 1 lần (thay vì 272 calls!)
      const priceDataMap = await Redis.getMultipleSymbolDetails(symbols);

      // 3️⃣ Phân tích song song
      await Promise.all(
        symbols.map(async (sym) => {
          try {
            // Lookup từ Map O(1)
            const symbolConfig = symbolConfigMap.get(sym) || getSymbolInfo(ConfigSymbol, sym);
            const priceData = priceDataMap.get(sym);

            if (!priceData || priceData.length <= 1) return;
            const TYPE = String(process.env.ANALYSIS_TYPE);
            // console.log(TYPE);
            if(String(process.env.ANALYSIS_TYPE) === 'ANALYSIS_TYPE_1'){
                await Analysis(priceData, sym, symbolConfig ,Delay_Stop ,Spread_Plus);
            }else if(String(process.env.ANALYSIS_TYPE) === 'ANALYSIS_TYPE_2'){
                await Analysis_Type2(priceData, sym, symbolConfig , Delay_Stop ,Spread_Plus);
            }
          } catch (err) {
            console.error(`[Analysis] Error ${sym}:`, err.message);
          }
        })
      );

      const elapsed = Date.now() - startTime;

      // Monitoring
      if (elapsed > TIMEOUT_WARN) {
        console.log(
          colors.red, `⚠️ JOB ANALYSIS`, colors.reset,
          `SLOW: ${elapsed}ms (${symbols.length} symbols)`
        );
      } else {
        // console.log(
        //   colors.green, `✓ JOB ANALYSIS`, colors.reset,
        //   `${elapsed}ms (${symbols.length} symbols)`
        // );
      }
    } catch (error) {
      console.error(`[JOB ${process.pid}] Analysis error:`, error.message);
    }

    // Tính thời gian còn lại và schedule tick tiếp theo
    const elapsed = Date.now() - startTime;
    const nextDelay = Math.max(0, interval - elapsed);
    setTimeout(tick, nextDelay);
  }

  tick();
}

// Loop refresh config
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