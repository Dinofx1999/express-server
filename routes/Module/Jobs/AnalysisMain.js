const { getSymbolInfo } = require('../Jobs/Func.helper');
const Redis = require('../Redis/clientRedis');
const { Analysis } = require('../Jobs/Analysis');
const { connectMongoDB } = require('../../Database/mongodb');
const { getAllSymbolConfigs } = require('../../Database/symbol-config.helper');
let ConfigSymbol = [];

async function startJob() {
  console.log(`[JOB ${process.pid}] Analysis booting...`);

  // Kết nối MongoDB TRƯỚC KHI chạy job
  try {
    await connectMongoDB();
    console.log(`[JOB ${process.pid}] MongoDB connected.`);
  } catch (error) {
    console.error(`[JOB ${process.pid}] MongoDB connection failed:`, error.message);
    return; // Không chạy job nếu không kết nối được
  }

  // Load config lần đầu
  try {
    ConfigSymbol = await getAllSymbolConfigs();
    console.log(`[JOB ${process.pid}] Loaded ${ConfigSymbol.length} symbol configs.`);
  } catch (error) {
    console.error(`[JOB ${process.pid}] Failed to load configs:`, error.message);
  }

  const interval = Number(process.env.CRON_INTERVAL_ANALYZE || 500);

  setInterval(async () => {
    try {
      const now = new Date().toISOString();
      const ALL_Symbol = await Redis.getAllUniqueSymbols();
      // console.log(`[${now}] [JOB ${process.pid}] Fetched ${ALL_Symbol.length} unique symbols.`);
      
      for (const symbol of ALL_Symbol) {
        const sym = String(symbol).trim().toUpperCase();
        const symbolConfig = await getSymbolInfo(ConfigSymbol, sym);
        const priceData = await Redis.getSymbolDetails(sym);
        if (priceData.length <= 1 || sym === undefined) continue;
        await Analysis(priceData, sym, symbolConfig);
      }
    } catch (error) {
      console.error(`[JOB ${process.pid}] Analysis error:`, error.message);
    }
  }, interval);

  // Refresh config định kỳ
  setInterval(async () => {
    try {
      ConfigSymbol = await getAllSymbolConfigs();
    } catch (error) {
      console.error(`[JOB ${process.pid}] Refresh config error:`, error.message);
    }
  }, 1000); // Mỗi 10 giây

  console.log(`[JOB ${process.pid}] Analysis ready.`);
}

module.exports = startJob;