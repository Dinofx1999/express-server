/* eslint-disable */

const { log, colors } = require('../Helpers/Log');
const {getAllSymbolConfigs } = require('../../Database/symbol-config.helper');
const {getSymbolInfo} = require('../Jobs/Func.helper');
const {Analysis} = require('../Jobs/Analysis');
const { getAnalysisInTimeWindow } = require('../../Database/analysis-config.helper');
const { symbolsSetType } = require('../Jobs/Analysis.helper');
const { connectMongoDB } = require('../../Database/mongodb');
const Redis = require('../Redis/clientRedis');
// const {saveAnalysis} = require('../resdis/redis.store');

async function startJob() {
  console.log(`[JOB ${process.pid}] Cron.Mongo.Analyses booting...`);

    // ⚠️ KẾT NỐI MONGODB TRƯỚC
    try {
        await connectMongoDB();
        console.log(`[JOB ${process.pid}] MongoDB connected.`);
    } catch (error) {
        console.error(`[JOB ${process.pid}] MongoDB connection failed:`, error.message);
        return;
    }
const interval = Number(process.env.CRON_INTERVAL_ANALYZE || 10000);
  // Interval 10 giây
  setInterval(async () => {
    let Type_1 = [];
    let Type_2 = [];
    // const test = symbolsSetType.has('ABC');
    const result  = await getAnalysisInTimeWindow(Number(process.env.TIME_SECONDS), {
      minCount: Number(process.env.MIN_COUNT),
      minKhoangCach: 1,
    });


    result.forEach(item => {
            if (symbolsSetType.has(item.Symbol)) {
              Type_1.push(item);
            } else {
              Type_2.push(item);
            }
          });
    const data = {
      Type_1,
      Type_2
    };
    console.log(`[JOB ${process.pid}] Fetched ${Type_1.length} Type_1 and ${Type_2.length} Type_2 analyses from MongoDB.`);
    await Redis.saveAnalysis(`ANALYSIS`, data);

  }, interval);

  console.log(`[JOB ${process.pid}] Save Analysis ready.`);
}


 async function start_Get_ConfigSymbol() {
  setInterval(() => {
  const m = process.memoryUsage();
  console.log("HeapUsed:", (m.heapUsed/1024/1024).toFixed(2),"MB");
}, 5000);
}
// Auto-run khi process là ROLE=JOB
module.exports = startJob;