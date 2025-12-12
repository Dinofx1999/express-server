/* eslint-disable */

const { log, colors } = require('../Helpers/Log');
const { getTimeGMT7 } = require('../Helpers/time');
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

  try {
    await connectMongoDB();
    console.log(`[JOB ${process.pid}] MongoDB connected.`);
  } catch (error) {
    console.error(`[JOB ${process.pid}] MongoDB connection failed:`, error.message);
    return;
  }

  const interval = Number(process.env.CRON_INTERVAL_ANALYZE || 500);
  let isRunning = false;

  // ✅ FUNCTION để tái sử dụng
  const runAnalysis = async () => {
    if (isRunning) return;
    isRunning = true;

    try {
      let Type_1 = [];
      let Type_2 = [];

      const result = await getAnalysisInTimeWindow(Number(process.env.TIME_SECONDS), {
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
        Type_2,
        time_analysis: getTimeGMT7(),
      };

      await Redis.saveAnalysis(data);
      // console.log(`[JOB ${process.pid}] Analysis saved: Type_1=${Type_1.length}, Type_2=${Type_2.length}`);
    } catch (error) {
      console.error(`[JOB ${process.pid}] Analysis error:`, error.message);
    } finally {
      isRunning = false;
    }
  };

  // ✅ CHẠY NGAY LẦN ĐẦU
  await runAnalysis();

  // ✅ SAU ĐÓ MỚI setInterval
  setInterval(runAnalysis, interval);

  console.log(`[JOB ${process.pid}] Save Analysis ready.`);
}
module.exports = startJob;