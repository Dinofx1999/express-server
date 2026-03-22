var express = require('express');
var router = express.Router();
const {  API_ALL_INFO_BROKERS ,
          API_PORT_BROKER_ENDPOINT, 
          API_DESTROY_BROKER,
          API_RESET , 
          VERSION ,
          API_CONFIG_SYMBOL , 
          API_ANALYSIS_CONFIG , API_PRICE_SYMBOL ,
          API_RESET_ALL_BROKERS , API_GET_CONFIG_SYMBOL } = require('../Module/Constants/API.Service');

// const SymbolDebounceQueue = require("../Module/Redis/DebounceQueue");
const queue = require('../Module/Redis/symbolQueue.singleton');
const Redis = require('../Module/Redis/clientRedis');
const RedisH2 = require('../Module/Redis/redis.helper2');
const { getAllBrokers , getBrokerMeta,getAllUniqueSymbols ,getSymbolAcrossBrokers , getAllBrokerMetaArray} = require("../Module/Redis/redis.price.query");
const {flushAllRedis , getAllPricesByBroker ,getMultipleSymbolAcrossBrokersWithMetaFast , getRedis} = require('../Module/Redis/redis.helper2');
const PriceFlush = require('../Module/Redis/priceBuffer.redisFlush');
// const { getAllUniqueSymbols } = require("../Redis/redis.price.query");
// RedisH.initRedis({
//   host: '127.0.0.1',
//   port: 6379,
//   db: 0,          // ⚠️ PHẢI giống worker ghi
//   compress: true
// });

const { authRequired, requireRole } = require('../Auth/authMiddleware');
const { calculatePercentage } = require('../Module/Helpers/text.format');


// const queue = new SymbolDebounceQueue({
//   debounceTime: 100, // 5s không có payload mới
//   maxWaitTime: 100,  // Tối đa 10s
//   maxPayloads: 500,  // Tối đa 5000 unique payloads
//   delayBetweenTasks: 100, // 500ms delay giữa các task
//   cooldownTime: 5000, // 10s cooldown after processing
// });
// ✅ Áp dụng auth cho TẤT CẢ routes trong controller này
// router.use(authRequired);

/* Lấy Toàn Bộ Thông Tin Từ Redis */
router.get(`/${API_ALL_INFO_BROKERS}`,authRequired, async function(req, res, next) {
 const data = await getAllBrokers();
  return res.status(200).json(data);
});

router.get(`/${API_ALL_INFO_BROKERS}:broker/info`,authRequired, async function(req, res, next) {
  console.log("Params Broker:", req.params.broker);
 const data = await getAllPricesByBroker(req.params.broker);
 let result = [];
 data.map(item => {
    if(item.trade.toUpperCase() === "TRUE")result.push(item.symbol);
 });
  return res.status(200).json(result);
});


router.get(`/${VERSION}/:symbol/getdata`,authRequired, async function(req, res, next) {
  const { symbol } = req.params;
 const test = await getSymbolAcrossBrokers(symbol);
  return res.status(200).json(test);
});
/* Reset Tất Cả của 1 Broker*/
router.get(`/${API_RESET}`,authRequired, async function(req, res, next) {
  const { broker , symbol} = req.params;
  const Broker_Check = await getBrokerMeta(broker);
  const PORT = Broker_Check?.port || null;
  const INDEX = Broker_Check?.index || null;
  const message = `Reset Received for Broker: ${broker} , Symbol: ${symbol}`;
 console.log(message);

  if(symbol === 'ALL-SYMBOLS'){
    console.log("Initiating reset for ALL-SYMBOLS across all brokers...");

    const ALL_Symbol = await getAllUniqueSymbols();
    ALL_Symbol.forEach((symb, index) => {
      // console.log(`Queueing reset for symbol: ${symb}`);
      const groupKey = "RESET";
      const payload = { symbol: symb, brokers: index };
      
      queue.receive(groupKey, payload, async (symb, meta) => {
        console.log(`🚀 Processing: ${symb}`);
        console.log(`Brokers đã gửi: ${meta.brokers.join(", ")}`);
        await SymbolInfo(symb);
      });
    });
    
    

    return res.status(200).json({
      'mess' : `Reset for ALL-SYMBOLS across all brokers initiated.`,
      'code' : 1
    });
  }


  if(PORT && INDEX !== '0' && INDEX !== 0 && INDEX !== null){
    if(broker.toUpperCase() === 'ALL'){
      
      const Broker = await SymbolInfo(symbol);
    }else{
        await Redis.publish(String(PORT), JSON.stringify({
          Symbol: symbol,
          Broker: broker,
        }));
    }
    return res.status(200).json({
      'mess' : message,
      'code' : 1
    });
  }else if(PORT && (INDEX === '0' || INDEX === 0)){
    return res.status(200).json({
      'mess' : `Broker '${broker}' is in index 0, cannot reset.`,
      'code' : 0
    });
  }else if(broker.toUpperCase() === 'ALL'){
    console.log("Initiating reset for ALL brokers for symbol:", symbol);
    await Redis.publish("RESET_ALL", JSON.stringify({
    Symbol: symbol,
    Broker: "ALL-BROKERS",
  }));
  console.log("Published RESET_ALL message to Redis.");
    return res.status(200).json({
      'mess' : message,
      'code' : 1
    });
  }
  return res.status(400).json({
    'mess' : `Broker '${broker}' Not Found`,
    'code' : 0
  });
});
router.get(`/${API_DESTROY_BROKER}`,authRequired, async function(req, res, next) {
  const { broker} = req.params;
  const Broker_Check = await getBrokerMeta(broker);
  const PORT = Broker_Check?.port || null;
  if(PORT){
    await Redis.publish(String(PORT), JSON.stringify({
    type: "destroy_broker",
    Broker: broker,
  }));
    return res.status(200).json({
      'mess' : `Destroy Broker ${broker} Sent`,
      'code' : 1
    });
  }
  return res.status(400).json({
    'mess' : `Broker ${broker} Not Found`,
    'code' : 0
  });
});
router.get(`/${VERSION}/reset-all-brokers`,authRequired, async function(req, res, next) {
  if (isResetting) {
    return res.status(409).json({
      success: false,
      message: 'Reset is already in progress',
      progress: resetProgress
    });
  }

  res.json({ 
    success: true, 
    message: 'Reset process started in background' 
  });

  resetBrokersLoop().catch(err => {
    console.error('❌ resetBrokersLoop failed:', err);
  });
});


// API để check progress
router.get('/v1/api/reset-status', (req, res) => {
  res.json({
    isRunning: isResetting,
    progress: resetProgress
  });
});
router.get(`/${VERSION}/:symbol/:broker/:type_order/:price_bid/:key_secret/order`,authRequired, async function(req, res, next) {
  const { symbol, broker, type_order, price_bid, key_secret } = req.params;
  await Redis.publish("ORDER", JSON.stringify({
          Symbol: symbol,
          Broker: broker,
          Type_Order: type_order,
          Price_Bid: price_bid,
          Key_SECRET: key_secret
        }));
  return  res.status(200).json({
    'mess' : `Order Received for Broker: ${broker} , Symbol: ${symbol} , Type_Order: ${type_order}`,
    'code' : 1
  });
});
router.get(`/${VERSION}/:broker/:symbol/:points/test-reset`,authRequired, async function(req, res, next) {
  const { broker , symbol , points} = req.params;
  const Broker_Check = await getBrokerMeta(broker);
  const PORT = Broker_Check?.port || null;
  if(Broker_Check){
     const PORT = Broker_Check?.port || null;
     await Redis.publish(String(PORT), JSON.stringify({
        Symbol: symbol,
        Broker: broker,
        Points: points,
        Type : "Test_price"
      }));
  res.status(200).json({ message: `Sended Broker ${broker} for Symbol ${symbol} with Points ${points}.`, success: true });
  }else{
    res.status(400).json({ message: `Broker ${broker} Not Found.` , success: false });
  }
});
router.get(`/${VERSION}/test_time_open`,authRequired, async function(req, res, next) {
  await Redis.publish("RESET_ALL", JSON.stringify({
    Type : "Test_Time_Open",
  }));
  res.status(200).json({ message: "Reset all brokers initiated." });
});
router.get(`/${VERSION}/reset-broker-server`,authRequired, async function(req, res, next) {
  try {
    await flushAllRedis();
    res.status(200).json({ message: "All broker data cleared from Redis." });
  }catch (error) {
    console.error("Error clearing broker data:", error);
    res.status(500).json({ message: "Error clearing broker data." });
  }
    
   
});


// Assumptions:
// - Redis: pub/sub client có .publish(channel, message)
// - RedisH2: redis.helper2.js (có getRedis(), getAllBrokers())
// - calculatePercentage(statusString): hàm của bạn (giữ nguyên)

let isResetting = false;
let resetProgress = { current: 0, total: 0, currentBroker: null, skipped: [] };

async function resetBrokersLoop() {
  if (isResetting) {
    return {
      success: false,
      message: 'Reset is already in progress',
      progress: resetProgress,
    };
  }

  const redis = RedisH2.getRedis();

  // ✅ lấy list broker theo kiểu mới: SET "brokers"
  const brokerList = await RedisH2.getAllBrokers_TRUE(); // array: ['b','ab',...]
  // console.log("Brokers to reset:", brokerList);
  if (!brokerList || brokerList.length <= 1) {
    console.log('❌ No brokers to reset');
    return { success: false, message: 'No brokers to reset' };
  }

  // ✅ lấy meta của tất cả broker (1 lần đầu)
  const pipeMeta = redis.pipeline();
  for (const b of brokerList) pipeMeta.hgetall(`broker_meta:${b}`);
  const metaRes = await pipeMeta.exec();

  const allBrokers = brokerList.map((b, i) => {
    const [err, meta] = metaRes[i] || [];
    const m = (!err && meta) ? meta : {};
    return {
      broker_: b,
      // meta mới có thể là index hoặc indexBroker
      indexBroker: m.indexBroker ?? m.index ?? '',
      totalsymbol: m.totalsymbol ?? '',
      status: m.status ?? '',
    };
  });

  isResetting = true;
  resetProgress = {
    current: 0,
    total: allBrokers.length,
    currentBroker: null,
    skipped: [],
  };

  console.log(`🔄 Starting reset for ${allBrokers.length} brokers...`);
  console.log(`📋 Broker list: ${allBrokers.map(b => b.broker_).join(', ')}`);

  try {
    for (let index = 0; index < allBrokers.length; index++) {
      const broker = allBrokers[index];

      const idxNum = Number(broker.indexBroker);
      if (idxNum === 0 || broker.indexBroker === '0') {
        console.log(`⏭️ [${index + 1}/${allBrokers.length}] Skipping broker ${broker.broker_} at index 0`);
        resetProgress.current = index + 1;
        continue;
      }

      resetProgress.current = index + 1;
      resetProgress.currentBroker = broker.broker_;

      let success = false;
      let retryCount = 0;
      const maxRetries = 3;

      while (!success && retryCount < maxRetries) {
        try {
          console.log(`\n🔄 [${index + 1}/${allBrokers.length}] Processing: ${broker.broker_} (attempt ${retryCount + 1})`);

          // ✅ publish reset
          await Redis.publish(
            "RESET_ALL",
            JSON.stringify({
              Symbol: "ALL-BROKERS",
              Broker: broker.broker_, // broker_ lowercase
            })
          );

          const maxWaitTime = 120000; // 120s
          const startTime = Date.now();
          let lastPercentage = 0;

          let zeroCheckCount = 0;
          const maxZeroChecks = 5;

          while (true) {
            await new Promise(resolve => setTimeout(resolve, 1000)); // poll 1s
            const elapsed = Date.now() - startTime;

            if (elapsed > maxWaitTime) {
              console.log(`⏱️ [${index + 1}/${allBrokers.length}] Timeout for ${broker.broker_} at ${lastPercentage}%`);
              break;
            }

            // ✅ poll đúng theo kiểu mới: broker_meta:<broker_>.status
            const [statusRaw, totalSymbolRaw] = await Promise.all([
              redis.hget(`broker_meta:${broker.broker_}`, 'status'),
              redis.hget(`broker_meta:${broker.broker_}`, 'totalsymbol'),
            ]);

            if (statusRaw == null) {
              console.log(`⚠️ Broker ${broker.broker_} has no status field!`);
              break;
            }

            const totalSym = Number(totalSymbolRaw ?? broker.totalsymbol ?? 0);
            const percentage = Number(Number(calculatePercentage(String(statusRaw))).toFixed(0));

            // ✅ check 0% liên tiếp
            if (percentage === 0) {
              zeroCheckCount++;
              console.log(`⚠️ [${index + 1}/${allBrokers.length}] ${broker.broker_}: 0% (check ${zeroCheckCount}/${maxZeroChecks})`);
              if (zeroCheckCount >= maxZeroChecks) {
                console.log(`🔁 [${index + 1}/${allBrokers.length}] ${broker.broker_}: Still 0% after ${maxZeroChecks} checks, will retry...`);
                break;
              }
            } else {
              zeroCheckCount = 0;
            }

            lastPercentage = percentage;

            // log mỗi ~10s
            if (elapsed % 10000 < 1000) {
              console.log(`⏳ [${index + 1}/${allBrokers.length}] ${broker.broker_}: ${percentage}% (${Math.round(elapsed / 1000)}s)`);
            }

            // ✅ SUCCESS: >=30% hoặc totalsymbol nhỏ
            if (percentage >= 30 || totalSym <= 10) {
              console.log(`✅ [${index + 1}/${allBrokers.length}] ${broker.broker_} reached ${percentage}% (totalsymbol=${totalSym})`);
              success = true;
              break;
            }
          }

          if (!success) {
            retryCount++;
            if (retryCount < maxRetries) {
              console.log(`🔁 [${index + 1}/${allBrokers.length}] Retrying ${broker.broker_} (${retryCount}/${maxRetries})...`);
              await new Promise(resolve => setTimeout(resolve, 5000));
            }
          }
        } catch (error) {
          console.error(`❌ Error processing ${broker.broker_}:`, error?.message || error);
          retryCount++;
          if (retryCount < maxRetries) {
            await new Promise(resolve => setTimeout(resolve, 5000));
          }
        }
      }

      if (!success) {
        console.log(`❌ [${index + 1}/${allBrokers.length}] FAILED: ${broker.broker_} after ${maxRetries} retries`);
        resetProgress.skipped.push(broker.broker_);
      }
    }

    console.log('\n========== RESET SUMMARY ==========');
    console.log(`✅ Completed: ${allBrokers.length - resetProgress.skipped.length}/${allBrokers.length}`);
    if (resetProgress.skipped.length > 0) {
      console.log(`❌ Skipped brokers: ${resetProgress.skipped.join(', ')}`);
    }
    console.log('====================================\n');

    return { success: true, message: 'Completed', skipped: resetProgress.skipped };
  } finally {
    isResetting = false;
  }
}

const SymbolInfo = async (symbol) => {
  const ALL_Symbol = await getAllUniqueSymbols();
  const All_Broker = await getAllBrokers();
  const symbols = ALL_Symbol
        .map(s => String(s).trim())
        .filter(Boolean);
  const priceDataMap = await getMultipleSymbolAcrossBrokersWithMetaFast(symbols , All_Broker ,getRedis());
  const priceData = priceDataMap.get(symbol);
  if(!priceData || priceData.length <= 1) return null;
  for(let i = 0 ; i < priceData.length ; i ++){
    const broker = priceData[i].broker_;
    const index = priceData[i].index;
    const PORT = priceData[i]?.port || null;
    if(i === 0){
      if(index === '0' || index === 0){
        console.log(`Broker ${broker} is in index 0, skipping.`);
        continue;
      }
       await Redis.publish(String(PORT), JSON.stringify({
        Symbol: symbol,
        Broker: broker,
      }));
      await sleep(20);
    }
    await Redis.publish(String(PORT), JSON.stringify({
        Symbol: symbol,
        Broker: broker,
      }));

    console.log(broker);
  }
  
  return priceData;
}

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));


module.exports = router;
