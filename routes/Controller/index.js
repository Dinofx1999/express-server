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


const Redis = require('../Module/Redis/clientRedis');

const { authRequired, requireRole } = require('../Auth/authMiddleware');
const { calculatePercentage } = require('../Module/Helpers/text.format');

// ✅ Áp dụng auth cho TẤT CẢ routes trong controller này
// router.use(authRequired);

/* Lấy Toàn Bộ Thông Tin Từ Redis */
router.get(`/${API_ALL_INFO_BROKERS}`,authRequired, async function(req, res, next) {
  const Data = await Redis.getAllBrokers();
  return res.status(200).json(Data);
});
/* Reset Tất Cả của 1 Broker*/
router.get(`/${API_RESET}`,authRequired, async function(req, res, next) {
  // const Data = await Redis.getAllBrokers();
  // return res.status(200).json(Data);
  const { broker , symbol} = req.params;
  const Broker_Check = await Redis.getBroker(broker);
  const PORT = Broker_Check?.port || null;
  const INDEX = Broker_Check?.index || null;
  const message = `Reset Received for Broker: ${broker} , Symbol: ${symbol}`;

  if(PORT && INDEX !== '0' && INDEX !== 0 && INDEX !== null){
    await Redis.publish(String(PORT), JSON.stringify({
    Symbol: symbol,
    Broker: broker,
  }));
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
  const Broker_Check = await Redis.getBroker(broker);
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
   res.json({ message: 'Reset process started in background' });
  
  // Chạy background (không await)
  resetBrokersLoop().catch(err => {
    console.error('❌ resetBrokersLoop failed:', err);
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
router.get(`/${VERSION}/:symbol/:broker/test_reset`,authRequired, async function(req, res, next) {
  const { broker , symbol} = req.params;
  const Broker_Check = await Redis.getBroker(broker);
  const PORT = Broker_Check?.port || null;
  await Redis.publish(String(PORT), JSON.stringify({
    Symbol: symbol,
    Broker: broker,
    Type : "Test_price"
  }));
  res.status(200).json({ message: "Reset all brokers initiated." });
});
router.get(`/${VERSION}/test_time_open`,authRequired, async function(req, res, next) {
  await Redis.publish("RESET_ALL", JSON.stringify({
    Type : "Test_Time_Open",
  }));
  res.status(200).json({ message: "Reset all brokers initiated." });
});
router.get(`/${VERSION}/reset-broker-server`,authRequired, async function(req, res, next) {
    const deleteResult = await Redis.clearAllAppData();
    return res.status(200).json({
      deleteResult
    });
});

// ✅ Hàm chạy background với while loop
// async function resetBrokersLoop() {

// const allBrokers = await Redis.getAllBrokers();
// if(allBrokers.length <= 1){
//   console.log('❌ No brokers to reset');
//   return;
// }
// console.log(`🔄 Starting reset for ${allBrokers.length} brokers...`);
// let index = 0;
//   while (index < allBrokers.length && allBrokers.length > 1) {
//     const allBrokers_ = await Redis.getAllBrokers();
//     try {
//       if (allBrokers_.length === 0) {
//         console.log('❌ No brokers found');
//         break;
//       }
//       if(index === 0 ){
       
//          console.log(`✅ Continue Reset: ${allBrokers[index].broker}`);
//          await Redis.publish("RESET_ALL", JSON.stringify({
//           Symbol: "ALL-BROKERS",
//           Broker: allBrokers[index].broker_,
//         }));
//          index++;
//       }
//       const status = String(allBrokers_[index-1].status);
//       const Per_status = Number(Number(calculatePercentage(status)).toFixed(0));
//       if(Per_status >= 30){
//         index++;
//          await Redis.publish("RESET_ALL", JSON.stringify({
//           Symbol: "ALL-BROKERS",
//           Broker: allBrokers[index-1].broker_,
//         }));
//         console.log(`✅ Continue Reset: ${allBrokers[index-1].broker_}`);
//       }
//       if(index === allBrokers.length){
//         console.log('✅ Completed resetting all brokers');
//         break;
//       }
      
//     } catch (error) {
//       console.error('❌ Error in reset loop:', error);
//       // Chờ rồi retry
//       // await new Promise(resolve => setTimeout(resolve, 10000));
//     }
//   }
// }

// async function resetBrokersLoop() {
//   const allBrokers = await Redis.getAllBrokers();
//   if (allBrokers.length <= 1) {
//     console.log('❌ No brokers to reset');
//     return;
//   }
  
//   console.log(`🔄 Starting reset for ${allBrokers.length} brokers...`);
  
//   for (let index = 0; index < allBrokers.length; index++) {
//     const broker = allBrokers[index];
    
//     // Publish reset cho broker hiện tại
//     await Redis.publish("RESET_ALL", JSON.stringify({
//       Symbol: "ALL-BROKERS",
//       Broker: broker.broker_,
//     }));
//     console.log(`✅ Reset started: ${broker.broker_}`);
    
//     // Đợi broker đạt 30% trước khi tiếp tục
//     while (true) {
//       await new Promise(resolve => setTimeout(resolve, 1000)); // delay 1s
      
//       const updatedBrokers = await Redis.getAllBrokers();
//       const currentBroker = updatedBrokers[index];
//       const percentage = Number(calculatePercentage(String(currentBroker.status)).toFixed(0));
      
//       if (percentage >= 30) {
//         console.log(`📊 Broker ${broker.broker_} reached ${percentage}%`);
//         break;
//       }
//     }
//   }
  
//   console.log('✅ Completed resetting all brokers');
// }

async function resetBrokersLoop() {
  const allBrokers = await Redis.getAllBrokers();
  if (allBrokers.length <= 1) {
    console.log('❌ No brokers to reset');
    return;
  }

  console.log(`🔄 Starting reset for ${allBrokers.length} brokers...`);

  for (let index = 0; index < allBrokers.length; index++) {
    const broker = allBrokers[index];

    try {
      await Redis.publish("RESET_ALL", JSON.stringify({
        Symbol: "ALL-BROKERS",
        Broker: broker.broker_,
      }));
      console.log(`✅ [${index + 1}/${allBrokers.length}] Reset started: ${broker.broker_}`);

      // Đợi broker đạt 30% với TIMEOUT
      const maxWaitTime = 60000; // 60 giây tối đa cho mỗi broker
      const startTime = Date.now();
      
      while (true) {
        await new Promise(resolve => setTimeout(resolve, 1000));

        // Check timeout
        if (Date.now() - startTime > maxWaitTime) {
          console.log(`⏱️ [${index + 1}/${allBrokers.length}] Timeout for ${broker.broker_}, moving to next...`);
          break;
        }

        const updatedBrokers = await Redis.getAllBrokers();
        const currentBroker = updatedBrokers.find(b => b.broker_ === broker.broker_);

        if (!currentBroker || !currentBroker.status) {
          console.log(`⚠️ [${index + 1}/${allBrokers.length}] Broker ${broker.broker_} not found, skipping...`);
          break;
        }

        const percentage = Number(Number(calculatePercentage(String(currentBroker.status))).toFixed(0));
        
        // Log progress
        console.log(`⏳ [${index + 1}/${allBrokers.length}] ${broker.broker_}: ${percentage}%`);

        if (percentage >= 30) {
          console.log(`✅ [${index + 1}/${allBrokers.length}] ${broker.broker_} reached ${percentage}%`);
          break;
        }
      }
    } catch (error) {
      console.error(`❌ [${index + 1}/${allBrokers.length}] Error: ${broker.broker_}:`, error.message);
    }
  }

  console.log('✅ Completed resetting all brokers');
}


module.exports = router;
