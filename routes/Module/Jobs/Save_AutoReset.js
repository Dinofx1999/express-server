/* eslint-disable */
const {formatString, normSym} = require('../Helpers/text.format');
const {getTimeGMT7 ,getMinuteSecond } = require('../Helpers/time');
const {getSymbolInfo , getForexSession ,Digit , Digit_Rec} = require('../Jobs/Func.helper');
const SymbolDebounceQueue = require("../Redis/DebounceQueue");
// const {Analysis} = require('../Jobs/Analysis');
const Redis = require('../Redis/clientRedis');
const {Insert_UpdateAnalysisConfig} = require('../../Database/analysis-config.helper');

const queue = new SymbolDebounceQueue({
  debounceTime: 3000, // 5s không có payload mới
  maxWaitTime: 5000,  // Tối đa 10s
  maxPayloads: 5000,  // Tối đa 5000 unique payloads
  delayBetweenTasks: 300, // 500ms delay giữa các task
  cooldownTime: 5000, // 10s cooldown after processing
});


 async function AutoReset(data, symbol ,symbolConfig_data ,Delay_Stop, spread_plus) {
    try {

    let total_length = data.length;
    for(let i = 1; i < total_length; i++){
        const CHECK = data[0];
        const CURRENT = data[i];
        const Time_CR_Check = getMinuteSecond(CHECK.timecurent_broker);
        const Time_CR_Current = getMinuteSecond(CURRENT.timecurent_broker);

        // if(CHECK.symbol === "GBPUSD" && Time_CR_Check ) console.log(getMinuteSecond(CURRENT.timecurent_broker),getMinuteSecond(getTimeGMT7('datetime')), CURRENT);

        let Max_Delay = Number(process.env.MAX_NEGATIVE_DELAY) * 60; // Chuyển phút sang ms
        let Delay_symbol = Number(CURRENT.timedelay);
        if( Delay_symbol < Max_Delay && CURRENT.timecurrent + Delay_Stop < CHECK.timecurrent && CHECK.trade !== "TRUE") continue; // Bỏ qua nếu delay quá lớn

        let SPREAD_MIN_CURRENT = Number(CURRENT.spread_mdf);
        let SPREAD_X_CURRENT = Number(spread_plus) || 1;

        let SPREAD_X_SESSION = 1;
        let SESSION = getForexSession(getTimeGMT7());

        if(symbolConfig_data){
            if(SESSION === "Sydney") SPREAD_X_SESSION = symbolConfig_data.Sydney;
            if(SESSION === "Tokyo") SPREAD_X_SESSION = symbolConfig_data.Tokyo;
            if(SESSION === "London") SPREAD_X_SESSION = symbolConfig_data.London;
            if(SESSION === "NewYork") SPREAD_X_SESSION = symbolConfig_data.NewYork;
            if( CURRENT.typeaccount === "STD" && SPREAD_MIN_CURRENT < symbolConfig_data.Spread_STD) SPREAD_MIN_CURRENT = symbolConfig_data.Spread_STD;
            if( CURRENT.typeaccount === "ECN" && SPREAD_MIN_CURRENT < symbolConfig_data.Spread_ECN) SPREAD_MIN_CURRENT = symbolConfig_data.Spread_ECN;
        }

        let Digit_ = parseInt(CHECK.digit);
        let Point =  parseFloat(Digit(Digit_));
        let BID_CHECK = parseFloat(CHECK.bid_mdf);
        let ASK_CHECK = parseFloat(CHECK.ask_mdf);
        let SPREAD_POINT = parseFloat(SPREAD_MIN_CURRENT) * Point;
        let ASK_CHECK_MDF = ASK_CHECK + SPREAD_POINT;
        let SPREAD_PLUS = parseFloat(SPREAD_X_CURRENT * SPREAD_X_SESSION * SPREAD_MIN_CURRENT);
        let SPREAD_PLUS_POINT = parseFloat(SPREAD_PLUS * Point);
        let ASK_CR = parseFloat(CURRENT.ask_mdf);
        let BID_CR = parseFloat(CURRENT.bid_mdf);
        
        //Type
        let Type = 'Delay Price';
        if(Number(data[i].timedelay)<0)
            Type = 'Delay Price Stop';

    //   if(symbol === "NZDUSD" && CURRENT.broker ==="valutrade-mt5") console.log("SPREAD MIN: " , SPREAD_MIN_CURRENT ,
    //     " , Spread x: ", SPREAD_X_SESSION ,
    //     " , Spread X Cr: ", spread_plus , " , Spread S: ", Spread_Sync , Point_Spread , Price_BUY_CURRENT , " < " , Price_BUY_CHECK );
        const END_POINT_BUY_CHECK = parseFloat(BID_CHECK - SPREAD_PLUS_POINT);
        const END_POINT_SELL_CHECK = parseFloat(ASK_CHECK + SPREAD_PLUS_POINT);
        
        const percent_BUY = calcPercent(ASK_CR, BID_CHECK, END_POINT_BUY_CHECK);
        const percent_SELL = calcPercent(BID_CR, ASK_CHECK, END_POINT_SELL_CHECK);
        // if(CURRENT.broker ==="FXPRO MT5" && symbol === "BTCUSD")console.log(`A = ${ASK_CR} B = ${ASK_CHECK} C = ${END_POINT_SELL_CHECK} => Percent: ${percent_BUY}%`);
       // ---- SELL ----
        if (percent_SELL > Number(process.env.PERCENT_A)  && percent_SELL < Number(process.env.PERCENT_B)) {
        // console.log(`SELL Alert Condition Met for ${symbol} at broker ${CURRENT.broker}: Percent: ${percent_SELL}%`);
        await checkAndAlert(symbol,CURRENT.port, CURRENT.broker, percent_SELL, 'SELL',BID_CR, ASK_CHECK, END_POINT_SELL_CHECK);
        } else {
        // Thoát điều kiện → reset đếm
        clearAlert(symbol, CURRENT.broker, 'SELL');
        }
        
        // ---- BUY ----
        if (percent_BUY > Number(process.env.PERCENT_A) && percent_BUY < Number(process.env.PERCENT_B)) {
            // console.log(`BUY Alert Condition Met for ${symbol} at broker ${CURRENT.broker}: Percent: ${percent_BUY}%`);
         await checkAndAlert(symbol, CURRENT.port,  CURRENT.broker, percent_BUY, 'BUY',ASK_CR, BID_CHECK, END_POINT_BUY_CHECK);
        } else {
        clearAlert(symbol, CURRENT.broker, 'BUY');
        }
    }
    } catch (error) {
        console.error(`Lỗi Phân Tích Gia de reset${symbol}:`, error);
    }
}


// Track trạng thái từng symbol+broker
const alertTracker = new Map();

async function checkAndAlert(symbol, Port, broker, percent, type, A, B, C) {
  const key = `${symbol}|${broker}|${type}`;
  const now = Date.now();
  const REQUIRED_MS = process.env.ALERT_HOLD_TIME_MS || 3000; // Thời gian giữ cảnh báo (mặc định 3s)

  const tracker = alertTracker.get(key);

  if (!tracker) {
    // Lần đầu thoả → bắt đầu đếm
    alertTracker.set(key, { startTime: now, alerted: false });
    return;
  }

  // Đã alert rồi → bỏ qua, chờ clearAlert khi hụt điều kiện
  if (tracker.alerted) return;

  // Chưa đủ 3s → tiếp tục chờ
  if ((now - tracker.startTime) < REQUIRED_MS) return;

  // ✅ Đủ 3s → đánh dấu alerted + thông báo
  alertTracker.set(key, { ...tracker, alerted: true });

  console.log(
    `🚨 ALERT [${type}] Sym: ${symbol} | Broker: ${broker}` +
    ` | A: ${A}, B: ${B}, C: ${C} | Percent: ${percent}%` +
    ` | Liên tục: ${((now - tracker.startTime) / 1000).toFixed(1)}s`
  );

  // ✅ Publish Redis
  try {
    const groupKey = "RESET";
    const payload = { symbol, broker };

    queue.receive(groupKey, payload, async (symb, meta) => {
      console.log(`🚀 Processing: ${symb}`);
      await Redis.publish(String(Port), JSON.stringify({
        Symbol: symb,
        Broker: formatString(broker),
      }));
    });
  } catch (e) {
    console.error('[checkAndAlert] publish error:', e?.message || e);
  }

alertTracker.delete(key);
}
// function checkAndAlert(symbol, Port, broker, percent, type, A, B, C) {
//   const key = `${symbol}|${broker}|${type}`;
//   const now = Date.now();
//   const REQUIRED_MS = 3000;

//   const tracker = alertTracker.get(key);

//   console.log(`[CHECK] key=${key} | tracker=${JSON.stringify(tracker)} | elapsed=${tracker ? now - tracker.startTime : 0}ms`);

//   if (!tracker) {
//     alertTracker.set(key, { startTime: now, alerted: false });
//     console.log(`[START] Bắt đầu đếm cho ${key}`);
//     return;
//   }

//   if (tracker.alerted) {
//     console.log(`[SKIP] Đã alert rồi, chờ clearAlert`);
//     return;
//   }

//   if ((now - tracker.startTime) < REQUIRED_MS) {
//     console.log(`[WAIT] Chưa đủ 3s: ${((now - tracker.startTime)/1000).toFixed(1)}s`);
//     return;
//   }

//   // ✅ Đủ 3s
//   alertTracker.set(key, { ...tracker, alerted: true });
//   console.log(`🚨 ALERT [${type}] Sym: ${symbol} | Broker: ${broker} | Percent: ${percent}%`);
// }
function clearAlert(symbol, broker, type) {
  alertTracker.delete(`${symbol}|${broker}|${type}`);
}

function calcPercent(A, B, C) {
  const BC = Math.abs(B - C); // khoảng cách B->C = 100%
  if (BC === 0) return null;  // tránh chia 0

  const AC = Math.abs(A - C); // khoảng cách A->C
  const percent = (AC / BC) * 100;

  return Math.round(percent * 100) / 100; // làm tròn 2 chữ số
}

module.exports = {AutoReset };