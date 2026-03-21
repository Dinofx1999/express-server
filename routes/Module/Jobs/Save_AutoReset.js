/* eslint-disable */
const {formatString, normSym} = require('../Helpers/text.format');
const {getTimeGMT7 ,getMinuteSecond } = require('../Helpers/time');
const {getSymbolInfo , getForexSession ,Digit , Digit_Rec} = require('../Jobs/Func.helper');
// const {Analysis} = require('../Jobs/Analysis');
const Redis = require('../Redis/clientRedis');
const {Insert_UpdateAnalysisConfig} = require('../../Database/analysis-config.helper');

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

       if(percent_SELL > 90 && percent_SELL< 50) console.log("Sym: " , symbol, "Broker: ", CURRENT.broker, " A: ",BID_CR , ", B: ", ASK_CHECK ,", C: ",END_POINT_SELL_CHECK , " => Percent: ", percent_SELL, "%");
        if(percent_BUY > 90 && percent_BUY < 50) console.log("Sym: " , symbol, "Broker: ", CURRENT.broker, " A: ",ASK_CR , ", B: ", BID_CHECK ,", C: ",END_POINT_BUY_CHECK , " => Percent: ", percent_BUY, "%");

    }
    } catch (error) {
        console.error(`Lỗi Phân Tích Gia de reset${symbol}:`, error);
    }
}

function calcPercent(A, B, C) {
  const BC = Math.abs(B - C); // khoảng cách B->C = 100%
  if (BC === 0) return null;  // tránh chia 0

  const AC = Math.abs(A - C); // khoảng cách A->C
  const percent = (AC / BC) * 100;

  return Math.round(percent * 100) / 100; // làm tròn 2 chữ số
}

module.exports = {AutoReset };