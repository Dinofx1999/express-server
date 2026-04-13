/* eslint-disable */
const {formatString, normSym} = require('../Helpers/text.format');
const {getTimeGMT7 ,getMinuteSecond } = require('../Helpers/time');
const {getSymbolInfo , getForexSession ,Digit , Digit_Rec} = require('../Jobs/Func.helper');
// const {Analysis} = require('../Jobs/Analysis');
const Redis = require('../Redis/clientRedis');
const {Insert_UpdateAnalysisConfig} = require('../../Database/analysis-config.helper');

 async function Analysis(data, symbol ,symbolConfig_data ,Delay_Stop, spread_plus) {
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
            // if( CURRENT.typeaccount === "STD" && SPREAD_MIN_CURRENT < symbolConfig_data.Spread_STD) SPREAD_MIN_CURRENT = symbolConfig_data.Spread_STD;
            // if( CURRENT.typeaccount === "ECN" && SPREAD_MIN_CURRENT < symbolConfig_data.Spread_ECN) SPREAD_MIN_CURRENT = symbolConfig_data.Spread_ECN;
        }

        

        let Digit_ = parseInt(CHECK.digit_root) || parseInt(CHECK.digit);
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

        if(Symbol === "USDJPY") console.log(CHECK.broker);

    //   if(symbol === "NZDUSD" && CURRENT.broker ==="valutrade-mt5") console.log("SPREAD MIN: " , SPREAD_MIN_CURRENT ,
    //     " , Spread x: ", SPREAD_X_SESSION ,
    //     " , Spread X Cr: ", spread_plus , " , Spread S: ", Spread_Sync , Point_Spread , Price_BUY_CURRENT , " < " , Price_BUY_CHECK );

    // if(symbol === "WTI")  console.log('SPREAD_MIN_CURRENT:', SPREAD_MIN_CURRENT, 'Point:', Point, 'Digit_:', Digit_ ,'SPREAD_POINT:', SPREAD_POINT ,'SPREAD_PLUS:', SPREAD_PLUS ,'SPREAD_PLUS_POINT:', SPREAD_PLUS_POINT ,'ASK_CHECK:', ASK_CHECK, 'BID_CHECK:', BID_CHECK, 'ASK_CR:', ASK_CR, 'BID_CR:', BID_CR ,parseFloat(BID_CHECK - SPREAD_PLUS_POINT));
        if(parseFloat(ASK_CR) < parseFloat(BID_CHECK - SPREAD_PLUS_POINT)){
            const KhoangCach = parseFloat((parseFloat(BID_CHECK - SPREAD_PLUS_POINT) - parseFloat(ASK_CR)))*parseFloat(Digit_Rec(parseInt(CHECK.digit))) ;
            // if(symbol === "NZDUSD") console.log(CURRENT.broker,"SPREAD MIN: " , SPREAD_MIN_CURRENT);
            const timeStart = getTimeGMT7();
            const Payload = {
                    Broker: CURRENT.broker,
                    TimeStart: timeStart,
                    TimeCurrent: timeStart,
                    Symbol: symbol,
                    Count: 0,
                    Messenger: "BUY",
                    Broker_Main: CHECK.broker,
                    KhoangCach: parseInt(KhoangCach),
                    Symbol_Raw: CURRENT.symbol_raw,
                    Spread_main: CURRENT.spread,
                    Spread_Sync: SPREAD_PLUS,
                    IsStable: false,
                    Type,
                    Delay: CURRENT.timedelay,
            };
        //    if(symbol === "NZDUSD") console.log(`=> Phát hiện Chậm Giá BUY: ${symbol} | Khoảng Cách: ${Payload.KhoangCach} | Time: ${timeStart}`,parseFloat(ASK_CR), "<", parseFloat(BID_CHECK - SPREAD_PLUS_POINT) ,KhoangCach);
            await Insert_UpdateAnalysisConfig(symbol,Payload);
        }

        // Check SELL
        
        // let Price_SELL_CURRENT = parseFloat(CURRENT.bid_mdf) - parseFloat(Point_Spread);
        // let Price_SELL_CHECK = parseFloat(CHECK.bid_mdf);

            

        if(parseFloat(BID_CR) > parseFloat(ASK_CHECK + SPREAD_PLUS_POINT)){
            const timeStart = getTimeGMT7();
            const KhoangCach = parseFloat((parseFloat(BID_CR) - parseFloat(ASK_CHECK + SPREAD_PLUS_POINT)))*parseFloat(Digit_Rec(parseInt(CHECK.digit))) ;
            const Payload = {
                    Broker: CURRENT.broker,
                    TimeStart: timeStart,
                    TimeCurrent: timeStart,
                    Symbol: symbol,
                    Count: 0,
                    Messenger: "SELL",
                    Broker_Main: CHECK.broker,
                    KhoangCach:parseInt(KhoangCach) ,
                    Symbol_Raw: CURRENT.symbol_raw,
                    Spread_main: CURRENT.spread,
                    Spread_Sync: SPREAD_PLUS,
                    IsStable: false,
                    Type,
                    Delay: CURRENT.timedelay,
            };
            // console.log(`=> Phát hiện Chậm Giá BUY: ${symbol} | Khoảng Cách: ${Payload.KhoangCach} | Time: ${timeStart}`);
            await Insert_UpdateAnalysisConfig(symbol,Payload);
        }
    }
    } catch (error) {
        console.error(`Lỗi Phân Tích Chậm Giá ${symbol}:`, error);
    }
}

module.exports = {Analysis };