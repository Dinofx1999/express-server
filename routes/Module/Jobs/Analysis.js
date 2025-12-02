/* eslint-disable */
const {formatString, normSym} = require('../Helpers/text.format');
const {getTimeGMT7} = require('../Helpers/time');
const {getSymbolInfo , getForexSession ,Digit , Digit_Rec} = require('../Jobs/Func.helper');
// const {Analysis} = require('../Jobs/Analysis');
const Redis = require('../Redis/clientRedis');
const {Insert_UpdateAnalysisConfig} = require('../../Database/analysis-config.helper');

 async function Analysis(data, symbol ,symbolConfig_data) {
    try {
        // console.log(`\n--- Analysis for ${symbol} at ${getTimeGMT7()} ---`);
    let total_length = data.length;
    for(let i = 1; i < total_length; i++){
        const CHECK = data[0];
        const CURRENT = data[i];
        let SPREAD_MIN_CURRENT = Number(CURRENT.spread_mdf);
        let SPREAD_X_CURRENT = Number(process.env.SPREAD_X_CURRENT) || 1.5;
        let SESSION = getForexSession(getTimeGMT7());
        // console.log(data[i]);
        // console.log(`Session: ${SESSION} | Symbol: ${symbol} | Broker: ${CURRENT.Brokerroker} | Spread_Min: ${SPREAD_MIN_CURRENT} | Spread_X: ${SPREAD_X_CURRENT}`);
        if(symbolConfig_data){
            if(SESSION === "Sydney") SPREAD_X_CURRENT = symbolConfig_data.Sydney;
            if(SESSION === "Tokyo") SPREAD_X_CURRENT = symbolConfig_data.Tokyo;
            if(SESSION === "London") SPREAD_X_CURRENT = symbolConfig_data.London;
            if(SESSION === "NewYork") SPREAD_X_CURRENT = symbolConfig_data.NewYork;
            if( CURRENT.Typeaccount === "STD") SPREAD_MIN_CURRENT = symbolConfig_data.Spread_STD;
            if( CURRENT.Typeaccount === "ECN") SPREAD_MIN_CURRENT = symbolConfig_data.Spread_ECN;
        }
        

        // if(symbol === "BTCUSD" && CURRENT.Broker === "FXBIG")console.log(`Session: ${SESSION} |Spread_Min: ${SPREAD_MIN_CURRENT} | Spread_X: ${SPREAD_X_CURRENT} | CURRENT.typeaccount : ${CURRENT.typeaccount }`);
        // if(symbol === "BTCUSD" && CURRENT.Broker === "FXBIG")console.log(CURRENT);

        //Check BUY
        let Spread_Sync = parseFloat(SPREAD_MIN_CURRENT * SPREAD_X_CURRENT);
        let Point_BUY = Spread_Sync * (Digit(parseInt(CHECK.digit)));
        let Price_BUY_CURRENT = parseFloat(CURRENT.ask_mdf) + parseFloat(Point_BUY);
        let Price_BUY_CHECK = parseFloat(CHECK.bid_mdf);

        //Type
        let Type = 'Delay Price';
        if(Number(data[i].timedelay)<0)
            Type = 'Delay Price Stop';

    //   if(symbol === "XAUUSD") console.log(data[i]);
        if(parseFloat(Price_BUY_CURRENT) < parseFloat(Price_BUY_CHECK)){
            const timeStart = getTimeGMT7();
            
            const Payload = {
                    Broker: CURRENT.Broker,
                    TimeStart: timeStart,
                    TimeCurrent: timeStart,
                    Symbol: symbol,
                    Count: 0,
                    Messenger: "BUY",
                    Broker_Main: CHECK.Broker,
                    KhoangCach: parseInt((Price_BUY_CHECK - Price_BUY_CURRENT)*parseFloat(Digit_Rec(parseInt(CHECK.digit)))) ,
                    Symbol_Raw: CURRENT.symbol_raw,
                    Spread_main: CURRENT.spread,
                    Spread_Sync: Spread_Sync,
                    IsStable: false,
                    Type,
            };
            // console.log(`=> Phát hiện Chậm Giá BUY: ${symbol} | Khoảng Cách: ${Payload.KhoangCach} | Time: ${timeStart}`);
            await Insert_UpdateAnalysisConfig(symbol,Payload);
        }


        //Check SELL
        let Point_SELL = Spread_Sync * parseFloat(Digit(parseInt(CHECK.digit)));
        let Price_SELL_CURRENT = parseFloat(CURRENT.bid_mdf - Point_SELL);
        let Price_SELL_CHECK = parseFloat(CHECK.bid_mdf) + parseFloat(Spread_Sync);

        // if(symbol === "XAUUSD") console.log(`Point_SELL: ${Point_SELL} ~ ${CURRENT.bid_mdf} - ${Point_SELL}  = Price_SELL_CURRENT: ${Price_SELL_CURRENT} > Price_SELL_CHECK: ${Price_SELL_CHECK}`);

        if(parseFloat(Price_SELL_CURRENT) > parseFloat(Price_SELL_CHECK)){
            const timeStart = getTimeGMT7();
            const Payload = {
                    Broker: CURRENT.Broker,
                    TimeStart: timeStart,
                    TimeCurrent: timeStart,
                    Symbol: symbol,
                    Count: 0,
                    Messenger: "SELL",
                    Broker_Main: CHECK.Broker,
                    KhoangCach: parseInt((Price_SELL_CURRENT - Price_SELL_CHECK)*parseFloat(Digit_Rec(parseInt(CHECK.digit)))) ,
                    Symbol_Raw: CURRENT.symbol_raw,
                    Spread_main: CURRENT.spread,
                    Spread_Sync: Spread_Sync,
                    IsStable: false,
                    Type,
            };
            // console.log(`=> Phát hiện Chậm Giá BUY: ${symbol} | Khoảng Cách: ${Payload.KhoangCach} | Time: ${timeStart}`);
            await Insert_UpdateAnalysisConfig(symbol,Payload);
        }
    }
    } catch (error) {
        console.error(`Lỗi Phân Tích Chậm Giá ${symbol}:`, error);
    }
    
    // Perform analysis logic here
}

module.exports = {Analysis };