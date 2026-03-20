/* eslint-disable */
const {formatString, normSym} = require('../Helpers/text.format');
const {getTimeGMT7 ,getMinuteSecond } = require('../Helpers/time');
const {getSymbolInfo , getForexSession ,Digit , Digit_Rec} = require('../Jobs/Func.helper');
const Redis = require('../Redis/clientRedis');
const {Insert_UpdateAnalysisConfig} = require('../../Database/analysis-config.helper');

async function Analysis_Type3(data, symbol, symbolConfig_data, Delay_Stop, spread_plus) {
    try {
        let total_length = data.length;
        if(total_length <= 1) return; // Không đủ dữ liệu để phân tích
        
        // Nếu chỉ có 2 broker: so sánh 0 với 1
        if(total_length === 2) {
            // console.log(`[${symbol}] So sánh ${data[0].broker} với ${data[1].broker}`);
            await detectDelayPriceSignal({
                data,
                currentIndex: 1,
                checkIndex: 0,
                symbol,
                symbolConfig: symbolConfig_data,
                spreadPlus: spread_plus,
                delayStop: Delay_Stop
            });
            return;
        }
        
        // Nếu có > 2 broker: TẤT CẢ các sàn đều được làm CHECK
        // Nhưng mỗi sàn CHỈ so với 2 SÀN ĐẦU TIÊN (index 0 và 1)
        // 
        // Ví dụ [AXI, PEP, ICM, EXN]:
        // - AXI (i=0) so với: PEP (i=1), ICM (i=2)
        // - PEP (i=1) so với: AXI (i=0), ICM (i=2)
        // - ICM (i=2) so với: AXI (i=0), PEP (i=1)
        // - EXN (i=3) so với: AXI (i=0), PEP (i=1)
        
        for(let checkIndex = 0; checkIndex < total_length; checkIndex++) {
            // console.log(`[${symbol}] ===== Lấy ${data[checkIndex].broker} (i=${checkIndex}) làm chuẩn =====`);
            
            // Chỉ so sánh với 2 sàn đầu tiên (index 0 và 1)
            const compareIndices = [0, 1];
            
            for(let targetIndex of compareIndices) {
                // Bỏ qua nếu so sánh chính nó
                if(checkIndex === targetIndex) continue;
                
                // Bỏ qua nếu targetIndex vượt quá số lượng broker
                if(targetIndex >= total_length) continue;
                
                // console.log(`[${symbol}] So sánh ${data[targetIndex].broker} (i=${targetIndex}) với ${data[checkIndex].broker} (CHECK=${checkIndex})`);
                
                await detectDelayPriceSignal({
                    data,
                    currentIndex: targetIndex,
                    checkIndex: checkIndex,
                    symbol,
                    symbolConfig: symbolConfig_data,
                    spreadPlus: spread_plus,
                    delayStop: Delay_Stop
                });
            }
        }
        
    } catch (error) {
        console.error(`Lỗi Phân Tích Chậm Giá ${symbol}:`, error);
    }
}

/**
 * Phát hiện và insert signal vào DB
 */
async function detectDelayPriceSignal({
    data,
    currentIndex,
    checkIndex = 0,
    symbol,
    symbolConfig = null,
    spreadPlus = 1,
    delayStop = 0
}) {
    const CHECK = data[checkIndex];
    const CURRENT = data[currentIndex];
    
    const maxDelay = Number(process.env.MAX_NEGATIVE_DELAY) * 60;
    const delaySymbol = Number(CURRENT.timedelay);
    
    if (delaySymbol < maxDelay && 
        CURRENT.timecurrent + delayStop < CHECK.timecurrent && 
        CHECK.trade !== "TRUE") {
        return;
    }

    let spreadMinCurrent = Number(CURRENT.spread_mdf);
    let spreadXCurrent = Number(spreadPlus) || 1;
    let spreadXSession = 1;
    const session = getForexSession(getTimeGMT7());
    
    if (symbolConfig) {
        if (session === "Sydney") spreadXSession = symbolConfig.Sydney;
        if (session === "Tokyo") spreadXSession = symbolConfig.Tokyo;
        if (session === "London") spreadXSession = symbolConfig.London;
        if (session === "NewYork") spreadXSession = symbolConfig.NewYork;
        
        if (CURRENT.typeaccount === "STD" && spreadMinCurrent < symbolConfig.Spread_STD) {
            spreadMinCurrent = symbolConfig.Spread_STD;
        }
        if (CURRENT.typeaccount === "ECN" && spreadMinCurrent < symbolConfig.Spread_ECN) {
            spreadMinCurrent = symbolConfig.Spread_ECN;
        }
    }

    const digit = parseInt(CHECK.digit);
    const point = parseFloat(Digit(digit));
    const bidCheck = parseFloat(CHECK.bid_mdf);
    const askCheck = parseFloat(CHECK.ask_mdf);
    const spreadPlusCalc = parseFloat(spreadXCurrent * spreadXSession * spreadMinCurrent);
    const spreadPlusPoint = parseFloat(spreadPlusCalc * point);
    const askCurrent = parseFloat(CURRENT.ask_mdf);
    const bidCurrent = parseFloat(CURRENT.bid_mdf);

    const type = Number(CURRENT.timedelay) < 0 ? 'Delay Price Stop' : 'Delay Price';

    // Check BUY Signal
    if (askCurrent < (bidCheck - spreadPlusPoint)) {
        const khoangCach = parseFloat((bidCheck - spreadPlusPoint) - askCurrent) * 
                          parseFloat(Digit_Rec(digit));
        
        const timeStart = getTimeGMT7();
        const payload = {
            Broker: CURRENT.broker,
            TimeStart: timeStart,
            TimeCurrent: timeStart,
            Symbol: symbol,
            Count: 0,
            Messenger: "BUY",
            Broker_Main: CHECK.broker,
            KhoangCach: parseInt(khoangCach),
            Symbol_Raw: CURRENT.symbol_raw,
            Spread_main: CURRENT.spread,
            Spread_Sync: spreadPlusCalc,
            IsStable: false,
            Type: type,
            Delay: CURRENT.timedelay,
        };
        
        // console.log(`[${symbol}] => INSERT BUY: ${CURRENT.broker} chậm giá so với ${CHECK.broker}, khoảng cách: ${payload.KhoangCach}`);
        await Insert_UpdateAnalysisConfig(symbol, payload);
    }

    // Check SELL Signal
    if (bidCurrent > (askCheck + spreadPlusPoint)) {
        const khoangCach = parseFloat(bidCurrent - (askCheck + spreadPlusPoint)) * 
                          parseFloat(Digit_Rec(digit));
        
        const timeStart = getTimeGMT7();
        const payload = {
            Broker: CURRENT.broker,
            TimeStart: timeStart,
            TimeCurrent: timeStart,
            Symbol: symbol,
            Count: 0,
            Messenger: "SELL",
            Broker_Main: CHECK.broker,
            KhoangCach: parseInt(khoangCach),
            Symbol_Raw: CURRENT.symbol_raw,
            Spread_main: CURRENT.spread,
            Spread_Sync: spreadPlusCalc,
            IsStable: false,
            Type: type,
            Delay: CURRENT.timedelay,
        };
        
        // console.log(`[${symbol}] => INSERT SELL: ${CURRENT.broker} chậm giá so với ${CHECK.broker}, khoảng cách: ${payload.KhoangCach}`);
        await Insert_UpdateAnalysisConfig(symbol, payload);
    }
}

module.exports = {Analysis_Type3};