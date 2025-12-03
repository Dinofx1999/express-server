/* eslint-disable */
const { formatString, normSym } = require('../Helpers/text.format');
const { getTimeGMT7 } = require('../Helpers/time');
const { getSymbolInfo, getForexSession, Digit, Digit_Rec } = require('../Jobs/Func.helper');
const Redis = require('../Redis/clientRedis');
const { Insert_UpdateAnalysisConfig } = require('../../Database/analysis-config.helper');

// ═══════════════════════════════════════════════════════════
// CONFIG
// ═══════════════════════════════════════════════════════════
const OUTLIER_THRESHOLD_PIPS = 5;  // Ngưỡng phát hiện broker lệch (pips)

/**
 * Hàm phân tích chính
 * @param {Array} data - Danh sách giá từ các broker
 * @param {string} symbol - Symbol (VD: EURUSD)
 * @param {Object} symbolConfig_data - Config theo symbol
 */
async function Analysis_Type2(data, symbol, symbolConfig_data) {
    try {
        // Kiểm tra dữ liệu
        if (!data || data.length < 2) return;

        const digit = parseInt(data[0].digit);
        const pipValue = Digit(digit);

        // ═══════════════════════════════════════════════════════════
        // 1. TÌM NHÓM CHUẨN (MEDIAN) THAY VÌ LẤY BROKER ĐẦU TIÊN
        // ═══════════════════════════════════════════════════════════
        const { mainGroup, outlierGroup, medianBid, medianAsk, closestBroker } = findMainGroup(data, pipValue);

        // Nếu không đủ broker trong nhóm chuẩn → bỏ qua
        if (mainGroup.length < 2) {
            return;
        }

        // Nếu không có broker lệch → không cần phân tích
        if (outlierGroup.length === 0) {
            return;
        }

        // ═══════════════════════════════════════════════════════════
        // 2. TẠO BROKER CHUẨN TỪ MEDIAN
        //    Sử dụng broker gần median nhất làm đại diện
        // ═══════════════════════════════════════════════════════════
        const CHECK = {
            ...closestBroker,
            bid_mdf: medianBid,
            ask_mdf: medianAsk,
            Broker: closestBroker.Broker,  // Dùng tên broker thật
        };

        // ═══════════════════════════════════════════════════════════
        // 3. CHỈ PHÂN TÍCH CÁC BROKER BỊ LỆCH
        // ═══════════════════════════════════════════════════════════
        for (const CURRENT of outlierGroup) {
            await analyzeSignal(CHECK, CURRENT, symbol, symbolConfig_data, digit);
        }

    } catch (error) {
        console.error(`Lỗi Phân Tích Chậm Giá ${symbol}:`, error);
    }
}

/**
 * Tìm nhóm chuẩn (đa số) và nhóm lệch
 * @param {Array} data - Danh sách giá từ các broker
 * @param {number} pipValue - Giá trị 1 pip (VD: 0.00001 với digit=5)
 * @returns {Object} { mainGroup, outlierGroup, medianBid, medianAsk, closestBroker }
 */
function findMainGroup(data, pipValue) {
    // Lấy danh sách bid và ask
    const bids = data.map(d => parseFloat(d.bid_mdf));
    const asks = data.map(d => parseFloat(d.ask_mdf));

    // Tính median
    const medianBid = calculateMedian(bids);
    const medianAsk = calculateMedian(asks);

    // Phân loại: Chuẩn vs Lệch
    const mainGroup = [];
    const outlierGroup = [];

    for (const broker of data) {
        const bidDiff = Math.abs(parseFloat(broker.bid_mdf) - medianBid);
        const diffPips = bidDiff / pipValue;

        if (diffPips > OUTLIER_THRESHOLD_PIPS) {
            // Broker lệch
            outlierGroup.push({
                ...broker,
                diffPips: diffPips.toFixed(1),
                direction: parseFloat(broker.bid_mdf) > medianBid ? 'HIGH' : 'LOW'
            });
        } else {
            // Broker chuẩn
            mainGroup.push(broker);
        }
    }

    // Tìm broker gần median nhất trong mainGroup
    let closestBroker = mainGroup[0];
    let minDiff = Infinity;

    for (const broker of mainGroup) {
        const diff = Math.abs(parseFloat(broker.bid_mdf) - medianBid);
        if (diff < minDiff) {
            minDiff = diff;
            closestBroker = broker;
        }
    }

    return { mainGroup, outlierGroup, medianBid, medianAsk, closestBroker };
}

/**
 * Tính median của mảng số
 * @param {Array} arr - Mảng số
 * @returns {number} Giá trị median
 */
function calculateMedian(arr) {
    const sorted = [...arr].sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    return sorted.length % 2 !== 0
        ? sorted[mid]
        : (sorted[mid - 1] + sorted[mid]) / 2;
}

/**
 * Phân tích tín hiệu BUY/SELL
 * @param {Object} CHECK - Broker chuẩn (median)
 * @param {Object} CURRENT - Broker đang phân tích (bị lệch)
 * @param {string} symbol - Symbol
 * @param {Object} symbolConfig_data - Config theo symbol
 * @param {number} digit - Số chữ số thập phân
 */
async function analyzeSignal(CHECK, CURRENT, symbol, symbolConfig_data, digit) {
    

    
    // if (Number(CURRENT.timedelay) < Number(process.env.MAX_NEGATIVE_DELAY) * 60 || (-30 * 60)) {
    //     // console.log(`[SKIP] ${symbol} | ${CURRENT.Broker} delay quá lớn: ${CURRENT.timedelay}ms`);
    //     return;  // Bỏ qua, không phân tích
    // }

    // Lấy config spread
    let SPREAD_MIN_CURRENT = Number(CURRENT.spread_mdf);
    let SPREAD_X_CURRENT = Number(process.env.SPREAD_X_CURRENT) || 1.5;
    let SESSION = getForexSession(getTimeGMT7());

    // Apply config theo session và loại tài khoản
    if (symbolConfig_data) {
        if (SESSION === "Sydney") SPREAD_X_CURRENT = symbolConfig_data.Sydney;
        if (SESSION === "Tokyo") SPREAD_X_CURRENT = symbolConfig_data.Tokyo;
        if (SESSION === "London") SPREAD_X_CURRENT = symbolConfig_data.London;
        if (SESSION === "NewYork") SPREAD_X_CURRENT = symbolConfig_data.NewYork;
        if (CURRENT.Typeaccount === "STD") SPREAD_MIN_CURRENT = symbolConfig_data.Spread_STD;
        if (CURRENT.Typeaccount === "ECN") SPREAD_MIN_CURRENT = symbolConfig_data.Spread_ECN;
    }

    // Xác định loại delay
    let Type = 'Delay Price';
    if (Number(CURRENT.timedelay) < 0) {
        Type = 'Delay Price Stop';
    }

    // Tính spread và point
    let Spread_Sync = parseFloat(SPREAD_MIN_CURRENT * SPREAD_X_CURRENT);
    let Point = Spread_Sync * Digit(digit);

    // ═══════════════════════════════════════════════════════════
    // CHECK BUY
    // ═══════════════════════════════════════════════════════════
    let Price_BUY_CURRENT = parseFloat(CURRENT.ask_mdf) + Point;
    let Price_BUY_CHECK = parseFloat(CHECK.bid_mdf);

    if (Price_BUY_CURRENT < Price_BUY_CHECK) {
        const timeStart = getTimeGMT7();
        const KhoangCach = parseInt((Price_BUY_CHECK - Price_BUY_CURRENT) * Digit_Rec(digit));

        const Payload = {
            Broker: CURRENT.Broker,
            TimeStart: timeStart,
            TimeCurrent: timeStart,
            Symbol: symbol,
            Count: 0,
            Messenger: "BUY",
            Broker_Main: CHECK.Broker,
            KhoangCach,
            Symbol_Raw: CURRENT.symbol_raw,
            Spread_main: CURRENT.spread,
            Spread_Sync: Spread_Sync,
            IsStable: false,
            Type,
            Delay: CURRENT.timedelay,
            // Thông tin về độ lệch
            // DiffPips: CURRENT.diffPips,
            // Direction: CURRENT.direction,
        };

        await Insert_UpdateAnalysisConfig(symbol, Payload);
    }

    // ═══════════════════════════════════════════════════════════
    // CHECK SELL
    // ═══════════════════════════════════════════════════════════
    let Price_SELL_CURRENT = parseFloat(CURRENT.bid_mdf) - Point;
    let Price_SELL_CHECK = parseFloat(CHECK.bid_mdf) + Spread_Sync;

    if (Price_SELL_CURRENT > Price_SELL_CHECK) {
        const timeStart = getTimeGMT7();
        const KhoangCach = parseInt((Price_SELL_CURRENT - Price_SELL_CHECK) * Digit_Rec(digit));

        const Payload = {
            Broker: CURRENT.Broker,
            TimeStart: timeStart,
            TimeCurrent: timeStart,
            Symbol: symbol,
            Count: 0,
            Messenger: "SELL",
            Broker_Main: CHECK.Broker,
            KhoangCach,
            Symbol_Raw: CURRENT.symbol_raw,
            Spread_main: CURRENT.spread,
            Spread_Sync: Spread_Sync,
            IsStable: false,
            Type,
            Delay: CURRENT.timedelay,
            // Thông tin về độ lệch
            // DiffPips: CURRENT.diffPips,
            // Direction: CURRENT.direction,
        };

        await Insert_UpdateAnalysisConfig(symbol, Payload);
    }
}

module.exports = { Analysis_Type2 };