/* eslint-disable */
/* analysis-config.helper.js */
const { getCollection } = require('./mongodb');
const { log, colors } = require('../Module/Helpers/Log');
const GAP_RESET_SECONDS = process.env.GAP_RESET_SECONDS || 2;

function parseYMDHMS(str) {
  const m = /^(\d{4})\.(\d{2})\.(\d{2}) (\d{2}):(\d{2}):(\d{2})$/.exec(str);
  if (!m) return null;
  const [ , y, mo, d, h, mi, s ] = m.map(Number);
  return new Date(Date.UTC(y, mo - 1, d, h, mi, s));
}
function diffSec(a, b) {
  const A = parseYMDHMS(a);
  const B = parseYMDHMS(b);
  if (!A || !B) return null;
  return Math.round((B - A) / 1000);
}
const normalizeCount = (val) => {
  if (val === undefined || val === null) return undefined;
  const n = parseInt(val, 10);
  return Number.isFinite(n) ? n : 0;
};

async function Insert_UpdateAnalysisConfig(symbol, newConfig) {
    
  const collection = getCollection(String(process.env.ANALYSIS_DB) || 'analysis');
// console.log('In Insert_UpdateAnalysisConfig:', symbol, newConfig , collection);
  try {
    const Symbol = newConfig?.Symbol || symbol;
    if (!newConfig?.Broker || !Symbol) throw new Error('Broker và Symbol là bắt buộc.');
    if (!newConfig?.TimeCurrent) throw new Error('TimeCurrent là bắt buộc.');

    const filter = { Broker: newConfig.Broker, Symbol };
    const existing = await collection.findOne(filter);
    const incomingCount = normalizeCount(newConfig?.Count);

    // ===== INSERT: set TimeStart 1 lần duy nhất =====
    if (!existing) {
      const toInsert = {
        ...newConfig,
        Symbol,
        TimeStart: newConfig.TimeStart || newConfig.TimeCurrent,
        Count: incomingCount ?? 0,
        createdAt: new Date(),
        updatedAt: new Date(),
      };
      await collection.insertOne(toInsert);
      // log(colors.green, 'analysis', colors.cyan, `Inserted ${newConfig.Broker}:${Symbol}`);
      return toInsert;
    }

    // ===== UPDATE =====
    // 1) Skip duplicate / out-of-order
    if (existing.TimeCurrent === newConfig.TimeCurrent) {
      // log(colors.yellow, 'analysis', colors.cyan,`Skip ${newConfig.Broker}:${Symbol} | Duplicate TimeCurrent ${newConfig.TimeCurrent}`);
      return existing;
    }
    const gapToPrev = diffSec(existing.TimeCurrent, newConfig.TimeCurrent);
    if (gapToPrev == null || gapToPrev <= 0) {
      // log(colors.yellow, 'analysis', colors.cyan,`Skip ${newConfig.Broker}:${Symbol} | Non-forward TimeCurrent prev=${existing.TimeCurrent} new=${newConfig.TimeCurrent}`);
      return existing;
    }

    const prevCount = Number.isFinite(parseInt(existing.Count, 10)) ? parseInt(existing.Count, 10) : 0;

    // 2) Tính Count + xác định có reset không
    const isReset = gapToPrev > GAP_RESET_SECONDS;
    const nextCount = isReset ? 0 : (prevCount + 1);

    // 3) Build $set — KHÔNG bao giờ set TimeStart trừ khi reset
    const { TimeStart: _ignore, ...restPayload } = newConfig;
    const setDoc = {
      ...restPayload,            // các field khác (Messenger, Broker_Main, ...)
      Symbol,
      IsStable: existing.IsStable,
      TimeCurrent: newConfig.TimeCurrent,
      Count: nextCount,
      updatedAt: new Date(),
    };
    if (isReset) {
      setDoc.TimeStart = newConfig.TimeCurrent; // chỉ set khi reset
    }

    const { value: updated } = await collection.findOneAndUpdate(
      filter,
      { $set: setDoc },          // ← chỉ $set, không $setOnInsert để tránh conflict
      { returnDocument: 'after', upsert: false }
    );

    return updated;
  } catch (error) {
    log(colors.red, 'analysis', colors.cyan, `Lỗi Khi Thêm Mới Symbol Lỗi Vào Database : ${error.message}`);
    throw error;
  }
}

/**
 * Convert time string "YYYY.MM.DD HH:MM:SS" sang Date object
 */
function parseTimeString(timeStr) {
  // "2025.11.05 18:16:25" → "2025-11-05T18:16:25"
  const isoStr = timeStr.replace(/\./g, '-').replace(' ', 'T');
  return new Date(isoStr);
}

/**
 * Format Date object sang "YYYY.MM.DD HH:MM:SS"
 */
function formatTimeString(date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  const hours = String(date.getHours()).padStart(2, '0');
  const minutes = String(date.getMinutes()).padStart(2, '0');
  const seconds = String(date.getSeconds()).padStart(2, '0');
  
  return `${year}.${month}.${day} ${hours}:${minutes}:${seconds}`;
}

/**
 * Lấy analysis trong khoảng X giây trước (flexible)
 */
/**
 * Lấy analysis trong khoảng X giây trước với filters đầy đủ
 */
async function getAnalysisInTimeWindow(secondsAgo = 2, options = {}) {
  try {
    const collection = getCollection(String(process.env.ANALYSIS_DB) || 'analyses');
    
    const serverTime = new Date();
    const timeAgo = new Date(serverTime.getTime() - (secondsAgo * 1000));
    
    const maxTimeCurrent = formatTimeString(serverTime);
    const minTimeCurrent = formatTimeString(timeAgo);
    
    const limit = options.limit || 100;
    const skip = options.skip || 0;
    const additionalFilters = options.filters || {};
    
    // Build query
    const query = {
      TimeCurrent: {
        $gte: minTimeCurrent,
        $lte: maxTimeCurrent
      },
      ...additionalFilters
    };
    
    // ✅ Count filters
    if (options.minCount !== undefined || options.maxCount !== undefined) {
      query.Count = {};
      
      if (options.minCount !== undefined) {
        query.Count.$gt = options.minCount;
      }
      
      if (options.maxCount !== undefined) {
        query.Count.$lt = options.maxCount;
      }
      
      if (options.minCountInclusive !== undefined) {
        query.Count.$gte = options.minCountInclusive;
      }
      
      if (options.maxCountInclusive !== undefined) {
        query.Count.$lte = options.maxCountInclusive;
      }
    }
    
    // ✅ KhoangCach filters
    if (options.minKhoangCach !== undefined || options.maxKhoangCach !== undefined || options.excludeZeroKhoangCach) {
      query.KhoangCach = {};
      
      // KhoangCach > 0 (tự động thêm nếu có minCount hoặc excludeZeroKhoangCach = true)
      if (options.minCount !== undefined || options.excludeZeroKhoangCach === true) {
        query.KhoangCach.$gt = 0;
      }
      
      // Custom min KhoangCach
      if (options.minKhoangCach !== undefined) {
        query.KhoangCach.$gt = options.minKhoangCach;
      }
      
      // Custom max KhoangCach
      if (options.maxKhoangCach !== undefined) {
        query.KhoangCach.$lt = options.maxKhoangCach;
      }
      
      // KhoangCach >= min (inclusive)
      if (options.minKhoangCachInclusive !== undefined) {
        query.KhoangCach.$gte = options.minKhoangCachInclusive;
      }
      
      // KhoangCach <= max (inclusive)
      if (options.maxKhoangCachInclusive !== undefined) {
        query.KhoangCach.$lte = options.maxKhoangCachInclusive;
      }
    }
    
    // Custom sort
    const sort = options.sort || { Count: -1 };
    
    // log(
    //   colors.blue,
    //   `🔍 Query window: ${secondsAgo}s`,
    //   colors.cyan,
    //   `Query: ${JSON.stringify(query)}`
    // );
    
    const results = await collection
      .find(query)
      .sort(sort)
      .skip(skip)
      .limit(limit)
      .toArray();
    
    // log(colors.green, `✅ Found ${results.length} records`);
    
    return results;
    
  } catch (error) {
    log(colors.red, `❌ getAnalysisInTimeWindow error:`, error.message);
    throw error;
  }
}


module.exports = { Insert_UpdateAnalysisConfig ,getAnalysisInTimeWindow };
