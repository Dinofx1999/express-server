'use strict';

const mongoose = require('mongoose');

// ===================== SCHEMA =====================
const SymbolAliasSchema = new mongoose.Schema(
  {
    // Symbol gốc (chuẩn hóa) - ví dụ: "GBPUSD"
    symbol: {
      type: String,
      required: [true, 'symbol là bắt buộc'],
      uppercase: true,
      trim: true,
      unique: true,
    },

    // Danh sách alias/hậu tố
    // ví dụ: ["GBPUSD#", "GBPUSDpro", "GBPUSDp"]
    aliases: {
      type: [String],
      default: [],
    },

    // Mô tả
    description: {
      type: String,
      default: '',
      trim: true,
    },

    // Active / soft delete
    active: {
      type: Boolean,
      default: true,
    },

    // Người tạo (optional)
    createdBy: {
      type: String,
      default: '',
    },

    // Người cập nhật cuối
    updatedBy: {
      type: String,
      default: '',
    },
  },
  {
    timestamps: true,
    collection: 'symbol_aliases',
  }
);

// ===================== INDEXES =====================
SymbolAliasSchema.index({ symbol: 1 }, { unique: true });
SymbolAliasSchema.index({ aliases: 1 });
SymbolAliasSchema.index({ active: 1 });

// ===================== VIRTUALS =====================
SymbolAliasSchema.virtual('totalAliases').get(function () {
  return this.aliases ? this.aliases.length : 0;
});

// ===================== STATIC METHODS =====================

/**
 * Normalize symbol string
 */
SymbolAliasSchema.statics.normalize = function (raw) {
  return String(raw || '').toUpperCase().trim();
};

/**
 * Resolve alias -> symbol gốc
 * "GBPUSD#" -> "GBPUSD"
 * "GBPUSD"  -> "GBPUSD"
 * "UNKNOWN" -> null
 */
SymbolAliasSchema.statics.resolveSymbol = async function (raw) {
  const sym = String(raw || '').trim();
  if (!sym) return null;

  // Tìm trong aliases
  const byAlias = await this.findOne({ aliases: sym, active: true }).lean();
  if (byAlias) return byAlias.symbol;

  // Tìm chính xác symbol gốc
  const byRoot = await this.findOne({
    symbol: sym.toUpperCase(),
    active: true,
  }).lean();
  if (byRoot) return byRoot.symbol;

  return null;
};

/**
 * Resolve nhiều symbols cùng lúc (batch - load 1 lần duy nhất)
 * Input:  ["GBPUSD#", "EURUSDpro", "BTCUSD"]
 * Output: Map { "GBPUSD#" => "GBPUSD", "EURUSDpro" => "EURUSD", ... }
 */
SymbolAliasSchema.statics.resolveMany = async function (raws = []) {
  const syms = raws.map(s => String(s || '').trim()).filter(Boolean);
  if (!syms.length) return new Map();

  const docs = await this.find({ active: true }).lean();

  // Build lookup map
  const lookup = new Map();
  for (const doc of docs) {
    lookup.set(doc.symbol, doc.symbol);
    for (const ali of doc.aliases || []) {
      lookup.set(String(ali).trim(), doc.symbol);
    }
  }

  const result = new Map();
  for (const s of syms) {
    result.set(s, lookup.get(s) || lookup.get(s.toUpperCase()) || null);
  }
  return result;
};

/**
 * Thêm alias vào symbol gốc (upsert nếu chưa có symbol)
 */
SymbolAliasSchema.statics.addAlias = async function (symbol, alias, updatedBy = '') {
  const sym = String(symbol || '').toUpperCase().trim();
  const ali = String(alias || '').trim();
  if (!sym || !ali) throw new Error('symbol và alias không được rỗng');

  return this.findOneAndUpdate(
    { symbol: sym },
    {
      $addToSet: { aliases: ali },
      $set: { updatedBy, active: true },
      $setOnInsert: { symbol: sym, createdBy: updatedBy },
    },
    { upsert: true, new: true }
  );
};

/**
 * Xóa alias khỏi symbol gốc
 */
SymbolAliasSchema.statics.removeAlias = async function (symbol, alias, updatedBy = '') {
  const sym = String(symbol || '').toUpperCase().trim();
  const ali = String(alias || '').trim();
  if (!sym || !ali) throw new Error('symbol và alias không được rỗng');

  return this.findOneAndUpdate(
    { symbol: sym },
    {
      $pull: { aliases: ali },
      $set: { updatedBy },
    },
    { new: true }
  );
};

/**
 * Lấy tất cả aliases của symbol gốc
 */
SymbolAliasSchema.statics.getAliases = async function (symbol) {
  const doc = await this.findOne({
    symbol: String(symbol || '').toUpperCase().trim(),
    active: true,
  }).lean();
  return doc ? doc.aliases : [];
};

/**
 * Check alias đã tồn tại trong bất kỳ symbol nào chưa
 */
SymbolAliasSchema.statics.aliasExists = async function (alias) {
  const ali = String(alias || '').trim();
  const doc = await this.findOne({ aliases: ali, active: true }).lean();
  return doc
    ? { exists: true, symbol: doc.symbol }
    : { exists: false, symbol: null };
};

module.exports = mongoose.model('SymbolAlias', SymbolAliasSchema);

