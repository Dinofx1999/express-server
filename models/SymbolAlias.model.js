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

    // Số digit cao nhất trong tất cả các broker source của symbol này.
    // Server dùng giá trị này làm stdDigits khi normalize giá từ nhiều MT4.
    // Tự động cập nhật qua recalcMaxDigits() mỗi khi brokerDigits thay đổi.
    // Ví dụ: broker A digit 2, broker B digit 3 → maxDigits = 3
    maxDigits: {
      type: Number,
      default: 2,
      min: [1, 'maxDigits tối thiểu là 1'],
      max: [8, 'maxDigits tối đa là 8'],
    },

    // Digits của từng broker source, dùng để tự động tính lại maxDigits.
    // Map: { brokerName: digits }
    // Ví dụ: { "PEP MT5": 3, "FXPRIIMUS": 2, "EXCLUSIVE": 3 }
    brokerDigits: {
      type: Map,
      of: Number,
      default: {},
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
SymbolAliasSchema.index({ maxDigits: 1 });

// ===================== VIRTUALS =====================
SymbolAliasSchema.virtual('totalAliases').get(function () {
  return this.aliases ? this.aliases.length : 0;
});

// ===================== HOOKS =====================

/**
 * Tự động tính lại maxDigits từ brokerDigits trước khi save.
 * Đảm bảo maxDigits luôn = max của tất cả giá trị trong brokerDigits.
 * Nếu brokerDigits rỗng, giữ nguyên giá trị maxDigits hiện tại.
 */
SymbolAliasSchema.pre('save', function (next) {
  if (this.brokerDigits && this.brokerDigits.size > 0) {
    this.maxDigits = Math.max(...this.brokerDigits.values());
  }
  next();
});

SymbolAliasSchema.pre('findOneAndUpdate', function (next) {
  const update = this.getUpdate();
  // Nếu update có brokerDigits thì không tự tính được ở đây (không có doc đầy đủ).
  // recalcMaxDigits() phải được gọi thủ công sau khi setBrokerDigits().
  // Chỉ xử lý trường hợp set maxDigits trực tiếp để đảm bảo min/max.
  if (update?.$set?.maxDigits !== undefined) {
    update.$set.maxDigits = Math.min(8, Math.max(1, update.$set.maxDigits));
  }
  next();
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

  const byAlias = await this.findOne({ aliases: sym, active: true }).lean();
  if (byAlias) return byAlias.symbol;

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

/**
 * Cập nhật digits của 1 broker source, tự động tính lại maxDigits.
 *
 * @param {string} symbol     - Symbol gốc, ví dụ "WTI"
 * @param {string} brokerName - Tên broker, ví dụ "PEP MT5"
 * @param {number} digits     - Số digit của broker đó, ví dụ 3
 * @param {string} updatedBy
 *
 * @example
 * await SymbolAlias.setBrokerDigits('WTI', 'PEP MT5', 3, 'admin');
 * // brokerDigits: { "PEP MT5": 3, ... }
 * // maxDigits: tự động = max của tất cả brokerDigits
 */
SymbolAliasSchema.statics.setBrokerDigits = async function (
  symbol,
  brokerName,
  digits,
  updatedBy = ''
) {
  const sym = String(symbol || '').toUpperCase().trim();
  const name = String(brokerName || '').trim();
  const dig = parseInt(digits, 10);
  if (!sym || !name) throw new Error('symbol và brokerName không được rỗng');
  if (isNaN(dig) || dig < 1 || dig > 8) throw new Error('digits phải từ 1 đến 8');

  // Dùng $set trên Map field theo cú pháp dot-notation của Mongoose
  const doc = await this.findOneAndUpdate(
    { symbol: sym },
    {
      $set: {
        [`brokerDigits.${name}`]: dig,
        updatedBy,
        active: true,
      },
      $setOnInsert: { symbol: sym, createdBy: updatedBy },
    },
    { upsert: true, new: true }
  );

  // Tính lại maxDigits từ toàn bộ brokerDigits sau khi update
  return this.recalcMaxDigits(sym, updatedBy);
};

/**
 * Xóa digits của 1 broker source (khi broker offline/bị xóa),
 * tự động tính lại maxDigits.
 *
 * @param {string} symbol
 * @param {string} brokerName
 * @param {string} updatedBy
 */
SymbolAliasSchema.statics.removeBrokerDigits = async function (
  symbol,
  brokerName,
  updatedBy = ''
) {
  const sym = String(symbol || '').toUpperCase().trim();
  const name = String(brokerName || '').trim();
  if (!sym || !name) throw new Error('symbol và brokerName không được rỗng');

  await this.findOneAndUpdate(
    { symbol: sym },
    {
      $unset: { [`brokerDigits.${name}`]: '' },
      $set: { updatedBy },
    },
    { new: true }
  );

  return this.recalcMaxDigits(sym, updatedBy);
};

/**
 * Tính lại và lưu maxDigits = max(brokerDigits.values()).
 * Gọi sau mọi thao tác thay đổi brokerDigits.
 * Nếu brokerDigits rỗng, maxDigits giữ nguyên (không reset về 2).
 *
 * @param {string} symbol
 * @param {string} updatedBy
 * @returns {Promise<Document>} doc đã được cập nhật
 */
SymbolAliasSchema.statics.recalcMaxDigits = async function (symbol, updatedBy = '') {
  const sym = String(symbol || '').toUpperCase().trim();
  const doc = await this.findOne({ symbol: sym }).lean();
  if (!doc) return null;

  const digitsMap = doc.brokerDigits || {};
  const values = Object.values(digitsMap).map(Number).filter(n => !isNaN(n) && n >= 1);
  if (!values.length) return this.findOne({ symbol: sym });

  const newMax = Math.max(...values);
  return this.findOneAndUpdate(
    { symbol: sym },
    { $set: { maxDigits: newMax, updatedBy } },
    { new: true }
  );
};

/**
 * Lấy maxDigits hiện tại của symbol — dùng để broadcast stdDigits cho MT4.
 *
 * @param {string} symbol
 * @returns {Promise<number|null>}
 *
 * @example
 * const stdDigits = await SymbolAlias.getMaxDigits('WTI'); // 3
 */
SymbolAliasSchema.statics.getMaxDigits = async function (symbol) {
  const doc = await this.findOne({
    symbol: String(symbol || '').toUpperCase().trim(),
    active: true,
  })
    .select('maxDigits')
    .lean();
  return doc ? doc.maxDigits : null;
};

/**
 * Lấy maxDigits của nhiều symbol cùng lúc (batch).
 * Output: Map { "WTI" => 3, "GBPUSD" => 5, ... }
 *
 * @param {string[]} symbols
 * @returns {Promise<Map<string, number>>}
 */
SymbolAliasSchema.statics.getMaxDigitsMany = async function (symbols = []) {
  const syms = symbols.map(s => String(s || '').toUpperCase().trim()).filter(Boolean);
  if (!syms.length) return new Map();

  const docs = await this.find({ symbol: { $in: syms }, active: true })
    .select('symbol maxDigits')
    .lean();

  const result = new Map();
  for (const doc of docs) {
    result.set(doc.symbol, doc.maxDigits);
  }
  return result;
};

/**
 * Kiểm tra symbol tồn tại chưa, sau đó:
 *   - Chưa tồn tại  → thêm mới symbol với maxDigits = digits
 *   - Đã tồn tại    → chỉ cập nhật maxDigits nếu maxDigits hiện tại null hoặc < digits mới
 *
 * @param {string} symbol   - Symbol gốc, ví dụ "GBPUSD"
 * @param {number} digits   - Số digit muốn áp dụng, ví dụ 3
 * @param {string} createdBy / updatedBy
 * @returns {Promise<{ doc: Document, action: 'created' | 'updated' | 'skipped' }>}
 *   action:
 *     'created'  — symbol chưa tồn tại, đã tạo mới
 *     'updated'  — symbol đã tồn tại, maxDigits được nâng lên
 *     'skipped'  — symbol đã tồn tại, maxDigits hiện tại >= digits → giữ nguyên
 *
 * @example
 * // Trường hợp 1: GBPUSD chưa tồn tại
 * await SymbolAlias.upsertWithMaxDigits('GBPUSD', 3, 'admin');
 * // => { action: 'created', doc: { symbol: 'GBPUSD', maxDigits: 3, ... } }
 *
 * // Trường hợp 2: GBPUSD tồn tại, maxDigits = 2 < 3
 * await SymbolAlias.upsertWithMaxDigits('GBPUSD', 3, 'admin');
 * // => { action: 'updated', doc: { symbol: 'GBPUSD', maxDigits: 3, ... } }
 *
 * // Trường hợp 3: GBPUSD tồn tại, maxDigits = 3 >= 3
 * await SymbolAlias.upsertWithMaxDigits('GBPUSD', 3, 'admin');
 * // => { action: 'skipped', doc: { symbol: 'GBPUSD', maxDigits: 3, ... } }
 */
SymbolAliasSchema.statics.upsertWithMaxDigits = async function (
  symbol,
  digits,
  updatedBy = ''
) {
  const sym = String(symbol || '').toUpperCase().trim();
  const dig = parseInt(digits, 10);
  if (!sym) throw new Error('symbol không được rỗng');
  if (isNaN(dig) || dig < 0 || dig > 8) throw new Error(`digits phải từ 0 đến 8 digit = ${digits}`);

  const existing = await this.findOne({ symbol: sym }).lean();

  // Chưa tồn tại → tạo mới
  if (!existing) {
    const doc = await this.findOneAndUpdate(
      { symbol: sym },
      {
        $setOnInsert: {
          symbol: sym,
          maxDigits: dig,
          aliases: [],
          active: true,
          createdBy: updatedBy,
          updatedBy,
        },
      },
      { upsert: true, new: true }
    );
    return { doc, action: 'created' };
  }

  // Đã tồn tại — kiểm tra maxDigits hiện tại
  const currentMax = existing.maxDigits ?? null;
  const shouldUpdate = currentMax === null || currentMax < dig;

  if (!shouldUpdate) {
    return { doc: existing, action: 'skipped' };
  }

  // maxDigits null hoặc nhỏ hơn → nâng lên
  const doc = await this.findOneAndUpdate(
    { symbol: sym },
    { $set: { maxDigits: dig, updatedBy } },
    { new: true }
  );
  return { doc, action: 'updated' };
};

module.exports = mongoose.model('SymbolAlias', SymbolAliasSchema);