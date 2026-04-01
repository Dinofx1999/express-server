'use strict';

var express = require('express');
var router = express.Router();
var { symbolAlias } = require('../../models/index');

// ===================== IN-MEMORY CACHE =====================
let _cache = new Map();
let _cacheTime = 0;
const CACHE_TTL_MS = 30_000;

async function _loadCache() {
  const now = Date.now();
  if (_cache.size > 0 && now - _cacheTime < CACHE_TTL_MS) return;

  const docs = await symbolAlias.find({ active: true }).lean();
  const fresh = new Map();

  for (const doc of docs) {
    fresh.set(doc.symbol.toUpperCase(), doc.symbol);
    for (const ali of doc.aliases || []) {
      fresh.set(String(ali).trim().toUpperCase(), doc.symbol);
    }
  }

  _cache = fresh;
  _cacheTime = now;
}

function _invalidateCache() {
  _cache = new Map();
  _cacheTime = 0;
}

// ===================== INTERNAL (dùng trong code khác) =====================
async function resolveSymbol(rawSymbol) {
  await _loadCache();
  const key = String(rawSymbol || '').trim().toUpperCase();
  return _cache.get(key) || null;
}

async function resolveSymbols(rawSymbols = []) {
  await _loadCache();
  const result = {};
  for (const s of rawSymbols) {
    const key = String(s || '').trim().toUpperCase();
    result[s] = _cache.get(key) || null;
  }
  return result;
}

// ===================== ROUTES =====================

// GET /all — lấy tất cả
router.get('/all', async function (req, res, next) {
  try {
    const { search = '', page = 1, limit = 50 } = req.query;

    const query = { active: true };
    if (search) {
      const regex = new RegExp(String(search).toUpperCase(), 'i');
      query.$or = [{ symbol: regex }, { aliases: regex }, { description: regex }];
    }

    const skip = (Number(page) - 1) * Number(limit);

    const [data, total] = await Promise.all([
      symbolAlias.find(query).sort({ symbol: 1 }).skip(skip).limit(Number(limit)).lean(),
      symbolAlias.countDocuments(query),
    ]);

    return res.status(200).json({ ok: true, total, page: Number(page), limit: Number(limit), data });
  } catch (error) {
    return res.status(500).json({ ok: false, mess: 'Lỗi server', error: error.message });
  }
});

// POST /create — tạo mới hoặc upsert
router.post('/create', async function (req, res, next) {
  try {
    const { symbol, aliases = [], description = '', createdBy = '' } = req.body;
    if (!symbol) return res.status(400).json({ ok: false, mess: 'symbol là bắt buộc' });

    const sym = String(symbol).toUpperCase().trim();
    const aliasArr = [...new Set(aliases.map(a => String(a).trim()).filter(Boolean))];

    // Kiểm tra alias trùng với symbol khác
    for (const ali of aliasArr) {
      const existed = await symbolAlias.findOne({ aliases: ali, active: true, symbol: { $ne: sym } });
      if (existed) {
        return res.status(400).json({
          ok: false,
          mess: `Alias "${ali}" đã được dùng bởi symbol "${existed.symbol}"`,
        });
      }
    }

    const doc = await symbolAlias.findOneAndUpdate(
      { symbol: sym },
      {
        symbol: sym,
        aliases: aliasArr,
        description: String(description).trim(),
        active: true,
        updatedBy: createdBy,
        $setOnInsert: { createdBy },
      },
      { upsert: true, new: true, setDefaultsOnInsert: true }
    );

    _invalidateCache();
    return res.status(200).json({ ok: true, mess: 'Tạo thành công', data: doc });
  } catch (error) {
    if (error.code === 11000) return res.status(400).json({ ok: false, mess: 'Symbol đã tồn tại' });
    return res.status(500).json({ ok: false, mess: 'Lỗi server', error: error.message });
  }
});

// POST /update — cập nhật aliases, description
router.post('/update', async function (req, res, next) {
  try {
    const { symbol, aliases, description, updatedBy = '' } = req.body;
    if (!symbol) return res.status(400).json({ ok: false, mess: 'symbol là bắt buộc' });

    const sym = String(symbol).toUpperCase().trim();
    const updateData = { updatedBy };

    if (Array.isArray(aliases)) {
      updateData.aliases = [...new Set(aliases.map(a => String(a).trim()).filter(Boolean))];
    }
    if (description !== undefined) updateData.description = String(description).trim();

    const doc = await symbolAlias.findOneAndUpdate(
      { symbol: sym },
      { $set: updateData },
      { new: true }
    );

    if (!doc) return res.status(404).json({ ok: false, mess: `Không tìm thấy symbol "${sym}"` });

    _invalidateCache();
    return res.status(200).json({ ok: true, mess: 'Cập nhật thành công', data: doc });
  } catch (error) {
    return res.status(500).json({ ok: false, mess: 'Lỗi server', error: error.message });
  }
});

// POST /add-alias — thêm 1 alias
router.post('/add-alias', async function (req, res, next) {
  try {
    const { symbol, alias, updatedBy = '' } = req.body;
    if (!symbol) return res.status(400).json({ ok: false, mess: 'symbol là bắt buộc' });
    if (!alias)  return res.status(400).json({ ok: false, mess: 'alias là bắt buộc' });

    const sym = String(symbol).toUpperCase().trim();
    const ali = String(alias).trim();

    // Kiểm tra alias trùng với symbol khác
    const existed = await symbolAlias.findOne({ aliases: ali, active: true, symbol: { $ne: sym } });
    if (existed) {
      return res.status(400).json({
        ok: false,
        mess: `Alias "${ali}" đã được dùng bởi symbol "${existed.symbol}"`,
      });
    }

    const doc = await symbolAlias.findOneAndUpdate(
      { symbol: sym },
      {
        $addToSet: { aliases: ali },
        $set: { updatedBy, active: true },
        $setOnInsert: { symbol: sym, createdBy: updatedBy },
      },
      { upsert: true, new: true }
    );

    _invalidateCache();
    return res.status(200).json({ ok: true, mess: 'Thêm alias thành công', data: doc });
  } catch (error) {
    return res.status(500).json({ ok: false, mess: 'Lỗi server', error: error.message });
  }
});

// POST /remove-alias — xóa 1 alias
router.post('/remove-alias', async function (req, res, next) {
  try {
    const { symbol, alias, updatedBy = '' } = req.body;
    if (!symbol) return res.status(400).json({ ok: false, mess: 'symbol là bắt buộc' });
    if (!alias)  return res.status(400).json({ ok: false, mess: 'alias là bắt buộc' });

    const sym = String(symbol).toUpperCase().trim();
    const ali = String(alias).trim();

    const doc = await symbolAlias.findOneAndUpdate(
      { symbol: sym },
      { $pull: { aliases: ali }, $set: { updatedBy } },
      { new: true }
    );

    if (!doc) return res.status(404).json({ ok: false, mess: `Không tìm thấy symbol "${sym}"` });

    _invalidateCache();
    return res.status(200).json({ ok: true, mess: 'Xóa alias thành công', data: doc });
  } catch (error) {
    return res.status(500).json({ ok: false, mess: 'Lỗi server', error: error.message });
  }
});

// POST /delete — soft delete
router.post('/delete', async function (req, res, next) {
  try {
    const { symbol } = req.body;
    if (!symbol) return res.status(400).json({ ok: false, mess: 'symbol là bắt buộc' });

    const sym = String(symbol).toUpperCase().trim();
    const doc = await symbolAlias.findOneAndUpdate(
      { symbol: sym },
      { $set: { active: false } },
      { new: true }
    );

    if (!doc) return res.status(404).json({ ok: false, mess: `Không tìm thấy symbol "${sym}"` });

    _invalidateCache();
    return res.status(200).json({ ok: true, mess: `Đã xóa symbol "${sym}"`, data: doc });
  } catch (error) {
    return res.status(500).json({ ok: false, mess: 'Lỗi server', error: error.message });
  }
});

// POST /resolve — resolve alias -> symbol gốc
router.post('/resolve', async function (req, res, next) {
  try {
    const { symbol, symbols } = req.body;

    // Batch
    if (Array.isArray(symbols)) {
      const result = await resolveSymbols(symbols);
      return res.status(200).json({ ok: true, data: result });
    }

    // Single
    if (symbol) {
      const resolved = await resolveSymbol(symbol);
      return res.status(200).json({ ok: true, symbol, resolved });
    }

    return res.status(400).json({ ok: false, mess: 'Cần truyền symbol hoặc symbols[]' });
  } catch (error) {
    return res.status(500).json({ ok: false, mess: 'Lỗi server', error: error.message });
  }
});

// POST /check-alias — kiểm tra alias đã tồn tại chưa
router.post('/check-alias', async function (req, res, next) {
  try {
    const { alias } = req.body;
    if (!alias) return res.status(400).json({ ok: false, mess: 'alias là bắt buộc' });

    const ali = String(alias).trim();
    const doc = await symbolAlias.findOne({ aliases: ali, active: true }).lean();

    return res.status(200).json({
      ok: true,
      exists: !!doc,
      symbol: doc ? doc.symbol : null,
    });
  } catch (error) {
    return res.status(500).json({ ok: false, mess: 'Lỗi server', error: error.message });
  }
});

// POST /cache-refresh — force refresh cache
router.post('/cache-refresh', async function (req, res, next) {
  try {
    _invalidateCache();
    await _loadCache();
    return res.status(200).json({ ok: true, mess: 'Cache đã được refresh', size: _cache.size });
  } catch (error) {
    return res.status(500).json({ ok: false, mess: 'Lỗi server', error: error.message });
  }
});

// GET /:symbol — lấy 1 symbol (để cuối tránh conflict với routes trên)
router.get('/:symbol', async function (req, res, next) {
  try {
    const sym = String(req.params.symbol || '').toUpperCase().trim();
    const doc = await symbolAlias.findOne({ symbol: sym, active: true }).lean();
    if (!doc) return res.status(404).json({ ok: false, mess: `Không tìm thấy symbol "${sym}"` });
    return res.status(200).json({ ok: true, data: doc });
  } catch (error) {
    return res.status(500).json({ ok: false, mess: 'Lỗi server', error: error.message });
  }
});

// ===================== EXPORTS =====================
module.exports = router;
module.exports.resolveSymbol  = resolveSymbol;  // dùng nội bộ