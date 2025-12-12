const express = require('express');
const router = express.Router();
const {userLogin} = require("../../models/index");
const { validateSchema } = require('../../validations/validateSchema');
const { loginSchema, registerSchema } = require('../../validations/schemas.yup');
const {log , colors} = require('../Module/Helpers/Log');
const crypto = require('crypto');
const { authRequired, requireRole } = require('../Auth/authMiddleware');

//HTTP Basic Auth
const passport = require('passport');
const BasicStrategy = require('passport-http').BasicStrategy;
const jwt = require('jsonwebtoken');
//JWT Authentication
const JwtStrategy = require('passport-jwt').Strategy;
const ExtractJwt = require('passport-jwt').ExtractJwt; //Giải mã ngược Token
//JWT Setting
const jwtSettings = require('../Module/Constants/jwtSetting');
const {  API_ALL_INFO_BROKERS , 
          VERSION,
          API_PORT_BROKER_ENDPOINT, 
          API_RESET , 
          API_RESET_ALL_ONLY_SYMBOL ,
          API_CONFIG_SYMBOL , 
          API_ANALYSIS_CONFIG , API_PRICE_SYMBOL ,
          API_RESET_ALL_BROKERS , API_GET_CONFIG_SYMBOL ,API_LOGIN,API_REGISTER } = require('../Module/Constants/API.Service');


function generateSecretId(length = 10) {
  return crypto.randomBytes(Math.ceil(length / 2))
  .toString('hex')
  .slice(0, length)
  .toUpperCase();
}

router.post(API_REGISTER, validateSchema(registerSchema), async function (req, res, next) {
    try {
        req.body.username = req.body.username.toLowerCase();
        
        // ═══ THÊM id_SECRET ═══
        req.body.id_SECRET = generateSecretId(10);
        
        const newUser = new userLogin(req.body);
        await newUser.save();
        
        log(colors.green, 'REGISTER', colors.cyan, `Người dùng mới đã được đăng ký: ${req.body.name} , Username: ${req.body.username} , Email: ${req.body.email} , ID_SECRET: ${req.body.id_SECRET}`);
        return res.status(200).send({ success: true, message: "Đăng ký thành công" });
    } catch (err) {
        if (err.code === 11000) { 
            const duplicateField = Object.keys(err.keyPattern)[0];
            log(colors.red, 'REGISTER', colors.yellow, `${duplicateField} đã tồn tại: ${req.body.name} , Username: ${req.body.username} , Email: ${req.body.email}`);
            return res.status(409).send({ ok: false, message: `${duplicateField} đã tồn tại` });
        }
        console.error("Lỗi:", err);
        return res.status(500).send({ ok: false, message: "Đã xảy ra lỗi" });
    }
});

// ===================== UPDATE USER (ADMIN) =====================
router.put('/account-user/:id', authRequired, async function (req, res) {
  try {
    const { id } = req.params;

    // Những field cho phép update
    const allowFields = ['name', 'email', 'rule', 'actived', 'password', 'username'];
    const updateData = {};

    for (const key of allowFields) {
      if (req.body[key] !== undefined) updateData[key] = req.body[key];
    }

    // Normalize username nếu có
    if (updateData.username) updateData.username = String(updateData.username).toLowerCase().trim();
    if (updateData.email) updateData.email = String(updateData.email).toLowerCase().trim();

    // Không cho update các field nhạy cảm
    delete updateData.id_SECRET;
    delete updateData._id;

    // Check tồn tại user
    const user = await userLogin.findById(id);
    if (!user) {
      return res.status(404).json({ ok: false, message: 'User không tồn tại' });
    }

    // Check trùng username/email (nếu đổi)
    if (updateData.username && updateData.username !== user.username) {
      const existsUsername = await userLogin.findOne({ username: updateData.username, _id: { $ne: id } });
      if (existsUsername) return res.status(409).json({ ok: false, message: 'username đã tồn tại' });
    }

    if (updateData.email && updateData.email !== user.email) {
      const existsEmail = await userLogin.findOne({ email: updateData.email, _id: { $ne: id } });
      if (existsEmail) return res.status(409).json({ ok: false, message: 'email đã tồn tại' });
    }

    // Update
    Object.assign(user, updateData);
    await user.save();

    return res.json({
      ok: true,
      message: 'Cập nhật tài khoản thành công',
      data: {
        id: user._id,
        username: user.username,
        email: user.email,
        role: user.rule,
        fullname: user.name,
        actived: user.actived,
        id_SECRET: user.id_SECRET
      }
    });
  } catch (err) {
    console.error('Lỗi update user:', err);
    return res.status(500).json({ ok: false, message: 'Lỗi server' });
  }
});


// ===================== UPDATE ME (SELF) =====================
router.put('/account-user/me', authRequired, async function (req, res) {
  try {
    // Tuỳ authMiddleware của bạn: thường req.user.sub hoặc req.user.username
    const usernameFromToken = req.user?.sub || req.user?.username;
    if (!usernameFromToken) {
      return res.status(401).json({ ok: false, message: 'Token không hợp lệ' });
    }

    // Field user được phép tự sửa (KHÔNG cho sửa role/actived)
    const allowFields = ['name', 'email', 'password'];
    const updateData = {};

    for (const key of allowFields) {
      if (req.body[key] !== undefined) updateData[key] = req.body[key];
    }

    if (updateData.email) updateData.email = String(updateData.email).toLowerCase().trim();
    delete updateData.id_SECRET;

    const user = await userLogin.findOne({ username: usernameFromToken });
    if (!user) return res.status(404).json({ ok: false, message: 'User không tồn tại' });

    // Check trùng email nếu đổi
    if (updateData.email && updateData.email !== user.email) {
      const existsEmail = await userLogin.findOne({ email: updateData.email, _id: { $ne: user._id } });
      if (existsEmail) return res.status(409).json({ ok: false, message: 'email đã tồn tại' });
    }

    Object.assign(user, updateData);
    await user.save();

    return res.json({
      ok: true,
      message: 'Cập nhật thông tin thành công',
      data: {
        id: user._id,
        username: user.username,
        email: user.email,
        role: user.rule,
        fullname: user.name,
        actived: user.actived,
        id_SECRET: user.id_SECRET
      }
    });
  } catch (err) {
    console.error('Lỗi update me:', err);
    return res.status(500).json({ ok: false, message: 'Lỗi server' });
  }
});



  router.get('/basic', passport.authenticate('basic', { session: false }), function (req, res, next) {
    res.json({ ok: true });
  });
  
 router.get('/account-user',authRequired, async function (req, res, next) {
  try {
    const data = await userLogin.find({}).lean(); // .lean() cho nhẹ & trả về object thuần

    // console.log(data); // lúc này là mảng user đúng nghĩa
    res.json({ ok: true, data });
  } catch (err) {
    console.error("Lỗi /account-user:", err);
    res.status(500).json({ ok: false, message: "Lỗi server khi lấy danh sách user" });
  }
});

  router.post(API_LOGIN, async (req, res, next) => {
  req.body.username = req.body.username.toLowerCase();
  const { username, password } = req.body;

  try {
    const userExists = await userLogin.findOne({ username, password });

    // ❌ Không tồn tại user
    if (!userExists) {
      return res.status(401).send({ success: false, message: 'Tài Khoản không tồn tại' });
    }

    // ❌ Chưa active
    if (userExists.actived !== true) {
      return res.status(403).send({
        success: false,
        message: 'Tài khoản chưa được kích hoạt, vui lòng liên hệ quản trị viên!'
      });
    }

    // ✅ Đã active → cấp token
    const payload = {
      message: 'payload',
      sub: username,
      iat: Date.now(),
      name: userExists.name,
    };

    const secret = jwtSettings.SECRET;

    // ACCESS TOKEN
    const accessToken = jwt.sign(payload, secret, {
      expiresIn: 24 * 60 * 60, // 24h
      audience: jwtSettings.AUDIENCE,
      issuer: jwtSettings.ISSUER,
      algorithm: 'HS512',
    });

    // REFRESH TOKEN
    const refreshToken = jwt.sign(
      { username },
      secret,
      { expiresIn: '365d' }
    );

    const user = userExists;

    res.send({
      success: true,
      accessToken,
      refreshToken,
      user: {
        username: user.username,
        email: user.email,
        role: user.rule,
        fullname: user.name,
        id_SECRET: user.id_SECRET
      }
    });

  } catch (error) {
    console.error('[LOGIN ERROR]', error);
    res.status(500).send({ message: 'Login failed -> error!' });
  }
});



  router.get('/login', passport.authenticate('jwt', { session: false }), function (req, res, next) {
    console.log(req.rawHeaders[1]);
    const str = req.rawHeaders[1];
    const token = str.split(" ")[1];
    const secretKey = jwtSettings.SECRET;
try {
  // Giải mã token
  const decoded = jwt.verify(token, secretKey, { algorithms: ['HS512'] });
  res.json(decoded);
} catch (error) {
  res.json('Error decoding JWT:', error.message);
}
  });

router.get('/get-name-by-secret/:id_SECRET',authRequired, async function (req, res) {
  try {
    const { id_SECRET } = req.params;

    const user = await userLogin.findOne({ id_SECRET }).lean();

    if (!user) {
      return res.status(404).json({
        ok: false,
        message: "Không tìm thấy user với id_SECRET này"
      });
    }

    return res.json({
      ok: true,
      name: user.name,
      username: user.username,
      email: user.email,
      role: user.rule
    });

  } catch (err) {
    console.error("Lỗi /get-name-by-secret:", err);
    return res.status(500).json({
      ok: false,
      message: "Lỗi server"
    });
  }
});

module.exports = router;