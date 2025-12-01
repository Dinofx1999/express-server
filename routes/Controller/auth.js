const express = require('express');
const router = express.Router();
const {userLogin} = require("../../models/index");
const { validateSchema } = require('../../validations/validateSchema');
const { loginSchema, registerSchema } = require('../../validations/schemas.yup');
const {log , colors} = require('../Module/Helpers/Log');

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

router.post(API_REGISTER, validateSchema(registerSchema), async function (req, res, next) {
    try {
      req.body.username = req.body.username.toLowerCase();
      const newUser = new userLogin(req.body);
      await newUser.save();
      log(colors.green, 'REGISTER', colors.cyan, `Người dùng mới đã được đăng ký: ${req.body.name} , Username: ${req.body.username} , Email: ${req.body.email}`);
      return res.status(200).send({ success: true, message: "Đăng ký thành công" });
    } catch (err) {
      if (err.code === 11000) { 
        // Kiểm tra trường bị trùng lặp
        const duplicateField = Object.keys(err.keyPattern)[0];
        log(colors.red, 'REGISTER', colors.yellow, `${duplicateField} đã tồn tại: ${req.body.name} , Username: ${req.body.username} , Email: ${req.body.email}`);
        return res.status(409).send({ ok: false, message: `${duplicateField} đã tồn tại` });

      }
      console.error("Lỗi:", err);
      return res.status(500).send({ ok: false, message: "Đã xảy ra lỗi" });
    }
  });


  router.get('/basic', passport.authenticate('basic', { session: false }), function (req, res, next) {
    res.json({ ok: true });
  });
  
  router.get('/test', function (req, res, next) {
    console.log("OK");
    res.json({ ok: true });
  });

  router.post(API_LOGIN, async (req, res, next) => {
    req.body.username = req.body.username.toLowerCase();
    const { username, password } = req.body;
    
    try {
        const userExists = await userLogin.findOne({ username, password });
            if (userExists) {
                 // Cấp token
                // jwt
                const payload = {
                    message: 'payload',
                    sub: username,
                    iat: Date.now(),
                    name: userExists.name,
                };
            
                const secret = jwtSettings.SECRET;
            
                // ACCESS TOKEN
                const accessToken = jwt.sign(payload, secret, {
                    expiresIn: 24 * 60 * 60, // 24 giờ
                    audience: jwtSettings.AUDIENCE,
                    issuer: jwtSettings.ISSUER,
                    algorithm: 'HS512',
                });
            
                // REFRESH TOKEN
                const refreshToken = jwt.sign({ username }, secret, { expiresIn: '365d' });
                const user =  userExists;
                 const data = {
                  success: true,
                  accessToken,
                  refreshToken,
                  user: {
                    username: user.username,
                    email: user.email,
                    role: user.role,
                    fullname: user.fullname
                  }
                };
                
                res.send(data);
            } else {
                res.status(401).send({ message: 'Login failed!' });
            }
    } catch (error) {
        res.status(401).send({ message: 'Login failed -> error!' });
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


module.exports = router;