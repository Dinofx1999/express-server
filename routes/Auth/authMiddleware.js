// middlewares/authMiddleware.js
const passport = require('passport');
const JwtStrategy = require('passport-jwt').Strategy;
const ExtractJwt = require('passport-jwt').ExtractJwt;
const jwt = require('jsonwebtoken');
const jwtSettings = require('../Module/Constants/jwtSetting');
const { userLogin } = require('../../models/index');

// Cấu hình JWT Strategy
const opts = {
  jwtFromRequest: ExtractJwt.fromAuthHeaderAsBearerToken(),
  secretOrKey: jwtSettings.SECRET,
  issuer: jwtSettings.ISSUER,
  audience: jwtSettings.AUDIENCE,
  algorithms: ['HS512']
};

passport.use(new JwtStrategy(opts, async (jwt_payload, done) => {
  try {
    // console.log('✅ JWT Payload:', jwt_payload); // Debug
    const user = await userLogin.findOne({ username: jwt_payload.sub });
    if (user) {
      return done(null, user);
    }
    console.log('❌ User not found:', jwt_payload.sub); // Debug
    return done(null, false);
  } catch (error) {
    console.log('❌ Strategy error:', error.message); // Debug
    return done(error, false);
  }
}));

// Middleware với error handling chi tiết
const authRequired = (req, res, next) => {
  const authHeader = req.headers['authorization'];
  
  // Debug: Log header
//   console.log('🔍 Auth Header:', authHeader);
  
  if (!authHeader) {
    return res.status(401).json({ 
      mess: 'Missing Authorization header', 
      code: 0 
    });
  }
  
  if (!authHeader.startsWith('Bearer ')) {
    return res.status(401).json({ 
      mess: 'Invalid token format. Use: Bearer <token>', 
      code: 0 
    });
  }
  
  const token = authHeader.slice(7);
  
  // Debug: Verify token manually trước
  try {
    const decoded = jwt.verify(token, jwtSettings.SECRET, {
      algorithms: ['HS512'],
      issuer: jwtSettings.ISSUER,
      audience: jwtSettings.AUDIENCE
    });
    // console.log('✅ Token valid:', decoded);
  } catch (err) {
    // console.log('❌ Token error:', err.message);
    return res.status(401).json({ 
      mess: `Token invalid: ${err.message}`, 
      code: 0 
    });
  }
  
  // Passport authenticate
  passport.authenticate('jwt', { session: false }, (err, user, info) => {
    if (err) {
      console.log('❌ Passport error:', err);
      return res.status(500).json({ mess: 'Server error', code: 0 });
    }
    if (!user) {
      console.log('❌ Passport info:', info);
      return res.status(401).json({ 
        mess: info?.message || 'Unauthorized', 
        code: 0 
      });
    }
    req.user = user;
    next();
  })(req, res, next);
};

module.exports = { authRequired };