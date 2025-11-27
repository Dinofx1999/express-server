const Redis = require('ioredis');
const {log , colors} = require('../Helpers/Log');
const redisClient = new Redis({
    host: 'localhost',
    port: 6379,
    // password: 'yourpassword', // Nếu có mật khẩu
});

redisClient.on('connect', () => {
    log(colors.green, 'REDIS', colors.reset, 'Redis connected');
});

redisClient.on('error', (err) => {
    log(colors.red, 'REDIS', colors.reset, 'Redis connection error', err);
});

module.exports = redisClient;