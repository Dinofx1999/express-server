const Redis = require('ioredis');
const { log, colors } = require('../Helpers/Log');

// ✅ REDIS CLIENT SINGLETON
const redisConfig = {
  host: process.env.REDIS_HOST || 'localhost',
  port: process.env.REDIS_PORT || 6379,
  password: process.env.REDIS_PASSWORD || undefined,
  db: process.env.REDIS_DB || 0,
  
  // Connection settings
  connectTimeout: 10000,
  keepAlive: 30000,
  enableReadyCheck: true,
  enableOfflineQueue: true,
  
  // Performance
  enableAutoPipelining: true,
  autoPipeliningIgnoredCommands: [],
  
  // Retry strategy
  retryStrategy: (times) => {
    if (times > 10) {
      log(colors.red, 'REDIS', colors.reset, '❌ Max retries reached');
      return null; // Stop retrying
    }
    const delay = Math.min(times * 100, 3000);
    log(colors.yellow, 'REDIS', colors.reset, `Retry ${times} in ${delay}ms`);
    return delay;
  },
  
  // Reconnect on error
  reconnectOnError: (err) => {
    const targetErrors = ['READONLY', 'ECONNRESET', 'ETIMEDOUT'];
    return targetErrors.some(e => err.message.includes(e));
  },
  
  maxRetriesPerRequest: 3,
};

// Create Redis client
const redisClient = new Redis(redisConfig);

// Event handlers
redisClient.on('connect', () => {
  log(colors.green, 'REDIS', colors.reset, 'Connecting...');
});

redisClient.on('ready', () => {
  log(colors.green, 'REDIS', colors.reset, '✅ Connected and ready');
});

redisClient.on('error', (err) => {
  console.error('❌ Redis Error:', err.message);
});

redisClient.on('close', () => {
  log(colors.yellow, 'REDIS', colors.reset, '⚠️ Connection closed');
});

redisClient.on('reconnecting', (delay) => {
  log(colors.yellow, 'REDIS', colors.reset, `Reconnecting in ${delay}ms...`);
});

redisClient.on('end', () => {
  log(colors.red, 'REDIS', colors.reset, '❌ Connection ended');
});

// Graceful shutdown
process.on('SIGINT', async () => {
  log(colors.yellow, 'REDIS', colors.reset, 'Closing Redis connection...');
  await redisClient.quit();
  process.exit(0);
});

process.on('SIGTERM', async () => {
  log(colors.yellow, 'REDIS', colors.reset, 'Closing Redis connection...');
  await redisClient.quit();
  process.exit(0);
});

module.exports = redisClient;