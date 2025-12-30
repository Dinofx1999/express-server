// redis.broker.meta.js
const Redis = require("ioredis");
const redis = new Redis({ host: "127.0.0.1", port: 6379, db: 0 });

async function updateBrokerMetaFromRaw(rawData) {
  if (!rawData || typeof rawData !== "object") return;

  const broker_ = String(rawData.broker_ || rawData.broker || "")
    .trim()
    .toLowerCase();
  if (!broker_) return;

  const meta = {
    index: String(rawData.index ?? ""),
    broker: String(rawData.broker ?? ""),    
    broker_: String(rawData.broker_ ?? ""),
    version: String(rawData.version ?? ""),
    totalsymbol: String(rawData.totalsymbol ?? ""),
    timecurent: String(rawData.timecurent ?? ""),
    status: String(rawData.status ?? ""),
    timeUpdated: String(rawData.timeUpdated ?? ""),
    port: String(rawData.port ?? ""),
    typeaccount: String(rawData.typeaccount ?? ""),
    auto_trade: String(rawData.auto_trade ?? ""),
    receivedAt: String(Date.now())
  };

  await redis.hmset(`broker_meta:${broker_}`, meta);
}
module.exports = { updateBrokerMetaFromRaw };
