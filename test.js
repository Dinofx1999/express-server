const helper = require("./routes/Module/Redis/redis.helper2");

(async () => {
  const r = helper.getRedis();

  const serverInfo = await r.info("server");
  const runIdLine = serverInfo.split("\n").find((l) => l.startsWith("run_id:"));
  console.log("[NODE] redis", runIdLine);

  await r.set("redis_helper2_test", Date.now());
  console.log("[NODE] SET redis_helper2_test OK");
  process.exit(0);
})();