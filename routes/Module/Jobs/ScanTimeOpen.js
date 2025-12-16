const { getSymbolInfo } = require("../Jobs/Func.helper");
const Redis = require("../Redis/clientRedis");
const { Analysis } = require("../Jobs/Analysis");
const { Analysis_Type2 } = require("../Jobs/Analysis_Type2");
const { connectMongoDB } = require("../../Database/mongodb");
const { getAllSymbolConfigs } = require("../../Database/symbol-config.helper");
const { colors } = require("../Helpers/Log");
const { getTimeGMT7 } = require("../Helpers/time");
const SymbolDebounceQueue = require("../Redis/DebounceQueue");

const { configAdmin } = require("../../../models/index");

const queue = new SymbolDebounceQueue({
  debounceTime: 5000, // 5s không có payload mới
  maxWaitTime: 10000, // Tối đa 10s
  maxPayloads: 5000, // Tối đa 5000 unique payloads
  delayBetweenTasks: 500, // 500ms delay giữa các task
  cooldownTime: 10000, // 10s cooldown after processing
});

async function startJob() {
  console.log(`[JOB ${process.pid}] Scan Open Time booting...`);

  try {
    await connectMongoDB();
    console.log(`[JOB ${process.pid}] MongoDB connected.`);
  } catch (error) {
    console.error(
      `[JOB ${process.pid}] MongoDB connection failed:`,
      error.message
    );
    return;
  }

  // ===========================================
  // ✅ Thêm interval 1 giây (hoặc anh đổi số ms)
  // ===========================================

  const intervalMs = Number(process.env.CRON_INTERVAL_SCAN_OPEN || 1000);
  const interval_getConfig = Number(
    process.env.CRON_INTERVAL_GET_CONFIG || 1000
  );

  let isRunning = false;
  let isRunningConfig = false;

  //Get Time OPen để reset
  setInterval(async () => {
    if (isRunning) return; // 🔒 tránh overlap
    isRunning = true;
    const timeCurrent = getTimeGMT7("time"); // "HH:MM:SS" dạng string
    const config = await Redis.getConfigAdmin();

    if (!config) {
      console.error(`[JOB ${process.pid}] No config found`);
      isRunning = false;
      return;
    }

    const start = timeToNumber(config.TimeStopReset.start);
    const end = timeToNumber(config.TimeStopReset.end);
    const now = timeToNumber(timeCurrent);

    // Nếu now nằm TRONG khoảng ⇒ return dừng job
    if (now >= start && now <= end) {
      isRunning = false;
      return;
    }

    // ⇨ Nếu không nằm trong start–end thì job tiếp tục chạy
    try {
      await ScanTimeOpenSymbol();
    } catch (err) {
      console.error(`[JOB ${process.pid}] Scan error:`, err);
    }
    isRunning = false;
  }, intervalMs);

  //Get Config Admin
  setInterval(async () => {
    if (isRunningConfig) return; // 🔒 tránh overlap
    isRunningConfig = true;
    try {
      const config = await configAdmin.findOne();
      await Redis.saveConfigAdmin(config);
    } catch (err) {
      console.error(`[JOB ${process.pid}] Scan error:`, err);
    }

    isRunningConfig = false;
  }, interval_getConfig);

  console.log(
    `[JOB ${process.pid}] Scan Open Time ready. Interval: ${intervalMs} ms`
  );
}

function timeToNumber(t) {
  return Number(t.replaceAll(":", ""));
}

async function ScanTimeOpenSymbol() {
  // 1️⃣ Lấy danh sách symbols
  const ALL_Symbol = await Redis.getAllUniqueSymbols();

  const symbols = ALL_Symbol.map((s) => String(s).trim().toUpperCase()).filter(
    Boolean
  );

  // 2️⃣ Lấy TẤT CẢ price data 1 lần
  const priceDataMap = await Redis.getMultipleSymbolDetails(symbols);

  await Promise.all(
    symbols.map(async (sym) => {
      try {
        const priceData = priceDataMap.get(sym);
        priceData.map(async (data) => {
          try {
            if (!data) {
              console.log(`[ScanTimeOpen] No symbol info for ${sym}`);
              return;
            }
            const timetrade = data.timetrade || [];
            const timeCr = data.timecurrent || "";
            const last_reset = data.last_reset || "";
            const broker = data.Broker || "no broker";
            timetrade.forEach(async (timeEntry) => {
              try {
                const time_current = replaceTime(timeCr, timeEntry.open);
                const status = timeEntry.status || "";
                //  if(sym === "XAUUSD") console.log(`1 ${sym} - ${status}: ${last_reset} <  ${time_current}  => Reset Open Time`);
                if (status === "true" && last_reset < time_current) {
                  // console.log(`2 ${sym} - ${status} : ${last_reset} <  ${time_current}  => Reset Open Time`);
                  const groupKey = "RESET";

                  const payload = {
                    symbol: sym,
                    broker,
                  };
                  const result = queue.receive(
                    groupKey,
                    payload,
                    async (symb, meta) => {
                      console.log(`🚀 Processing: ${symb}`);
                      console.log(
                        `   Brokers đã gửi: ${meta.brokers.join(", ")}`
                      );

                      await Redis.publish(
                        "RESET_ALL",
                        JSON.stringify({
                          Symbol: symb,
                          Broker: "ALL-BROKERS-SYMBOL",
                        })
                      );
                    }
                  );
                }
              } catch (err) {
                console.error(
                  `[ScanTimeOpen] Error processing time entry for ${sym}:`,
                  err.message
                );
              }
            });
          } catch (err) {
            console.error(
              `[ScanTimeOpen] Error fetching symbol info for ${sym}:`,
              err.message
            );
            return;
          }
        });
      } catch (err) {
        console.error(`[ScanTimeOpen] Error ${sym}:`, err.message);
      }
    })
  );
}

// ================================
// ⭐ Hàm replaceTime đã đúng
// ================================
function replaceTime(A, B) {
  const [datePart] = A.split(" ");
  return `${datePart} ${B}`;
}

function toDate(str) {
  // str: "2025.12.05 18:13:12"
  if (!str) return null;

  const [datePart, timePart] = str.split(" ");
  if (!datePart || !timePart) return null;

  const [Y, M, D] = datePart.split(".").map(Number);
  const [h, m, s] = timePart.split(":").map(Number);

  // Tháng trong JS bắt đầu từ 0 → phải -1
  return new Date(Y, M - 1, D, h, m, s);
}

module.exports = startJob;
