const Redis = require("../Redis/clientRedis");
const { connectMongoDB } = require("../../Database/mongodb");
const { getTimeGMT7 } = require("../Helpers/time");
const { isForexSymbol } = require("../Helpers/typeSymbol");
const SymbolDebounceQueue = require("../Redis/DebounceQueue");
const { configAdmin } = require("../../../models/index");

const {
  getBrokerResetting,
  getAllBrokers,
  getMultipleSymbolAcrossBrokersWithMetaFast,
  getRedis,
} = require("../Redis/redis.helper2");
const { getAllUniqueSymbols } = require("../Redis/redis.price.query");

const queue = new SymbolDebounceQueue({
  debounceTime: 3000, // 5s không có payload mới
  maxWaitTime: 5000,  // Tối đa 10s
  maxPayloads: 5000,  // Tối đa 5000 unique payloads
  delayBetweenTasks: 300, // 500ms delay giữa các task
  cooldownTime: 5000, // 10s cooldown after processing
});

let config;

// ================================
// Helpers time
// ================================
function pad2(n) {
  return String(n).padStart(2, "0");
}

function normalizeTimePart(t) {
  // "HH:MM" -> "HH:MM:00", "HH:MM:SS" -> giữ nguyên
  if (!t) return "00:00:00";
  const s = String(t).trim();
  if (/^\d{1,2}:\d{2}$/.test(s)) return `${s}:00`;
  if (/^\d{1,2}:\d{2}:\d{2}$/.test(s)) return s;
  return s; // nếu format khác thì trả nguyên (đỡ phá)
}

function timeToSeconds(t) {
  // t: "HH:MM" | "HH:MM:SS"
  const s = normalizeTimePart(t);
  const [h, m, sec] = s.split(":").map((x) => Number(x));
  return (h || 0) * 3600 + (m || 0) * 60 + (sec || 0);
}

function isInStopRange(timeNowHHMMSS, startHHMMSS, endHHMMSS) {
  // Handle cả trường hợp range qua ngày (vd 23:00 -> 01:00)
  const now = timeToSeconds(timeNowHHMMSS);
  const start = timeToSeconds(startHHMMSS);
  const end = timeToSeconds(endHHMMSS);

  if (start <= end) {
    return now >= start && now <= end;
  }
  // range qua ngày
  return now >= start || now <= end;
}

// ================================
// ⭐ replaceTime giữ nguyên như bạn (chỉ dùng cho datetime)
// ================================
function replaceTime(A, B) {
  const [datePart] = String(A).split(" ");
  return `${datePart} ${normalizeTimePart(B)}`;
}

function toDate(str) {
  // str: "2025.12.05 18:13:12"
  if (!str) return null;

  const [datePart, timePartRaw] = String(str).split(" ");
  if (!datePart || !timePartRaw) return null;

  const timePart = normalizeTimePart(timePartRaw);

  const [Y, M, D] = datePart.split(".").map(Number);
  const [h, m, s] = timePart.split(":").map(Number);

  return new Date(Y, (M || 1) - 1, D || 1, h || 0, m || 0, s || 0);
}

async function startJob() {
  console.log(`[JOB ${process.pid}] Scan Open Time booting...`);

  try {
    await connectMongoDB();
    console.log(`[JOB ${process.pid}] MongoDB connected.`);
  } catch (error) {
    console.error(`[JOB ${process.pid}] MongoDB connection failed:`, error.message);
    return;
  }

  const intervalMs = Number(process.env.CRON_INTERVAL_SCAN_OPEN || 1000);
  const interval_getConfig = Number(process.env.CRON_INTERVAL_GET_CONFIG || 1000);

  let isRunning = false;
  let isRunningConfig = false;

  // ===========================
  // Scan Time Open để reset
  // ===========================
  setInterval(async () => {
    if (isRunning) return;
    isRunning = true;

    try {
      const timeCurrent = getTimeGMT7(); // "HH:MM:SS"
      config = await Redis.getConfigAdmin();

      if (!config) {
        console.error(`[JOB ${process.pid}] No config found`);
        isRunning = false;
        return;
      }

      const timeConfig = config?.TimeStopReset || [];
      let Check_Run = true;

      // ✅ KHÔNG dùng async map (vì không await)
      for (const timeRange of timeConfig) {
        const startT = timeRange?.start;
        const endT = timeRange?.end;
        if (!startT || !endT) continue;

        // ✅ So sánh theo "giờ trong ngày" (đúng với timeCurrent HH:MM:SS)
        if (isInStopRange(timeCurrent, startT, endT)) {
          Check_Run = false;
          break;
        }
      }

      if (Check_Run === true) {
        await ScanTimeOpenSymbol();
      }
    } catch (err) {
      console.error(`[JOB ${process.pid}] Scan error:`, err);
    }

    isRunning = false;
  }, intervalMs);

  // ===========================
  // Get Config Admin
  // ===========================
  setInterval(async () => {
    if (isRunningConfig) return;
    isRunningConfig = true;

    try {
      const cfg = await configAdmin.findOne();
      await Redis.saveConfigAdmin(cfg);
    } catch (err) {
      console.error(`[JOB ${process.pid}] GetConfig error:`, err);
    }

    isRunningConfig = false;
  }, interval_getConfig);

  console.log(`[JOB ${process.pid}] Scan Open Time ready. Interval: ${intervalMs} ms`);
}

async function ScanTimeOpenSymbol() {
  const ALL_Symbol = await getAllUniqueSymbols();
  const All_Broker = await getAllBrokers();

  const symbols = (ALL_Symbol || [])
    .map((s) => String(s).trim().toUpperCase())
    .filter(Boolean);

  const priceDataMap_ = await getMultipleSymbolAcrossBrokersWithMetaFast(
    symbols,
    All_Broker,
    getRedis()
  );

  const resetting = await getBrokerResetting();
  if (Array.isArray(resetting) && resetting.length > 0) return;

  // ✅ giữ logic: chạy song song
  symbols?.forEach((sym) => {
    (async () => {
      try {
        const Check_Reset = isForexSymbol(sym);
        if (Check_Reset) return;

        const priceData = priceDataMap_.get(sym);
        if (!priceData || priceData.length <= 1) return;

        priceData.forEach((data) => {
          (async () => {
            try {
              let timetrade = [];
              try {
                timetrade = JSON.parse(data.timetrade) || [];
              } catch {
                timetrade = [];
              }

              if (String(data.index) === "0") return;
              const broker = data.Broker || data.broker_ || "no broker";

              // timecurrent + last_reset thường là "YYYY.MM.DD HH:MM:SS"
              const last_reset_str = data.last_reset || "";
              const time_current_str = data.timecurrent || "";

              const lastResetDate = toDate(last_reset_str) || new Date(0);

              for (const timeEntry of timetrade) {
                const status = String(timeEntry?.status || "").toLowerCase(); // "true"/"false"
                const openT = normalizeTimePart(timeEntry?.open);  // HH:MM(:SS)
                const closeT = normalizeTimePart(timeEntry?.close);

                // ghép thành datetime theo ngày của timecurrent
                let time_open_symbol_str = replaceTime(time_current_str, openT);
                let openDate = toDate(time_open_symbol_str);

                if (!openDate) continue;

                // ✅ qua ngày nếu open > close (so theo seconds)
                if (timeToSeconds(openT) > timeToSeconds(closeT)) {
                  openDate.setDate(openDate.getDate() - 1);
                }

                // ✅ so bằng timestamp
                if (status === "true" && lastResetDate.getTime() < openDate.getTime()) {
                  const groupKey = "RESET";
                  const payload = { symbol: sym, broker };

                  queue.receive(groupKey, payload, async (symb, meta) => {
                    console.log(`🚀 Processing: ${symb}`);
                    console.log(`Brokers đã gửi: ${meta.brokers.join(", ")}`);

                    await Redis.publish(
                      "RESET_ALL",
                      JSON.stringify({
                        Symbol: symb,
                        Broker: "ALL-BROKERS-SYMBOL",
                      })
                    );
                  });

                  // nếu muốn chỉ trigger 1 lần / symbol / vòng scan thì break ở đây
                  // break;
                }
              }
            } catch (err) {
              console.error(`[ScanTimeOpen] Error processing data for ${sym}:`, err.message);
            }
          })();
        });
      } catch (err) {
        console.error(`[ScanTimeOpen] Error ${sym}:`, err.message);
      }
    })();
  });
}

module.exports = startJob;
