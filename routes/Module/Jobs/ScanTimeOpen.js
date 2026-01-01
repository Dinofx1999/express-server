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

const { getBrokerResetting,
  getAllBrokers,
  getMultipleSymbolAcrossBrokersWithMetaFast,
  getRedis } = require('../Redis/redis.helper2');
const { getAllUniqueSymbols } = require("../Redis/redis.price.query");
const { startSession } = require("../../../models/UserLogin");

const queue = new SymbolDebounceQueue({
  debounceTime: 5000, // 5s không có payload mới
  maxWaitTime: 10000, // Tối đa 10s
  maxPayloads: 5000, // Tối đa 5000 unique payloads
  delayBetweenTasks: 500, // 500ms delay giữa các task
  cooldownTime: 10000, // 10s cooldown after processing
});
let config;

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
    config = await Redis.getConfigAdmin();
    if (!config) {
      console.error(`[JOB ${process.pid}] No config found`);
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

  const ALL_Symbol = await getAllUniqueSymbols();
  const All_Broker = await getAllBrokers();
  // Chuẩn hóa symbols
  const symbols = ALL_Symbol
    .map(s => String(s).trim().toUpperCase())
    .filter(Boolean);

  // 2️⃣ Lấy TẤT CẢ price data 1 lần (thay vì 272 calls!)
  const priceDataMap_ = await getMultipleSymbolAcrossBrokersWithMetaFast(symbols, All_Broker, getRedis());
  const resetting = await getBrokerResetting();
  if(Array.isArray(resetting) && resetting.length > 0) return;
  symbols?.map(async (sym) => {
    try {
      const priceData = priceDataMap_.get(sym);
      if(priceData?.length > 1) {
        priceData.map(async (data) => {
          try {
              const timetrade = JSON.parse(data.timetrade) || [];
              const  broker = data.Broker || data.broker_ || "no broker";
              timetrade?.forEach(async (timeEntry) => {
                 
                  const last_reset = data.last_reset || "";
                  const status = timeEntry.status || "";
                  const timeConfig = config?.TimeStopReset;
                  const time_current = data.timecurrent;
                   const time_open_symbol = replaceTime(time_current,timeEntry.open);

                  // console.log(timeConfig);
                  
                  if (status === "true" && last_reset < time_open_symbol) {
                  // if (status === "true" && last_reset > time_open_symbol && sym === "XAUUSD") {
                    timeConfig?.map(async (timeRange) => {
                      const start = replaceTime(time_current,timeRange.start);
                      const end = replaceTime(time_current,timeRange.end);
                      if ((time_open_symbol) >= start && (time_open_symbol) <= end){
                        return;
                      }else{
                        // if(sym === "VIX")console.log(start, " < ",time_open_symbol ," < " , end , " : ", time_current , "Last: " , last_reset);
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
                              console.log(`Brokers đã gửi: ${meta.brokers.join(", ")}`);
                              await Redis.publish(
                                "RESET_ALL",
                                JSON.stringify({
                                  Symbol: symb,
                                  Broker: "ALL-BROKERS-SYMBOL",
                                })
                              );
                            });
                          }
                    }); 
                  }
              });
          } catch (err) {
            console.error(`[ScanTimeOpen] Error processing data for ${sym}:`, err.message);
          }
        });
      }
    } catch (err) {
      console.error(`[ScanTimeOpen] Error ${sym}:`, err.message);
    }
  });

  // if (resetting.length > 0) return;
  // await Promise.all(
  //   symbols.map(async (sym) => {
  //     try {
  //       const priceData = priceDataMap_.get(sym);
  //       priceData.map(async (data) => {
  //         try {
  //           if (!data) {
  //             console.log(`[ScanTimeOpen] No symbol info for ${sym}`);
  //             return;
  //           }

  //           const timetrade = JSON.parse(data.timetrade) || [];
  //           const timeCr = data.timecurrent || "";
  //           const last_reset = data.last_reset || "";
  //           const broker = data.Broker || data.broker_ || "no broker";
  //           timetrade?.forEach(async (timeEntry) => {
  //             try {
  //               const time_open_symbol = timeEntry.open || "";

  //               const time_current = replaceTime(timeCr, timeEntry.open);
  //               const status = timeEntry.status || "";
  //               //  if(sym === "XAUUSD") console.log(`1 ${sym} - ${status}: ${last_reset} <  ${time_current}  => Reset Open Time`);
  //               if (status === "true" && last_reset < time_current) {

  //                 config.TimeStopReset.map(async (timetrade) => {
  //                   try {
  //                     const start = timeToNumber(timetrade.start);
  //                     const end = timeToNumber(timetrade.end);
  //                     if (timeToNumber(time_open_symbol) >= start && timeToNumber(time_open_symbol) <= end) return;
  //                   } catch (err) {
  //                     console.error(
  //                       `[ScanTimeOpen] Error processing time entry for ${sym}:`,
  //                       err.message
  //                     );
  //                   }
  //                 });

  //                 const groupKey = "RESET";

  //                 const payload = {
  //                   symbol: sym,
  //                   broker,
  //                 };
  //                 const result = queue.receive(
  //                   groupKey,
  //                   payload,
  //                   async (symb, meta) => {
  //                     console.log(`🚀 Processing: ${symb}`);
  //                     console.log(
  //                       `   Brokers đã gửi: ${meta.brokers.join(", ")}`
  //                     );

  //                     await Redis.publish(
  //                       "RESET_ALL",
  //                       JSON.stringify({
  //                         Symbol: symb,
  //                         Broker: "ALL-BROKERS-SYMBOL",
  //                       })
  //                     );
  //                   }
  //                 );
  //               }
  //             } catch (err) {
  //               console.error(
  //                 `[ScanTimeOpen] Error processing time entry for ${sym}:`,
  //                 err.message
  //               );
  //             }

  //           }
  //           );
  //         } catch (err) {
  //           console.error(
  //             `[ScanTimeOpen] Error fetching symbol info for ${sym}:`,
  //             err.message
  //           );
  //           return;
  //         }
  //       });
  //     } catch (err) {
  //       console.error(`[ScanTimeOpen] Error ${sym}:`, err.message);
  //     }
  //   })
  // );
}

// async function ScanTimeOpenSymbol() {
//   const r = getRedis(); // ioredis instance

//   // ====== CONFIG RETRY ======
//   const RETRY_INTERVAL_MS = 15_000; // 15s mới retry 1 lần
//   const MAX_RETRIES = 5;            // thử tối đa 5 lần
//   const PENDING_TTL_SEC = 10 * 60;  // pending sống 10 phút
//   const LOCK_TTL_SEC = 60;          // lock chống spam 60s/event

//   try {
//     const ALL_Symbol = await getAllUniqueSymbols();
//     const All_Broker = await getAllBrokers();

//     const symbols = (ALL_Symbol || [])
//       .map(s => String(s || "").trim().toUpperCase())
//       .filter(Boolean);

//     if (!symbols.length) return;

//     // Nếu đang resetting toàn hệ thống -> thoát
//     const resetting = await getBrokerResetting();
//     if (Array.isArray(resetting) && resetting.length > 0) return;

//     // Lấy all price data 1 lần
//     const priceDataMap_ = await getMultipleSymbolAcrossBrokersWithMetaFast(
//       symbols,
//       All_Broker,
//       r
//     );

//     // Parse TimeStopReset 1 lần
//     const blockedRanges = Array.isArray(config?.TimeStopReset)
//       ? config.TimeStopReset
//           .map(t => {
//             try {
//               const s = timeToNumber(t?.start);
//               const e = timeToNumber(t?.end);
//               if (!Number.isFinite(s) || !Number.isFinite(e)) return null;
//               return { s, e };
//             } catch {
//               return null;
//             }
//           })
//           .filter(Boolean)
//       : [];

//     const isBlockedTime = (timeOpenStr) => {
//       if (!blockedRanges.length) return false;
//       const t = timeToNumber(timeOpenStr);
//       if (!Number.isFinite(t)) return false;
//       for (const rr of blockedRanges) {
//         if (t >= rr.s && t <= rr.e) return true;
//       }
//       return false;
//     };

//     const nowMs = Date.now();

//     // Helper: key names
//     const pendingKey = (broker, sym) => `reset:pending:${broker}:${sym}`;
//     const lockKey = (broker, sym, timeCurrent) =>
//       `reset:lock:${broker}:${sym}:${timeCurrent}`;

//     // Helper: publish reset (bạn có thể đổi topic/payload theo hệ thống của bạn)
//     const publishReset = async (sym, broker) => {
//       // Bạn đang publish reset all brokers symbol, giữ nguyên như code cũ của bạn:
//       await Redis.publish(
//         "RESET_ALL",
//         JSON.stringify({
//           Symbol: sym,
//           Broker: "ALL-BROKERS-SYMBOL",
//         })
//       );
//       // Nếu bạn muốn reset theo broker riêng thì đổi payload ở đây.
//     };

//     // ====== LOOP ======
//     for (const sym of symbols) {
//       const priceDataArr = priceDataMap_?.get(sym);
//       if (!Array.isArray(priceDataArr) || !priceDataArr.length) continue;

//       for (const data of priceDataArr) {
//         if (!data) continue;

//         const broker = data.Broker || data.broker_ || "no_broker";
//         const timeCr = data.timecurrent || "";
//         const last_reset = data.last_reset || "";

//         // 1) Parse timetrade
//         let timetrade = [];
//         try {
//           timetrade = data.timetrade ? JSON.parse(data.timetrade) : [];
//           if (!Array.isArray(timetrade)) timetrade = [];
//         } catch {
//           timetrade = [];
//         }
//         if (!timetrade.length) continue;

//         // 2) Nếu đang pending -> kiểm tra success / retry
//         // pending format:
//         // { time_current, tries, lastTryMs }
//         let pending = null;
//         try {
//           const raw = await r.get(pendingKey(broker, sym));
//           pending = raw ? JSON.parse(raw) : null;
//         } catch {
//           pending = null;
//         }

//         if (pending?.time_current) {
//           // ✅ Success condition: last_reset >= pending.time_current
//           // (giống logic bạn đang dùng: last_reset < time_current => cần reset)
//           if (last_reset >= pending.time_current) {
//             // reset thành công -> xoá pending
//             await r.del(pendingKey(broker, sym));
//           } else {
//             // chưa thành công -> retry nếu đủ thời gian + chưa vượt max
//             const tries = Number(pending.tries || 0);
//             const lastTryMs = Number(pending.lastTryMs || 0);

//             if (tries < MAX_RETRIES && nowMs - lastTryMs >= RETRY_INTERVAL_MS) {
//               // lock theo event để tránh spam trong 1 vòng scan
//               const lk = lockKey(broker, sym, pending.time_current);
//               const locked = await r.set(lk, "1", "NX", "EX", LOCK_TTL_SEC);
//               if (locked) {
//                 await publishReset(sym, broker);

//                 const nextPending = {
//                   time_current: pending.time_current,
//                   tries: tries + 1,
//                   lastTryMs: nowMs,
//                 };
//                 await r.set(
//                   pendingKey(broker, sym),
//                   JSON.stringify(nextPending),
//                   "EX",
//                   PENDING_TTL_SEC
//                 );
//               }
//             }
//           }

//           // Nếu đang pending thì không tạo event mới trong vòng này nữa
//           continue;
//         }

//         // 3) Không pending -> tìm xem có event open cần reset không
//         for (const timeEntry of timetrade) {
//           const status = String(timeEntry?.status || "").toLowerCase();
//           if (status !== "true") continue;

//           const time_open_symbol = timeEntry?.open || "";
//           if (!time_open_symbol) continue;

//           // chặn nếu nằm trong khung TimeStopReset
//           if (isBlockedTime(time_open_symbol)) continue;

//           const time_current = replaceTime(timeCr, time_open_symbol);
//           if (!time_current) continue;

//           // Điều kiện cần reset
//           if (last_reset >= time_current) continue;

//           // ✅ event open của broker này -> reset 1 lần
//           const lk = lockKey(broker, sym, time_current);
//           const locked = await r.set(lk, "1", "NX", "EX", LOCK_TTL_SEC);
//           if (!locked) {
//             // đã có ai reset event này rồi (trong 60s)
//             break;
//           }

//           await publishReset(sym, broker);

//           // set pending để nếu chưa thành công thì retry
//           const newPending = {
//             time_current,
//             tries: 1,
//             lastTryMs: nowMs,
//           };
//           await r.set(
//             pendingKey(broker, sym),
//             JSON.stringify(newPending),
//             "EX",
//             PENDING_TTL_SEC
//           );

//           // ✅ chỉ reset 1 lần cho broker+symbol trong vòng scan
//           break;
//         }
//       }
//     }
//   } catch (err) {
//     console.error("[ScanTimeOpenSymbol] Fatal:", err?.message || err);
//   }
// }


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
