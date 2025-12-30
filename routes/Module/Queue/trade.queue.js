// trade.queue.js
const { sendTelegramMessage } = require('../Telegram/telegram.service'); // Hàm gửi Telegram
// const { saveTradeToDB } = require('./trade.repository');     // Nếu anh muốn lưu Mongo

// Hàng đợi lưu các lệnh trade từ client
const tradeQueue = [];
let isWorking = false;

const TRADE_QUEUE_INTERVAL = Number(process.env.TRADE_QUEUE_INTERVAL || 1000); // 0.5s

// Hàm add Job vào queue
function addTradeJob(trade) {
  tradeQueue.push(trade);
  console.log('[TradeQueue] Job added. Queue length:', tradeQueue.length);
}

// Hàm format message gửi Telegram
function formatTradeMessage(trade) {
  const Type      = trade.Type;
  const Symbol    = trade.Symbol;
  const Broker    = trade.Broker || "";
  const Ticket    = trade.Ticket;
  const Open      = trade.OpenPrice;
  const TimeOpen  = trade.TimeOpen;
  const Volume    = trade.Volume;
  const PriceSend = trade.PriceSend;
  const Comment   = trade.Comment || "-";
  const Status    = trade.Status || "SUCCESS";
  const Spread    = trade.Spread || "";
  const Message   = trade.Message || "";

  // Emoji và text theo status
  const statusEmoji = Status === "SUCCESS" ? "✅" : "❌";
  const typeEmoji = Type?.toUpperCase() === "BUY" ? "🟢" : "🔴";
  
  // Header nổi bật theo loại lệnh
  const header = Type?.toUpperCase() === "BUY" 
    ? `${typeEmoji} *BUY ${Symbol}*` 
    : `${typeEmoji} *SELL ${Symbol}*`;

  return [
    `━━━━━━━━━━━━━━━━━━`,
    `${header}  ${statusEmoji} ${Status}`,
    `━━━━━━━━━━━━━━━━━━`,
    ``,
    `🏦 *Broker:* ${Broker}`,
    `🎫 *Ticket:* \`${Ticket}\``,
    ``,
    `📊 *Trade Info*`,
    `├ Open Price: \`${Open}\``,
    `├ Volume: \`${Volume}\``,
    `├ Price Send: \`${PriceSend}\``,
    `└ Spread: \`${Spread}\``,
    ``,
    `🕒 *Time:* \`${TimeOpen}\``,
    `💬 *Key SECRET:* ${Comment}`,
    Message ? `📝 *Message:* ${Message}` : ``,
  ].filter(line => line !== ``).join('\n');
}

// Worker xử lý từng job trong queue
async function processTradeQueue() {

  if (isWorking) return;
  const job = tradeQueue.shift();
  if (!job) return;

  isWorking = true;
  try {
    // 1️⃣ Lưu lại (ở đây em cho fake, anh tự nối vào Mongo)
    // await saveTradeToDB(job);

    // 2️⃣ Gửi Telegram
    const text = formatTradeMessage(job);
    console.log(text);
    await sendTelegramMessage({
      chatId: process.env.TELEGRAM_CHAT_ID_DEFAULT,
      text,
      parse_mode: 'Markdown',
    });

    console.log('[TradeQueue] Sent trade to Telegram:', job.Symbol, job.Ticket);
  } catch (err) {
    console.error('[TradeQueue] Error:', err.message || err);
  } finally {
    isWorking = false;
  }
}

// Khởi động worker queue
function startTradeQueue() {
  console.log('[TradeQueue] Worker started. Interval:', TRADE_QUEUE_INTERVAL, 'ms');
  setInterval(processTradeQueue, TRADE_QUEUE_INTERVAL);
}

startTradeQueue();
module.exports = {
  startTradeQueue,
  addTradeJob,
};
