// telegram.service.js
const axios = require('axios');
const https = require('https');

const agent = new https.Agent({
  keepAlive: false,   // tránh giữ kết nối lâu gây reset
});

async function sendTelegramMessage({ chatId, text, parse_mode }, retry = 0) {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  if (!token) throw new Error('TELEGRAM_BOT_TOKEN is not set');

  const finalChatId = chatId || process.env.TELEGRAM_CHAT_ID_DEFAULT;
  if (!finalChatId) throw new Error('chat_id is empty');

  const url = `https://api.telegram.org/bot${token}/sendMessage`;

  try {
    const res = await axios.post(
      url,
      {
        chat_id: finalChatId,
        text,
        ...(parse_mode ? { parse_mode } : {}),
      },
      {
        timeout: 10000,      // 10s
        httpsAgent: agent,
      }
    );

    return res.data;
  } catch (err) {
    // log thêm để debug
    console.error('[Telegram] ERROR code:', err.code);
    if (err.response) {
      console.error('[Telegram] STATUS:', err.response.status);
      console.error('[Telegram] DATA  :', err.response.data);
    } else {
      console.error('[Telegram] MSG   :', err.message);
    }

    // Retry nếu là lỗi mạng kiểu reset / timeout
    const transientCodes = ['ECONNRESET', 'ETIMEDOUT', 'ECONNABORTED'];

    if (transientCodes.includes(err.code) && retry < 3) {
      const delayMs = 500 * (retry + 1);
      console.warn(`[Telegram] Retry ${retry + 1} after ${delayMs}ms...`);

      await new Promise((r) => setTimeout(r, delayMs));
      return sendTelegramMessage({ chatId, text, parse_mode }, retry + 1);
    }

    throw err;
  }
}

module.exports = {
  sendTelegramMessage,
};
