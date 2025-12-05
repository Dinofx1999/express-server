const { sendTelegramMessage } = require('./telegram.service');
require('dotenv').config();

(async () => {
  try {
    await sendTelegramMessage({
      text: 'Test ECONNRESET – gửi đơn giản',
    });
    console.log('✅ Sent ok');
  } catch (err) {
    console.error('❌ Still error:', err.code, err.message);
  }
})();