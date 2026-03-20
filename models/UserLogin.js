const mongoose = require("mongoose");
const crypto = require("crypto");

// Hàm tạo hash 10 ký tự
function generateSecretId(length = 10) {
  return crypto
    .randomBytes(Math.ceil(length / 2))
    .toString("hex")
    .slice(0, length)
    .toUpperCase();
}

const userLoginSchema = new mongoose.Schema(
  {
    username: { type: String, required: true, unique: true },
    password: { type: String, required: true },
    email: { type: String, required: true, unique: true },
    name: { type: String, required: true },
    rule: { type: String, required: true },
    actived: { type: Boolean, default: false },
    last_online: { type: String, default: "" },

    id_SECRET: {
      type: String,
      required: true,
      unique: true,
      default: () => generateSecretId(10),
    },

    // ✅ Khung giờ cho phép đăng nhập mỗi ngày
    loginWindow: {
      enabled: { type: Boolean, default: true },
      // phút trong ngày: 14:00 = 14*60 = 840, 18:00 = 1080
      startMinute: { type: Number, default: 14 * 60, min: 0, max: 1439 },
      endMinute: { type: Number, default: 18 * 60, min: 0, max: 1439 },
      // nếu true: cho phép login xuyên qua 00:00 (ví dụ 22:00 -> 02:00)
      allowOvernight: { type: Boolean, default: false },
      // timezone nếu muốn cố định theo VN
      timezone: { type: String, default: "Asia/Ho_Chi_Minh" },
    },
  },
  { versionKey: false }
);

module.exports = mongoose.model("userLogin", userLoginSchema);
