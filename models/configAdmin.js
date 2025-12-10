// models/configAdmin.model.js
const mongoose = require("mongoose");

const ConfigAdminSchema = new mongoose.Schema(
  {
    TimeStopReset: {
      start: { type: String, required: true },
      end: { type: String, required: true },
    },

    AutoTrade: { type: Boolean, default: true },
    sendTelegram: { type: Boolean, default: true },
    SpreadPlus: { type: Number, default: 1.2 },
  },
  { timestamps: true }
);

// ❗ Ràng buộc chỉ được tạo 1 record duy nhất
ConfigAdminSchema.statics.ensureSingleRecord = async function () {
  const count = await this.countDocuments();
  if (count >= 1) throw new Error("Chỉ được phép có 1 ConfigAdmin trong database.");
};

module.exports = mongoose.model("configadmins", ConfigAdminSchema);
