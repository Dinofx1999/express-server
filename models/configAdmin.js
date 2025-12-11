// models/configAdmin.model.js
const mongoose = require("mongoose");

const TimeRangeSchema = new mongoose.Schema({
  start: { type: String },  // HH:mm:ss
  end: { type: String },    // HH:mm:ss
});

const ConfigAdminSchema = new mongoose.Schema({
 AutoTrade: {
  Success: { type: Boolean, default: true },
  TimeEnable: []
},
  sendTelegram: { type: Boolean, default: true },
  Delay_Stop: { type: Number, default: 0 },
  Type_Analysis: { type: String, enum: ["type1", "type2"], default: "type1" },
  TimeStopReset: {
    start: { type: String, required: true },
    end: { type: String, required: true },
  },
  SpreadPlus: { type: Number, default: 1.2 },
});
 

// ❗ Ràng buộc chỉ được tạo 1 record duy nhất
ConfigAdminSchema.statics.ensureSingleRecord = async function () {
  const count = await this.countDocuments();
  if (count >= 1) throw new Error("Chỉ được phép có 1 ConfigAdmin trong database.");
};

module.exports = mongoose.model("configadmins", ConfigAdminSchema);
