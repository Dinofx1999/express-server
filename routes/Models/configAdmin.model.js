var express = require('express');
var router = express.Router();
var {configAdmin} = require('../../models/index');

exports.createConfig = async (req, res) => {
  try {
    await configAdmin.ensureSingleRecord(); // Ngăn không cho thêm record thứ 2
    console.log("Creating new ConfigAdmin:", req.body);
    const config = await configAdmin.create(req.body);

    res.json({ success: true, data: config });
  } catch (err) {
    res.status(400).json({ success: false, message: err.message });
  }
};


exports.updateConfig = async (req, res) => {
  try {
    const config = await configAdmin.findOne();

    if (!config) return res.status(404).json({ message: "Config chưa tồn tại." });

    const updated = await configAdmin.findByIdAndUpdate(config._id, req.body, {
      new: true,
    });
    res.json({ success: true, data: updated });
  } catch (err) {
    res.status(400).json({ success: false, message: err.message });
  }
};


exports.deleteConfig = async (req, res) => {
  try {
    const config = await configAdmin.findOne();

    if (!config) return res.status(404).json({ message: "Không có config để xóa." });

    await configAdmin.findByIdAndDelete(config._id);

    res.json({ success: true, message: "Đã xóa ConfigAdmin." });
  } catch (err) {
    res.status(400).json({ success: false, message: err.message });
  }
};
