const mongoose = require('mongoose');

const symbolSettingSchema = new mongoose.Schema({
    broker: {
        type: String,
        required: true,
    },
    symbol: {
        type: String,
        required: true,
    },
    time_active: {
        type: String,
    },
    time_run: {
        type: String,
    },
    time_open: {
        type: String,
    },
    time_close: {
        type: String,
    },
}, {
    timestamps: true, // Tự động thêm createdAt và updatedAt
});

module.exports = mongoose.model('symbolSetting', symbolSettingSchema);