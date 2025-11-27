const mongoose = require('mongoose');

const ErrorAnalysisSchema = new mongoose.Schema({
    broker: {
        type: String,
        required: true,
    },
    symbol: {
        type: String,
        required: true,
    },
    symbol_raw: {
        type: String,
        required: false,
    },
    spread: {
        type: String,
        required: true,
    },
    time_start: {
        type: String,
        required: true,
    },
    time_current: {
        type: String,
        required: true,
    },
    
    broker_Check: {
        type: [String], // Mảng các broker liên quan
        required: true,
    },
    distance_Check: {
        type: [String], // Mảng các broker liên quan
        required: true,
    },
    type: {
        type: String,
        enum: ['BUY', 'SELL'], // Chỉ cho phép giá trị 'BUY' hoặc 'SELL'
        required: true,
    },
    open_time: {
        type: String,
        required: true,
    },
    last_reset: {
        type: String,
    },
}, {
    timestamps: true, // Tự động thêm createdAt và updatedAt
});

module.exports = mongoose.model('ErrorAnalysis', ErrorAnalysisSchema);