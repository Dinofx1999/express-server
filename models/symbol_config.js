const mongoose = require('mongoose');

const symbolconfigSchema = new mongoose.Schema({
    broker: {
        type: String,
        required: true,
    },
    symbol: {
        type: String,
        required: true,
    },
    time_active: {
        type: Date,
        required: true,
    },
}, {
    timestamps: true, // Tự động thêm createdAt và updatedAt
});

module.exports = mongoose.model('symbolconfig', symbolconfigSchema);