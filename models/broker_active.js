const mongoose = require('mongoose');

const broker_ActivedSchema = new mongoose.Schema({
    broker: {
        type: String,
        required: true,
    },
    actived: {
        type: Boolean,
        enum: [true, false], // Chỉ cho phép giá trị 'true' hoặc 'false'
        required: true,
    }
}, {
    timestamps: true, // Tự động thêm createdAt và updatedAt
});

module.exports = mongoose.model('broker_Actived', broker_ActivedSchema);