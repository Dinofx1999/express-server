const mongoose = require('mongoose');
const crypto = require('crypto');

// Hàm tạo hash 10 ký tự
function generateSecretId(length = 10) {
    return crypto.randomBytes(Math.ceil(length / 2))
        .toString('hex')
        .slice(0, length)
        .toUpperCase();
}

const userLoginSchema = new mongoose.Schema({
    username: { type: String, required: true, unique: true },
    password: { type: String, required: true },
    email: { type: String, required: true, unique: true },
    name: { type: String, required: true },
    rule: { type: String, required: true },
    
    // ═══ THÊM TRƯỜNG MỚI ═══
    id_SECRET: { 
        type: String, 
        required: true, 
        unique: true,
        default: () => generateSecretId(10)
    },
}, {
    versionKey: false,
});

module.exports = mongoose.model('userLogin', userLoginSchema);