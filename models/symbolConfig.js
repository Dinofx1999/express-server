const mongoose = require('mongoose');
const { Schema, model } = mongoose;

const symbolConfigsSchema = new Schema({
  Symbol: { 
    type: String, 
    required: true, 
    unique: true 
  },
  Spread_STD: { 
    type: Number,  // Thay Float32Array bằng Number
    required: true,
    default: 1
  },
  Spread_ECN: { 
    type: Number,  // Thay Float32Array bằng Number
    required: true,
    default: 1
    // unique: true -- Removed as it usually doesn't make sense for a spread value to be unique
  },
  Sydney: { 
    type: Number, 
    required: true, 
    //default: 1 
  },
  Tokyo: { 
    type: Number, 
    required: true, 
   // default: 1 
  },
  London: { 
    type: Number, 
    required: true, 
   // default: 1 
  },
  NewYork: { 
    type: Number, 
    required: true, 
   // default: 1 
  },
}, {
  versionKey: false,
});

module.exports = mongoose.model('symbolConfigs', symbolConfigsSchema);