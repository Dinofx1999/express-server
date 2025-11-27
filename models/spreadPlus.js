const mongoose = require('mongoose');
const {Schema , model} = mongoose;
  const spreadPlusSchema = new mongoose.Schema({
    name_Setting: { type: String, required: true, unique: true },
    value: { type: Number, required: true, unique: true },
  }, {
    versionKey: false,
  });

  module.exports = mongoose.model('spreadPlus', spreadPlusSchema);
