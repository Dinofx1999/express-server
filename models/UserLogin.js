const mongoose = require('mongoose');
const {Schema , model} = mongoose;
  const userLoginSchema = new mongoose.Schema({
    username: { type: String, required: true, unique: true },
    password: { type: String, required: true },
    email: { type: String, required: true, unique: true  },
    name: { type: String, required: true },
    rule: { type: String, required: true },
  }, {
    versionKey: false,
  });

  module.exports = mongoose.model('userLogin', userLoginSchema);
