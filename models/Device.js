const mongoose = require('mongoose');

const deviceSchema = new mongoose.Schema({
  deviceId: {
    type: String,
    required: true,
    unique: true
  },
  status: {
    type: String,
    enum: ['ACTIVE', 'OFFLINE'],
    default: 'OFFLINE'
  },
  lastSeen: {
    type: Date,
    default: Date.now
  }
}, { timestamps: true });

module.exports = mongoose.model('Device', deviceSchema);
