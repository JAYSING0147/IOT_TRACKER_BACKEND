const mongoose = require('mongoose');

const deviceLogSchema = new mongoose.Schema({
  deviceId: {
    type: String,
    required: true,
    index: true
  },
  event: {
    type: String,
    enum: ['ONLINE', 'OFFLINE'],
    required: true
  },
  timestamp: {
    type: Date,
    default: Date.now,
    index: true
  }
});

module.exports = mongoose.model('DeviceLog', deviceLogSchema);
