require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const mqtt = require('mqtt');
const cors = require('cors');

const Device = require('./models/Device');

const app = express();
app.use(cors());
app.use(express.json());

// 1. Connect to MongoDB
mongoose.connect(process.env.MONGODB_URI)
  .then(() => console.log('Connected to MongoDB Atlas'))
  .catch(err => console.error('MongoDB connection error:', err));

// 2. Setup MQTT
const clientId = "SmartFarm_Backend_" + Math.random().toString(16).substring(2, 10);
const client = mqtt.connect('ws://mqtt.agri-rana.in:8080/mqtt', { clientId });

client.on('connect', () => {
  console.log('Connected to MQTT Broker via WebSocket');
  client.subscribe('motor/status/#', (err) => {
    if (!err) {
      console.log('Subscribed to motor/status/#');
    }
  });
});

client.on('message', async (topic, message) => {
  const parts = topic.split('/');
  const deviceId = parts[parts.length - 1];
  const payload = message.toString();

  if (payload === 'alive') {
    try {
      await Device.findOneAndUpdate(
        { deviceId },
        { status: 'ACTIVE', lastSeen: new Date() },
        { upsert: true, new: true }
      );
    } catch (err) {
      console.error('Error updating device:', err);
    }
  }
});

// 3. Offline Sweeper (runs every 10 seconds)
const TIMEOUT_MS = 75000;
setInterval(async () => {
  try {
    const cutoffTime = new Date(Date.now() - TIMEOUT_MS);
    const result = await Device.updateMany(
      { status: 'ACTIVE', lastSeen: { $lt: cutoffTime } },
      { $set: { status: 'OFFLINE' } }
    );
    if (result.modifiedCount > 0) {
      console.log(`Marked ${result.modifiedCount} devices as OFFLINE`);
    }
  } catch (err) {
    console.error('Sweeper error:', err);
  }
}, 10000);

// 4. API Endpoint for Frontend
app.get('/api/devices', async (req, res) => {
  try {
    const devices = await Device.find();
    res.json(devices);
  } catch (err) {
    res.status(500).json({ error: 'Failed to fetch devices' });
  }
});

const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
  console.log(`Backend server running on port ${PORT}`);
});
