require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const mqtt = require('mqtt');
const cors = require('cors');

const Device = require('./models/Device');
const DeviceLog = require('./models/DeviceLog');

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
      const existing = await Device.findOne({ deviceId });
      if (!existing || existing.status === 'OFFLINE') {
        // Device just came online, log it
        await DeviceLog.create({ deviceId, event: 'ONLINE' });
      }

      await Device.findOneAndUpdate(
        { deviceId },
        { status: 'ACTIVE', lastSeen: new Date() },
        { upsert: true, returnDocument: 'after' }
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
    
    const devicesToOffline = await Device.find({ 
      status: 'ACTIVE', 
      lastSeen: { $lt: cutoffTime } 
    });

    if (devicesToOffline.length > 0) {
      const logs = devicesToOffline.map(d => ({ deviceId: d.deviceId, event: 'OFFLINE' }));
      await DeviceLog.insertMany(logs);
      
      const result = await Device.updateMany(
        { _id: { $in: devicesToOffline.map(d => d._id) } },
        { $set: { status: 'OFFLINE' } }
      );
      console.log(`Marked ${result.modifiedCount} devices as OFFLINE`);
    }
  } catch (err) {
    console.error('Sweeper error:', err);
  }
}, 10000);

// 4. API Endpoints
app.get('/api/devices', async (req, res) => {
  try {
    const devices = await Device.find();
    res.json(devices);
  } catch (err) {
    res.status(500).json({ error: 'Failed to fetch devices' });
  }
});

// -- INSIGHTS APIs --

app.get('/api/insights/daily', async (req, res) => {
  try {
    const startOfDay = new Date();
    startOfDay.setHours(0, 0, 0, 0);

    const logsToday = await DeviceLog.find({ event: 'ONLINE', timestamp: { $gte: startOfDay } });
    const activeNow = await Device.find({ status: 'ACTIVE' });
    
    const uniqueDeviceIds = new Set();
    logsToday.forEach(log => uniqueDeviceIds.add(log.deviceId));
    activeNow.forEach(dev => uniqueDeviceIds.add(dev.deviceId));

    res.json({ totalActiveToday: uniqueDeviceIds.size });
  } catch (err) {
    res.status(500).json({ error: 'Failed' });
  }
});

app.get('/api/insights/hourly', async (req, res) => {
  try {
    const startOfDay = new Date();
    startOfDay.setHours(0, 0, 0, 0);
    
    const logs = await DeviceLog.find({ timestamp: { $gte: startOfDay }, event: 'ONLINE' });
    
    const devicesPerHour = Array(24).fill(null).map(() => new Set());
    
    logs.forEach(log => {
      const hour = new Date(log.timestamp).getHours();
      devicesPerHour[hour].add(log.deviceId);
    });
    
    const chartData = devicesPerHour.map((set, i) => ({
      hour: `${i}:00`,
      active: set.size
    }));
    
    res.json(chartData);
  } catch (err) {
    res.status(500).json({ error: 'Failed' });
  }
});

app.get('/api/insights/weekly', async (req, res) => {
  try {
    const data = [];
    for (let i = 6; i >= 0; i--) {
      const d = new Date();
      d.setDate(d.getDate() - i);
      d.setHours(0,0,0,0);
      const nextDay = new Date(d);
      nextDay.setDate(nextDay.getDate() + 1);

      const logs = await DeviceLog.find({ event: 'ONLINE', timestamp: { $gte: d, $lt: nextDay }});
      const unique = new Set(logs.map(l => l.deviceId));
      
      const dayName = d.toLocaleDateString('en-US', { weekday: 'short' });
      data.push({ name: dayName, active: unique.size });
    }
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: 'Failed' });
  }
});

const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
  console.log(`Backend server running on port ${PORT}`);
});
