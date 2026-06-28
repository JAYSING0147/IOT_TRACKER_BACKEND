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

function getISTDayBounds(dateParam) {
  let targetDate;
  if (dateParam) {
    const parts = dateParam.split('-');
    targetDate = new Date(Date.UTC(parseInt(parts[0]), parseInt(parts[1]) - 1, parseInt(parts[2])));
  } else {
    const now = new Date();
    targetDate = new Date(now.getTime() + (5.5 * 60 * 60 * 1000));
    targetDate.setUTCHours(0, 0, 0, 0);
  }
  
  const startOfDayUTC = new Date(targetDate.getTime() - (5.5 * 60 * 60 * 1000));
  const endOfDayUTC = new Date(startOfDayUTC.getTime() + (24 * 60 * 60 * 1000));
  
  return { startOfDayUTC, endOfDayUTC };
}

async function calculateSimUptimeAnalysis(dateParam) {
  const { startOfDayUTC, endOfDayUTC } = getISTDayBounds(dateParam);
  const now = new Date();
  
  const calcEnd = endOfDayUTC < now ? endOfDayUTC : now;
  const totalDuration = calcEnd.getTime() - startOfDayUTC.getTime();

  if (totalDuration <= 0) {
    return {
      airtel: { uptimePercent: 0, deviceCount: 0 },
      vi: { uptimePercent: 0, deviceCount: 0 }
    };
  }

  const devices = await Device.find();
  const logs = await DeviceLog.find({ 
    timestamp: { $gte: startOfDayUTC, $lt: endOfDayUTC } 
  }).sort({ timestamp: 1 });

  const logsByDevice = {};
  logs.forEach(log => {
    if (!logsByDevice[log.deviceId]) {
      logsByDevice[log.deviceId] = [];
    }
    logsByDevice[log.deviceId].push(log);
  });

  let airtelTotalUptime = 0;
  let airtelCount = 0;
  let viTotalUptime = 0;
  let viCount = 0;

  devices.forEach(device => {
    const isAirtel = device.deviceId.startsWith('899110');
    const isVI = device.deviceId.startsWith('899111');
    if (!isAirtel && !isVI) return; 

    const devLogs = logsByDevice[device.deviceId] || [];
    let uptimeMs = 0;
    
    if (devLogs.length === 0) {
      if (device.status === 'ACTIVE') {
        uptimeMs = totalDuration;
      }
    } else {
      let currentPos = startOfDayUTC;
      let currentState = devLogs[0].event === 'ONLINE' ? 'OFFLINE' : 'ACTIVE';
      
      devLogs.forEach(log => {
        const logTime = log.timestamp;
        if (currentState === 'ACTIVE') {
          uptimeMs += logTime.getTime() - currentPos.getTime();
        }
        currentPos = logTime;
        currentState = log.event === 'ONLINE' ? 'ACTIVE' : 'OFFLINE';
      });
      
      if (currentState === 'ACTIVE') {
        uptimeMs += calcEnd.getTime() - currentPos.getTime();
      }
    }
    
    const uptimePercent = (uptimeMs / totalDuration) * 100;
    
    if (isAirtel) {
      airtelTotalUptime += uptimePercent;
      airtelCount++;
    } else if (isVI) {
      viTotalUptime += uptimePercent;
      viCount++;
    }
  });

  return {
    airtel: {
      uptimePercent: airtelCount > 0 ? Math.round(airtelTotalUptime / airtelCount) : 0,
      deviceCount: airtelCount
    },
    vi: {
      uptimePercent: viCount > 0 ? Math.round(viTotalUptime / viCount) : 0,
      deviceCount: viCount
    }
  };
}

app.get('/api/devices/:deviceId/logs', async (req, res) => {
  try {
    const { deviceId } = req.params;
    const { date } = req.query;
    const { startOfDayUTC, endOfDayUTC } = getISTDayBounds(date);
    const logs = await DeviceLog.find({ 
      deviceId, 
      timestamp: { $gte: startOfDayUTC, $lt: endOfDayUTC } 
    }).sort({ timestamp: 1 });
    res.json(logs);
  } catch (err) {
    res.status(500).json({ error: 'Failed to fetch logs' });
  }
});

app.get('/api/insights/daily', async (req, res) => {
  try {
    const { date } = req.query;
    const { startOfDayUTC, endOfDayUTC } = getISTDayBounds(date);

    const logsToday = await DeviceLog.find({ 
      event: 'ONLINE', 
      timestamp: { $gte: startOfDayUTC, $lt: endOfDayUTC } 
    });
    const activeNow = await Device.find({ status: 'ACTIVE' });
    
    const uniqueDeviceIds = new Set();
    logsToday.forEach(log => uniqueDeviceIds.add(log.deviceId));
    
    const isToday = !date || date === new Date(Date.now() + 5.5 * 3600000).toISOString().split('T')[0];
    if (isToday) {
      activeNow.forEach(dev => uniqueDeviceIds.add(dev.deviceId));
    }

    const simAnalysis = await calculateSimUptimeAnalysis(date);
    
    res.json({ 
      totalActiveToday: uniqueDeviceIds.size,
      activeDevices: Array.from(uniqueDeviceIds),
      simAnalysis
    });
  } catch (err) {
    res.status(500).json({ error: 'Failed' });
  }
});

app.get('/api/insights/hourly', async (req, res) => {
  try {
    const { date } = req.query;
    const { startOfDayUTC, endOfDayUTC } = getISTDayBounds(date);
    const logs = await DeviceLog.find({ 
      timestamp: { $gte: startOfDayUTC, $lt: endOfDayUTC }, 
      event: 'ONLINE' 
    });
    
    const devicesPerHour = Array(24).fill(null).map(() => new Set());
    
    logs.forEach(log => {
      const istTime = new Date(log.timestamp.getTime() + (5.5 * 60 * 60 * 1000));
      const hour = istTime.getUTCHours();
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
    const { date } = req.query;
    const data = [];
    
    let istEndDay;
    if (date) {
      const parts = date.split('-');
      istEndDay = new Date(Date.UTC(parseInt(parts[0]), parseInt(parts[1]) - 1, parseInt(parts[2])));
    } else {
      const now = new Date();
      istEndDay = new Date(now.getTime() + (5.5 * 60 * 60 * 1000));
      istEndDay.setUTCHours(0, 0, 0, 0);
    }
    
    for (let i = 6; i >= 0; i--) {
      const d = new Date(istEndDay.getTime() - (i * 24 * 60 * 60 * 1000)); 
      const nextDay = new Date(d.getTime() + (24 * 60 * 60 * 1000));
      
      const dUTC = new Date(d.getTime() - (5.5 * 60 * 60 * 1000));
      const nextDayUTC = new Date(nextDay.getTime() - (5.5 * 60 * 60 * 1000));

      const logs = await DeviceLog.find({ event: 'ONLINE', timestamp: { $gte: dUTC, $lt: nextDayUTC }});
      const unique = new Set(logs.map(l => l.deviceId));
      
      const dayName = d.toLocaleDateString('en-US', { weekday: 'short', timeZone: 'UTC' });
      data.push({ name: dayName, active: unique.size });
    }
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: 'Failed' });
  }
});

app.get('/api/insights/rankings', async (req, res) => {
  try {
    const devices = await Device.find();
    const logs = await DeviceLog.find().sort({ timestamp: 1 });
    
    const logsByDevice = {};
    logs.forEach(log => {
      if (!logsByDevice[log.deviceId]) {
        logsByDevice[log.deviceId] = [];
      }
      logsByDevice[log.deviceId].push(log);
    });

    const now = new Date();
    const rankings = [];

    devices.forEach(device => {
      const devLogs = logsByDevice[device.deviceId] || [];
      let uptimePercent = 0;

      if (devLogs.length === 0) {
        uptimePercent = device.status === 'ACTIVE' ? 100 : 0;
      } else {
        const firstLogTime = devLogs[0].timestamp;
        const totalDuration = now.getTime() - firstLogTime.getTime();

        if (totalDuration <= 0) {
          uptimePercent = device.status === 'ACTIVE' ? 100 : 0;
        } else {
          let uptimeMs = 0;
          let currentPos = firstLogTime;
          let currentState = devLogs[0].event === 'ONLINE' ? 'OFFLINE' : 'ACTIVE';

          devLogs.forEach(log => {
            const logTime = log.timestamp;
            if (currentState === 'ACTIVE') {
              uptimeMs += logTime.getTime() - currentPos.getTime();
            }
            currentPos = logTime;
            currentState = log.event === 'ONLINE' ? 'ACTIVE' : 'OFFLINE';
          });

          if (currentState === 'ACTIVE') {
            uptimeMs += now.getTime() - currentPos.getTime();
          }

          uptimePercent = Math.round((uptimeMs / totalDuration) * 100);
        }
      }

      rankings.push({
        deviceId: device.deviceId,
        customerName: device.customerName || 'Unknown Device',
        status: device.status,
        uptimePercent
      });
    });

    const sortedMost = [...rankings].sort((a, b) => b.uptimePercent - a.uptimePercent);
    const sortedLeast = [...rankings].sort((a, b) => a.uptimePercent - b.uptimePercent);

    res.json({
      mostActive: sortedMost.slice(0, 5),
      leastActive: sortedLeast.slice(0, 5)
    });
  } catch (err) {
    res.status(500).json({ error: 'Failed to calculate rankings' });
  }
});

const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
  console.log(`Backend server running on port ${PORT}`);
});
