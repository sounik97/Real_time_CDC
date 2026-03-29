/**
 * 🚀 CDC Dashboard Server
 * Real-time WebSocket server for streaming CDC events to the browser
 */

const express = require('express');
const { WebSocketServer } = require('ws');
const { Kafka, CompressionTypes, CompressionCodecs } = require('kafkajs');
const SnappyCodec = require('kafkajs-snappy');
const http = require('http');
const path = require('path');

// Register Snappy compression codec
CompressionCodecs[CompressionTypes.Snappy] = SnappyCodec;

// Configuration
const PORT = process.env.PORT || 3000;
const KAFKA_BROKERS = (process.env.KAFKA_BOOTSTRAP_SERVERS || 'kafka:9092').split(',');

// Express app
const app = express();
app.use(express.static(path.join(__dirname, 'public')));

// Create HTTP server
const server = http.createServer(app);

// WebSocket server
const wss = new WebSocketServer({ server });

// Store connected clients
const clients = new Set();

// Event statistics
const stats = {
  totalEvents: 0,
  creates: 0,
  updates: 0,
  deletes: 0,
  reads: 0,
  eventsByTable: {},
  recentEvents: [],
  startTime: Date.now()
};

// Kafka consumer
const kafka = new Kafka({
  clientId: 'cdc-dashboard',
  brokers: KAFKA_BROKERS,
  retry: {
    initialRetryTime: 1000,
    retries: 20
  }
});

const consumer = kafka.consumer({ groupId: 'cdc-dashboard-group' });

// WebSocket connection handler
wss.on('connection', (ws) => {
  console.log('📱 New client connected');
  clients.add(ws);
  
  // Send current stats to new client
  ws.send(JSON.stringify({ type: 'stats', data: stats }));
  
  // Send recent events
  stats.recentEvents.forEach(event => {
    ws.send(JSON.stringify({ type: 'event', data: event }));
  });
  
  ws.on('close', () => {
    console.log('📴 Client disconnected');
    clients.delete(ws);
  });
  
  ws.on('error', (error) => {
    console.error('WebSocket error:', error);
    clients.delete(ws);
  });
});

// Broadcast to all clients
function broadcast(message) {
  const data = JSON.stringify(message);
  clients.forEach(client => {
    if (client.readyState === 1) { // OPEN
      client.send(data);
    }
  });
}

// Process CDC event
function processEvent(topic, message) {
  if (!message) return;

  const table = topic.replace('cdc.', '');

  // Handle Debezium envelope structure
  const operation = message.op || message.__op || 'r';
  const before = message.before || null;
  const after = message.after || null;
  const source = message.source || {};
  const ts_ms = message.ts_ms || Date.now();

  // Update stats
  stats.totalEvents++;

  switch (operation) {
    case 'c': stats.creates++; break;
    case 'u': stats.updates++; break;
    case 'd': stats.deletes++; break;
    case 'r': stats.reads++; break;
  }

  stats.eventsByTable[table] = (stats.eventsByTable[table] || 0) + 1;

  // Create event object with before/after
  const event = {
    id: stats.totalEvents,
    table,
    operation,
    before,
    after,
    source,
    timestamp: new Date(ts_ms).toISOString()
  };

  // Store recent events (keep last 50)
  stats.recentEvents.push(event);
  if (stats.recentEvents.length > 50) {
    stats.recentEvents.shift();
  }

  // Broadcast event to all clients
  broadcast({ type: 'event', data: event });
  broadcast({ type: 'stats', data: stats });

  console.log(`📨 Event: ${table} - ${operation}`);
}

// Wait for Kafka with retries
async function waitForKafka(maxRetries = 30) {
  for (let i = 0; i < maxRetries; i++) {
    try {
      const admin = kafka.admin();
      await admin.connect();
      await admin.disconnect();
      console.log('✅ Kafka is ready');
      return true;
    } catch (error) {
      console.log(`⏳ Waiting for Kafka... (${i + 1}/${maxRetries})`);
      await new Promise(r => setTimeout(r, 5000));
    }
  }
  return false;
}

// Wait for topics
async function waitForTopics(timeout = 120000) {
  const start = Date.now();
  const admin = kafka.admin();
  
  while (Date.now() - start < timeout) {
    try {
      await admin.connect();
      const topics = await admin.listTopics();
      await admin.disconnect();
      
      const cdcTopics = topics.filter(t => t.startsWith('cdc.'));
      if (cdcTopics.length > 0) {
        console.log('✅ Found CDC topics:', cdcTopics);
        return cdcTopics;
      }
      
      console.log('⏳ Waiting for CDC topics...');
      await new Promise(r => setTimeout(r, 5000));
    } catch (error) {
      console.log('⏳ Waiting for topics...');
      await new Promise(r => setTimeout(r, 5000));
    }
  }
  
  return [];
}

// Start Kafka consumer
async function startKafkaConsumer() {
  console.log('🔄 Starting Kafka consumer...');
  
  // Wait for Kafka
  if (!await waitForKafka()) {
    console.error('❌ Failed to connect to Kafka');
    return;
  }
  
  // Wait for topics
  const topics = await waitForTopics();
  
  if (topics.length === 0) {
    console.log('⚠️ No CDC topics found, using defaults');
    topics.push('cdc.customers', 'cdc.products', 'cdc.orders', 'cdc.order_items', 'cdc.inventory');
  }
  
  try {
    await consumer.connect();
    console.log('✅ Connected to Kafka');
    
    // Subscribe to topics
    await consumer.subscribe({ 
      topics,
      fromBeginning: true 
    });
    
    console.log('👂 Subscribed to topics:', topics);
    
    // Run consumer
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const value = message.value ? JSON.parse(message.value.toString()) : null;
          processEvent(topic, value);
        } catch (error) {
          console.error('Error processing message:', error);
        }
      }
    });
    
  } catch (error) {
    console.error('Kafka consumer error:', error);
    // Retry after delay
    setTimeout(startKafkaConsumer, 10000);
  }
}

// API endpoints
app.get('/api/stats', (req, res) => {
  res.json(stats);
});

app.get('/api/events', (req, res) => {
  res.json(stats.recentEvents);
});

app.get('/health', (req, res) => {
  res.json({ status: 'healthy', uptime: Date.now() - stats.startTime });
});

// Start server
server.listen(PORT, () => {
  console.log(`
╔════════════════════════════════════════════════════════════════╗
║                                                                ║
║   🚀 CDC Dashboard Server                                      ║
║                                                                ║
║   📊 Dashboard: http://localhost:${PORT}                         ║
║   🔌 WebSocket: ws://localhost:${PORT}                           ║
║   📡 API: http://localhost:${PORT}/api/stats                     ║
║                                                                ║
╚════════════════════════════════════════════════════════════════╝
  `);
  
  // Start Kafka consumer
  startKafkaConsumer();
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('🛑 Shutting down...');
  await consumer.disconnect();
  server.close();
  process.exit(0);
});
