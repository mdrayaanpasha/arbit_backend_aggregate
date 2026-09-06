// s_1/index.js — producer
import { Kafka } from 'kafkajs';
import express from 'express';
import fs from 'fs';
import cron from "node-cron";
import dotenv from 'dotenv';
import { fetchAll } from './services/datapull.js';
import { createClient } from 'redis';
dotenv.config();

const client = createClient({ url: process.env.REDIS_URL });
client.on('error', err => console.error('Redis error:', err));
await client.connect();


// Optional Kafka TLS — only for managed brokers (Aiven). Local Docker Kafka runs plaintext.
let kafkaSsl;
if (process.env.SERVICE_CERT && process.env.SERVICE_KEY) {
  fs.writeFileSync('/tmp/service.cert', Buffer.from(process.env.SERVICE_CERT, 'base64'));
  fs.writeFileSync('/tmp/service.key', Buffer.from(process.env.SERVICE_KEY, 'base64'));
  kafkaSsl = {
    ca: [fs.readFileSync('./ca.pem', 'utf-8')],
    cert: fs.readFileSync('/tmp/service.cert', 'utf-8'),
    key: fs.readFileSync('/tmp/service.key', 'utf-8'),
  };
}


const kafka = new Kafka({
  brokers: [process.env.KAFKA_URL],
  ...(kafkaSsl && { ssl: kafkaSsl }),
});

const producer = kafka.producer();
await producer.connect();

const app = express();
app.use(express.json());

app.post('/order', async (req, res) => {
  const ohlcv = await fetchAll();
  console.log(ohlcv);
  await producer.send({
    topic: 'orders',
    messages: [{ key: 'user-42', value: JSON.stringify(req.body) }]
  });
  res.json({ status: 'sent' });
});

app.listen(3000, () => console.log('Producer on 3000'));


cron.schedule("*/1 * * * *", async () => {
  const data = await fetchAll();
  await client.setEx('latest_ohlcv', 600, JSON.stringify(data));

  const topics = ["anomaly", "indicator", "persistence"];
  for (const topic of topics) {
    await producer.send({
      topic,
      messages: [{ key: 'ohlcv', value: "redis" }]
    });
  }
  console.log("Pushed OHLCV to all topics");
});