import { createClient } from 'redis';
import { Kafka } from 'kafkajs';
import { OHLVCData, Indicators } from './services/store_data.js';
import { PrismaClient } from '@prisma/client';
import express from 'express';
import cors from "cors";

import fs from "fs";
import dotenv from 'dotenv';
dotenv.config();

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

const prisma = new PrismaClient();
const client = createClient({ url: process.env.REDIS_URL });
await client.connect();

const kafka = new Kafka({
  brokers: [process.env.KAFKA_URL],
  ...(kafkaSsl && { ssl: kafkaSsl }),
});

const consumer = kafka.consumer({ groupId: 'persistence-group' });
await consumer.connect();
await consumer.subscribe({ topic: 'persistence', fromBeginning: false });

await consumer.run({
  eachMessage: async ({ message }) => {
    const batch = JSON.parse(await client.get('latest_ohlcv'));
    if (!batch) return console.log('No OHLCV data in Redis yet');

    for (const row of batch) {
      try {
        await OHLVCData(row.ticker, new Date(row.date), row.open, row.high, row.low, row.volume, row.close);
      } catch (e) {
        if (!e.message.includes('Unique constraint')) console.error(`OHLCV insert failed [${row.ticker}]:`, e.message);
      }
    }
    console.log(`Persisted ${batch.length} OHLCV rows`);

    const tickers = [...new Set(batch.map(d => d.ticker))];
    for (const ticker of tickers) {
      const raw = await client.get(`indicators:${ticker}`);
      if (!raw) { console.log(`[${ticker}] No indicators in Redis yet, skipping`); continue; }
      const ind = JSON.parse(raw);
      try {
        await Indicators(ind.ticker, new Date(ind.updatedAt), ind.RSI_14, ind.EMA_14, ind.SMA_14);
      } catch (e) {
        if (!e.message.includes('Unique constraint')) console.error(`Indicators insert failed [${ticker}]:`, e.message);
      }
      console.log(`[${ticker}] indicators persisted`);
    }
  }
});

// --- Express ---
const app = express();
app.use(cors());
app.get('/data', async (req, res) => {
  const { ticker } = req.query;
  const where = ticker ? { ticker: ticker.toUpperCase() } : {};

  const [ohlcv, indicators, anomaliesRaw] = await Promise.all([
    prisma.ohlvcData.findMany({ where, orderBy: { timestamp: 'desc' }, take: 100 }),
    prisma.indicators.findMany({ where, orderBy: { timestamp: 'desc' }, take: 100 }),
    client.get('anomaly_ohlcv')
  ]);

  res.json({
    ohlcv,
    indicators,
    anomalies: anomaliesRaw ? JSON.parse(anomaliesRaw) : []
  });
});

app.get('/data/:ticker', async (req, res) => {
  const ticker = req.params.ticker.toUpperCase();

  const [ohlcv, indicators, redisIndicators, anomaliesRaw] = await Promise.all([
    prisma.ohlvcData.findMany({ where: { ticker }, orderBy: { timestamp: 'desc' }, take: 50 }),
    prisma.indicators.findMany({ where: { ticker }, orderBy: { timestamp: 'desc' }, take: 50 }),
    client.get(`indicators:${ticker}`),
    client.get('anomaly_ohlcv')
  ]);

  res.json({
    ticker,
    ohlcv,
    indicators: {
      historical: indicators,
      latest: redisIndicators ? JSON.parse(redisIndicators) : null
    },
    anomalies: anomaliesRaw
      ? JSON.parse(anomaliesRaw).filter(a => a.ticker === ticker)
      : []
  });
});

app.listen(3003, () => console.log('Persistence service on 3003'));