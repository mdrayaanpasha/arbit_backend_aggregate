// s_2/index.js — indicators consumer
import { Kafka } from 'kafkajs';
import express from 'express';
import fs from 'fs';
import dotenv from 'dotenv';

import { createClient } from 'redis';
dotenv.config();

import YahooFinance from 'yahoo-finance2';
const yahooFinance = new YahooFinance();

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

const seedHistory = async (ticker) => {
  try {
    const result = await yahooFinance.historical(ticker, {
      period1: '2025-09-01',
      period2: new Date().toISOString().split('T')[0],
      interval: '1d'
    });
    return result.map(d => d.close);
  } catch (e) {
    console.warn(`[${ticker}] Yahoo seed failed:`, e.message);
    return [];
  }
};
const client = createClient({ url: process.env.REDIS_URL });
await client.connect();

// --- Indicator math ---
const calcSMA = (closes, period) => {
  if (closes.length < period) return null;
  const slice = closes.slice(-period);
  return slice.reduce((a, b) => a + b, 0) / period;
};

const calcEMA = (closes, period) => {
  if (closes.length < period) return null;
  const k = 2 / (period + 1);
  let ema = closes.slice(0, period).reduce((a, b) => a + b, 0) / period;
  for (let i = period; i < closes.length; i++) {
    ema = closes[i] * k + ema * (1 - k);
  }
  return ema;
};

const calcRSI = (closes, period = 14) => {
  if (closes.length < period + 1) return null;
  const slice = closes.slice(-period - 1);
  let gains = 0, losses = 0;
  for (let i = 1; i < slice.length; i++) {
    const diff = slice[i] - slice[i - 1];
    diff >= 0 ? (gains += diff) : (losses += Math.abs(diff));
  }
  const rs = gains / (losses || 1);
  return 100 - 100 / (1 + rs);
};

// --- Consumer ---
const consumer = kafka.consumer({ groupId: 'indicator-service' });
await consumer.connect();
await consumer.subscribe({ topic: 'indicator', fromBeginning: false });

await consumer.run({
 eachMessage: async ({ message }) => {
  const batch = JSON.parse(await client.get('latest_ohlcv'));
  if (!batch) return console.log('No OHLCV data in Redis yet');

  const tickers = [...new Set(batch.map(d => d.ticker))];

  for (const ticker of tickers) {
    const historyRaw = await client.get(`indicators:history:${ticker}`);
    let history;

    if (historyRaw) {
      history = JSON.parse(historyRaw);
    } else {
      console.log(`[${ticker}] seeding from Yahoo Finance...`);
      history = await seedHistory(ticker);
    }

    const newCloses = batch
      .filter(d => d.ticker === ticker)
      .sort((a, b) => new Date(a.date) - new Date(b.date))
      .map(d => d.close);

    const closes = [...history, ...newCloses];

    const indicators = {
      ticker,
      updatedAt: new Date().toISOString(),
      SMA_14: calcSMA(closes, 14),
      SMA_50: calcSMA(closes, 50),
      EMA_14: calcEMA(closes, 14),
      EMA_50: calcEMA(closes, 50),
      RSI_14: calcRSI(closes, 14),
      dataPoints: closes.length
    };

    await Promise.all([
      client.setEx(`indicators:${ticker}`, 600, JSON.stringify(indicators)),
      client.setEx(`indicators:history:${ticker}`, 600, JSON.stringify(closes.slice(-100)))
    ]);

    console.log(`[${ticker}] indicators updated:`, indicators);
  }
}
});

// --- Express ---
const app = express();

app.get('/indicators', async (req, res) => {
  const keys = await client.keys('indicators:*');
  const filtered = keys.filter(k => !k.includes('history'));
  if (!filtered.length) return res.json([]);
  const values = await Promise.all(filtered.map(k => client.get(k)));
  res.json(values.map(v => JSON.parse(v)));
});

app.get('/indicators/:ticker', async (req, res) => {
  const data = await client.get(`indicators:${req.params.ticker.toUpperCase()}`);
  if (!data) return res.status(404).json({ error: 'No data for ticker' });
  res.json(JSON.parse(data));
});

app.listen(3002, () => console.log('Indicators consumer on 3002'));