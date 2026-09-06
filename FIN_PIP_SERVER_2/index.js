import cron from "node-cron";
import express from "express";
import { Kafka } from "kafkajs";
import { createClient } from 'redis';
import fs from 'fs';
import dotenv from "dotenv";
import { json } from "stream/consumers";
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



const kafka = new Kafka({
  brokers: [process.env.KAFKA_URL],
  ...(kafkaSsl && { ssl: kafkaSsl }),
});

const client = createClient({ url: process.env.REDIS_URL });
client.on('error', err => console.error('Redis error:', err));
await client.connect();

const consumer = kafka.consumer({ groupId: 'anomaly-service' });
await consumer.connect();
await consumer.subscribe({ topic: 'anomaly', fromBeginning: false });


// HOW MESS LOOKS LIKE... [
//   {
//     ticker: 'AAPL',
//     date: '2026-03-03T14:30:00.000Z',
//     open: 263.4800109863281,
//     high: 265.55999755859375,
//     low: 260.1300048828125,
//     close: 263.75,
//     volume: 38568900
//   },
//   {
//     ticker: 'AAPL',
//     date: '2026-03-04T14:30:00.000Z',
//     open: 264.6499938964844,
//     high: 266.1499938964844,
//     low: 261.4200134277344,
//     close: 262.5199890136719,
//     volume: 39803100
//   },
//   {
//     ticker: 'AAPL',
//     date: '2026-03-05T14:30:00.000Z',
//     open: 260.7900085449219,
//     high: 261.55999755859375,
//     low: 257.25,
//     close: 260.2900085449219,
//     volume: 49658600
//   },
//   {
//     ticker: 'AAPL',
//     date: '2026-03-06T14:30:00.000Z',
//     open: 258.6300048828125,
//     high: 258.7699890136719,
//     low: 254.3699951171875,
//     close: 257.4599914550781,
//     volume: 41120000
//   },
//   {
//     ticker: 'AAPL',
//     date: '2026-03-09T13:30:00.000Z',
//     open: 255.69000244140625,
//     high: 261.1499938964844,
//     low: 253.67999267578125,
//     close: 259.8800048828125,
//     volume: 38153500
//   },
//   {
//     ticker: 'MSFT',
//     date: '2026-03-03T14:30:00.000Z',
//     open: 393.1400146484375,
//     high: 406.70001220703125,
//     low: 392.6700134277344,
//     close: 403.92999267578125,
//     volume: 38199200
//   },
//   {
//     ticker: 'MSFT',
//     date: '2026-03-04T14:30:00.000Z',
//     open: 401.2699890136719,
//     high: 411.0299987792969,
//     low: 400.30999755859375,
//     close: 405.20001220703125,
//     volume: 35808000
//   },
// ]

/*

in redis store anamoly_ohlcv:[
  { 

    ticker: 'AAPL',
    date: '2026-03-03T14:30:00.000Z',
    message: 'Anomaly detected for AAPL on 2026-03-03: close price changed by 5.00%',
    price: 263.75
  },
  {
  },  

]
*/
await consumer.run({
  eachMessage: async ({ message }) => {
  // message.value is just a trigger ('fetch'), data lives in Redis
  const batch = JSON.parse(await client.get('latest_ohlcv'));
  if (!batch) return console.log('No OHLCV data in Redis yet');

  const [prevBaselineRaw, existingAnomaliesRaw] = await Promise.all([
    client.get('ohlcv_baseline'),
    client.get('anomaly_ohlcv')
  ]);

  const existingAnomalies = existingAnomaliesRaw ? JSON.parse(existingAnomaliesRaw) : [];
  const newAnomalies = [];

  if (prevBaselineRaw) {
    const prevBaseline = JSON.parse(prevBaselineRaw);
    for (const curr of batch) {
      const prev = prevBaseline.find(o => o.ticker === curr.ticker);
      if (!prev) continue;
      const change = Math.abs((curr.close - prev.close) / prev.close) * 100;
      if (change > 5) {
        newAnomalies.push({
          ticker: curr.ticker,
          date: curr.date,
          message: `Anomaly detected for ${curr.ticker} on ${curr.date}: close price changed by ${change.toFixed(2)}%`,
          price: curr.close
        });
      }
    }
  }

  const anomalyMap = new Map(existingAnomalies.map(a => [`${a.ticker}_${a.date}`, a]));
  for (const a of newAnomalies) anomalyMap.set(`${a.ticker}_${a.date}`, a);
  const aggregated = Array.from(anomalyMap.values());


  await Promise.all([
  client.setEx('ohlcv_baseline', 600, JSON.stringify(batch)),
  client.setEx('anomaly_ohlcv', 600, JSON.stringify(newAnomalies))  // overwrite, not aggregate
]);

  console.log(`Anomalies: ${aggregated.length} total, ${newAnomalies.length} new`);
}
});

const app = express();
app.get("/", (req, res) => res.send("Hello World!"));

app.get("/anomalies", async (req, res) => {
  const data = await client.get('anomaly_ohlcv');
  res.json(data ? JSON.parse(data) : []);
});

app.get("/baseline", async (req, res) => {
  const data = await client.get('ohlcv_baseline');
  res.json(data ? JSON.parse(data) : []);
});

app.listen(3001, () => console.log("Consumer on 3001"));

cron.schedule("* * * * *", () => {
  console.log("Running a task every minute");
});