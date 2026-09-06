# Indicators Consumer

Kafka consumer that computes SMA, EMA, and RSI for each ticker on every OHLCV update. Seeds 100-day price history from Yahoo Finance on first run; appends live closes on subsequent runs.

---

## Architecture Role

```
Kafka (topic: indicator)
        ↓ trigger ("redis")
s_3 reads latest_ohlcv from Redis
        ↓ per ticker: load or seed 100-day close history
        ↓ append new closes → compute SMA/EMA/RSI
Redis ← writes: indicators:{TICKER} + indicators:history:{TICKER} (TTL: 10 min)
```

---

## Indicators Computed

| Indicator | Period | Minimum Data Required |
|-----------|--------|-----------------------|
| SMA       | 14, 50 | 14 / 50 closes        |
| EMA       | 14, 50 | 14 / 50 closes        |
| RSI       | 14     | 15 closes             |

Returns `null` if insufficient history — handled gracefully, won't crash.

---

## History Seeding

On first message per ticker, if `indicators:history:{TICKER}` is absent in Redis:

1. Fetches daily closes from Yahoo Finance (`2025-09-01` → today)
2. Stores last 100 closes under `indicators:history:{TICKER}`
3. Subsequent runs append from `latest_ohlcv` and re-slice to 100

> Seeding only happens once per ticker per cold start. After that, history is maintained in Redis.

---

## Redis Key Schema

| Key | Content | TTL |
|-----|---------|-----|
| `latest_ohlcv` | Full OHLCV batch (written by s_1) | 600s |
| `indicators:{TICKER}` | SMA/EMA/RSI output + metadata | 600s |
| `indicators:history:{TICKER}` | Last 100 closes (float array) | 600s |

---

## Stack

| Layer     | Technology                          |
|-----------|-------------------------------------|
| Runtime   | Node.js 20+ (ESM)                   |
| Messaging | Kafka consumer (group: `indicator-service`) |
| Cache     | Redis (read + write)                |
| Data seed | yahoo-finance2                      |
| HTTP      | Express (inspection endpoints)      |

---

## Environment Variables

| Variable       | Description                      |
|----------------|----------------------------------|
| `KAFKA_URL`    | Kafka broker address             |
| `REDIS_URL`    | Redis connection string          |
| `SERVICE_CERT` | SSL client cert (Base64-encoded) |
| `SERVICE_KEY`  | SSL private key (Base64-encoded) |

---

## Endpoints

| Method | Path                   | Returns                              |
|--------|------------------------|--------------------------------------|
| GET    | `/indicators`          | All tickers with computed indicators |
| GET    | `/indicators/:ticker`  | Single ticker (e.g. `/indicators/AAPL`) |

---

## Running

```bash
# Local
npm install
node index.js

# Docker
docker build -t indicators-consumer .
docker run -p 3002:3002 --env-file .env indicators-consumer
```

---

## Known Behavior

- Yahoo Finance seed hardcoded to `2025-09-01` — change for different history windows
- History capped at last 100 closes per ticker to bound memory
- `null` indicators are expected on first run if seed returns fewer closes than the period requires
- TTL of 600s matches producer cadence — keys expire if producer stops
