
# Anomaly Detection Consumer

Kafka consumer that triggers on new OHLCV data, compares close prices against a stored baseline, and flags tickers with >5% price swings. Results are written back to Redis for downstream access.

---

## Architecture Role

```
Kafka (topic: anomaly)
        ↓ trigger ("redis")
s_2 reads latest_ohlcv from Redis
        ↓ compare against ohlcv_baseline
        ↓ flag tickers where |Δclose| > 5%
Redis ← writes: anomaly_ohlcv + ohlcv_baseline (TTL: 10 min)
```

---

## Detection Logic

| Step | Action |
|------|--------|
| 1. Receive | Kafka message on `anomaly` topic (pointer, not payload) |
| 2. Fetch | Read `latest_ohlcv` from Redis |
| 3. Compare | Diff `close` vs `ohlcv_baseline` per ticker |
| 4. Flag | `\|Δclose / prev_close\| > 5%` → anomaly |
| 5. Write | Update `anomaly_ohlcv` + `ohlcv_baseline` in Redis |

Deduplication via `ticker_date` composite key — re-runs won't produce duplicate anomaly entries.

---

## Stack

| Layer     | Technology                   |
|-----------|------------------------------|
| Runtime   | Node.js 20+ (ESM)            |
| Messaging | Kafka consumer (group: `anomaly-service`) |
| Cache     | Redis (read source + write target) |
| HTTP      | Express (inspection endpoints) |

---

## Environment Variables

| Variable       | Description                        |
|----------------|------------------------------------|
| `KAFKA_URL`    | Kafka broker address               |
| `REDIS_URL`    | Redis connection string            |
| `SERVICE_CERT` | SSL client cert (Base64-encoded)   |
| `SERVICE_KEY`  | SSL private key (Base64-encoded)   |

---

## Endpoints

| Method | Path         | Returns                                      |
|--------|--------------|----------------------------------------------|
| GET    | `/anomalies` | All tickers with >5% swing (current window)  |
| GET    | `/baseline`  | Previous OHLCV state used for comparison     |

---

## Running

```bash
# Local
npm install
node index.js

# Docker
docker build -t anomaly-detector .
docker run -p 3001:3001 --env-file .env anomaly-detector
```

---

## Known Behavior

- No baseline on first run → no anomalies detected, baseline is seeded for next cycle
- `anomaly_ohlcv` is **overwritten** each run (not appended) — reflects current window only
- TTL on both Redis keys: 600s — aligns with producer's cache window


