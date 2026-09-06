
# OHLCV Producer

Scheduled microservice in a distributed financial data pipeline. Fetches market data every minute, caches it in Redis, and fan-outs Kafka notifications to three downstream consumers.

---

## Architecture Role

```
Yahoo Finance API
      ↓
  s_1 (Producer)
      ├── Redis  ← stores full OHLCV payload (TTL: 10 min)
      └── Kafka  → topics: anomaly | indicator | persistence
                   message: "redis" (pointer, not payload)
```

Downstream consumers receive a pointer, then pull from Redis directly — avoids re-serializing large payloads over Kafka.

---

## Stack

| Layer     | Technology              |
|-----------|-------------------------|
| Runtime   | Node.js 20+ (ESM)       |
| Scheduler | node-cron (every 1 min) |
| Messaging | Kafka (SSL/SASL via Aiven) |
| Cache     | Redis (cache-aside)     |
| HTTP      | Express (manual trigger endpoint) |

---

## Environment Variables

| Variable       | Description                              |
|----------------|------------------------------------------|
| `KAFKA_URL`    | Kafka broker address                     |
| `REDIS_URL`    | Redis connection string                  |
| `SERVICE_CERT` | SSL client cert (Base64-encoded)         |
| `SERVICE_KEY`  | SSL private key (Base64-encoded)         |

Certs are decoded at startup and written to `/tmp` — no files needed in the repo.

---

## Running

```bash
# Local
npm install
node index.js

# Docker
docker build -t ohlcv-producer .
docker run --env-file .env ohlcv-producer
```

---

## Notes

- Kafka SSL certs passed as Base64 env vars for Render/cloud compatibility
- Redis key: `latest_ohlcv`, TTL 600s
- Kafka message value is the string `"redis"` — consumers use it as a signal to read from cache, not as data


