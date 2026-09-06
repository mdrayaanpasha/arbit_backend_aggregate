# Persistence Consumer

Kafka consumer that drains `latest_ohlcv` and per-ticker indicators from Redis into PostgreSQL (via Prisma). Also serves as the primary data API for the React dashboard — combining historical DB records with live Redis state in a single response.

---

## Architecture Role

```
Kafka (topic: persistence)
        ↓ trigger ("redis")
s_4 reads latest_ohlcv + indicators:{TICKER} from Redis
        ↓ upsert to PostgreSQL (Prisma)
        ↓ duplicate rows silently skipped (Unique constraint)

GET /data or /data/:ticker
        ↓ PostgreSQL (historical)  +  Redis (latest indicators, anomalies)
        → unified JSON response to dashboard
```

---

## Data Written to PostgreSQL

| Table        | Source                        | Key                     |
|--------------|-------------------------------|-------------------------|
| `OHLVCData`  | `latest_ohlcv` (Redis)        | ticker + timestamp      |
| `Indicators` | `indicators:{TICKER}` (Redis) | ticker + updatedAt      |

Unique constraint violations are swallowed silently — re-runs are idempotent.

---

## API Endpoints

| Method | Path             | Returns                                                              |
|--------|------------------|----------------------------------------------------------------------|
| GET    | `/data`          | Last 100 OHLCV + indicators rows (all tickers) + live anomalies      |
| GET    | `/data?ticker=X` | Same, filtered to one ticker                                         |
| GET    | `/data/:ticker`  | 50 rows each from DB + live Redis indicators + filtered anomalies    |

`/data/:ticker` response shape:
```json
{
  "ticker": "AAPL",
  "ohlcv": [...],
  "indicators": {
    "historical": [...],
    "latest": { "SMA_14": ..., "EMA_14": ..., "RSI_14": ... }
  },
  "anomalies": [...]
}
```

---

## Stack

| Layer       | Technology                              |
|-------------|-----------------------------------------|
| Runtime     | Node.js 20+ (ESM)                       |
| Messaging   | Kafka consumer (group: `persistence-group`) |
| Cache       | Redis (read source)                     |
| Database    | PostgreSQL via Prisma                   |
| HTTP        | Express + CORS                          |

---

## Environment Variables

| Variable       | Description                      |
|----------------|----------------------------------|
| `KAFKA_URL`    | Kafka broker address             |
| `REDIS_URL`    | Redis connection string          |
| `DATABASE_URL` | PostgreSQL connection string     |
| `SERVICE_CERT` | SSL client cert (Base64-encoded) |
| `SERVICE_KEY`  | SSL private key (Base64-encoded) |

---

## Running

```bash
# Local
npm install
npx prisma migrate deploy
node index.js

# Docker
docker build -t persistence-consumer .
docker run -p 3003:3003 --env-file .env persistence-consumer
```

---

## Known Behavior

- Anomalies are **never persisted to DB** — read live from Redis only; lost on TTL expiry
- If `indicators:{TICKER}` hasn't been written by s_3 yet, that ticker is skipped silently
- `/data` and `/data/:ticker` both hit PostgreSQL + Redis on every request — no additional caching layer
