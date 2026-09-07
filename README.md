# Distributed Financial News & Filings Ingestion Pipeline

A decoupled, multi-worker ingestion pipeline powered by a single-node **Redpanda** streaming cluster, **Redpanda Console**, **Redis AOF** deduplication cache, and modular Python ingestion workers streaming data from Macro RSS feeds, GDELT 2.0 Doc API, and SEC EDGAR.

---

## Architecture Overview

```
                                  DATA SOURCES
         Macro RSS Feeds          GDELT 2.0 API           SEC EDGAR API
       (Bloomberg/FRED/etc.)     (Macro Keywords)       (8-K, 10-Q, 10-K)
                │                       │                       │
                ▼                       ▼                       ▼
        ┌──────────────┐        ┌──────────────┐        ┌──────────────┐
        │  worker-rss  │        │ worker-gdelt │        │  worker-sec  │
        │ (Poll: 3-5m) │        │ (Poll: 15m)  │        │ (Rate Limit: │
        │              │        │              │        │   <= 8 req/s)│
        └───────┬──────┘        └───────┬──────┘        └───────┬──────┘
                │                       │                       │
                │  Check & Set (Atomic: SET key 1 NX EX 604800) │
                ├───────────────────────┼───────────────────────┤
                │                       ▼                       │
                │             ┌───────────────────┐             │
                │             │    Redis Cache    │             │
                │             │  (appendonly:yes) │             │
                │             │   Port: 6379      │             │
                │             └───────────────────┘             │
                │                       │                       │
                ▼                       ▼                       ▼
          [acks=1, fast]          [acks=1, fast]        [acks=all, idempotent]
                │                       │                       │
                ▼                       ▼                       ▼
     Topic: financial.news.rss  Topic: financial.news.gdelt  Topic: financial.filings.sec
        └───────────────────────────────┼───────────────────────────────┘
                                        ▼
                            ┌────────────────────────┐
                            │    Redpanda Broker     │
                            │      (Port: 9092)      │
                            └───────────┬────────────┘
                                        ▼
                            ┌────────────────────────┐
                            │    Redpanda Console    │
                            │      (Port: 8080)      │
                            └────────────────────────┘
```

---

## Key Features

1. **Dedicated Topics**:
   - `financial.news.rss`: High-frequency macro headlines.
   - `financial.news.gdelt`: Global geopolitical and macroeconomic news from GDELT 2.0.
   - `financial.filings.sec`: High-priority corporate filings (8-K, 10-Q, 10-K).

2. **Differentiated Producer Reliability (Tiered ACKs)**:
   - **`fast` profile (`acks=1`)**: For RSS and GDELT to optimize latency and throughput.
   - **`critical` profile (`acks=all` / `-1`)**: For SEC filings with idempotence enabled (`enable.idempotence=True`) ensuring zero loss of critical corporate filings.

3. **Strict Rate Limiting for SEC EDGAR**:
   - Token bucket algorithm enforcing $\le 8\text{ req/s}$ (safely within SEC's 10 req/s limit).
   - Exponential backoff retry on HTTP 429 / 503 responses.
   - Compliant `User-Agent` format.

4. **Zero-Race Deduplication**:
   - Redis atomic command: `SET dedup:<source>:<id> 1 NX EX 604800` (7 days TTL).

---

## Getting Started

### 1. Start Infrastructure with Docker Compose

Start Redpanda, Redpanda Console, and Redis:
```powershell
docker compose up -d redpanda console redis
```

Verify service status:
```powershell
# Check running containers
docker compose ps

# Verify Redpanda cluster info
docker compose exec redpanda rpk cluster info

# Verify Redis ping and AOF persistence
docker compose exec redis redis-cli ping
docker compose exec redis redis-cli CONFIG GET appendonly
```

Access **Redpanda Console UI**:
👉 Open your browser at `http://localhost:8080`

---

### 2. Python Setup & Execution

Install dependencies:
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

#### Run Workers Independently:

- **Macro RSS Worker**:
  ```powershell
  python -m workers.rss_worker --once      # One-shot test
  python -m workers.rss_worker             # Continuous loop
  ```

- **GDELT Worker**:
  ```powershell
  python -m workers.gdelt_worker --once    # One-shot test
  python -m workers.gdelt_worker           # Continuous loop
  ```

- **SEC EDGAR Worker**:
  ```powershell
  python -m workers.sec_worker --once      # One-shot test
  python -m workers.sec_worker             # Continuous loop
  ```

- **Run All Ingestion Pipelines (One-Shot Test)**:
  ```powershell
  python main.py --once
  ```

---

### 3. Containerized Workers (Optional)

To run workers as background Docker containers alongside Redpanda and Redis:
```powershell
docker compose --profile workers up -d --build
```
