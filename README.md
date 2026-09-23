# Frozen Financial Sentiment Evaluation

Paper 1 is a reproducible evaluation of a frozen financial-headline sentiment
model, with explicit duplicate, story-weighting, and label-availability
controls. Start with [the research roadmap](ROADMAP.md) and
[the temporal contract and runbook](docs/research-protocol.md).

Research observations are captured **before live deduplication**, versioned in
schema v5, and exported using a conservative post-data-commit availability time.
Legacy Silver data is development material, not verified historical
availability evidence. Continual-learning utilities remain future extensions;
they are not part of Paper 1. Live collection and paper results are pending.

The infrastructure description below documents the existing operational path.
Its live Redis canonical-assignment race remains open and its flags are not
used by the independent research observation/replay path.

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
   - **`critical` profile (`acks=all` / `-1`)**: For SEC filings with idempotence enabled (`enable.idempotence=True`); durability still depends on broker replication, retention and recovery.

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

## Tier 2 exact-Jaccard verification

MinHash LSH (128 permutations, 32 bands of 4) retrieves candidates. Acceptance
uses exact `|A ∩ B| / |A ∪ B| >= 0.75` on the same normalized unigram/bigram
sets, including existing polarity anchors, that produced the signatures.
MinHash's estimated Jaccard no longer decides whether an article is merged.

V2 records are stored under `lsh:news:v2:event:<id>` and contain the ordered
headline, shingles, and original canonical ID. RSS/GDELT share V2 LSH buckets.
Records have the configured LSH TTL (24 hours by default); missing, expired,
or malformed records cannot authorize a match. Old V1 records and signature-only
buckets are left untouched and ignored. Coverage rebuilds as new events arrive.
Restart all workers together; mixed versions use different candidate windows.

Candidates must also pass the shared polarity guard, handling antonyms,
inflections, and short negation windows. Duplicate records preserve the original
canonical ID, including when Tier 3 targets a V2 lexical duplicate. This remains
a keyword heuristic: unknown verbs and subject-specific conflicts are limitations.
RSS/GDELT V2 lookup and insertion share a Redis lease, but it does not cover
Tier 3 reassignment or guarantee safety after expiry. It serializes Tier 2 and can
add queueing latency during bursts; Redis outages retain fail-open ingestion.

All retrieved candidates are checked in sorted ID order, with Redis reads
batched in groups of 256. The highest exact score wins, with lexical ID order
breaking ties. This removes the arbitrary 15-candidate cutoff, but does not
bound total work in a crowded bucket. Shingle payloads and full verification
have a pre-storage timing/payload-byte baseline in `docs/tier2-baseline.json`;
full resident-memory measurement and production throughput targets remain open.

LSH retrieval remains probabilistic. Tier 3 can still accept an event rejected
by Tier 2's lexical gate. Previously measured Tier 3 figures below predate this
Tier 2 change and are not new benchmarks.

Run all regressions:

```powershell
python -m unittest discover -s tests -v
```

The acceptance-gate tests mock candidate retrieval. `test_tier2_live.py` tests
actual Redis retrieval with unique prefixed keys and cleans up only its own keys.
Legacy Redis tests flush their designated test databases (14/15), so reserve
those databases for testing. These checks do not measure production throughput.

## Event handoff and delivery recovery

Workers use explicit event-specific dedup results and serialize public
`embedding` and `semantic_score` fields. Sync validates the complete CommonEvent
model, preserving `event_time` and `content_full` as well.

Workers first reserve exact IDs for `DEDUP_RESERVATION_SECONDS` (default 60).
Once a message is persisted in the SQLite outbox, its Redis marker is promoted
to the normal seven-day TTL. Local staging failures release the marker and
fuzzy payloads. A crash before staging leaves a short reservation rather than
a seven-day suppression. Set the reservation TTL above normal per-event
processing time; expiry can allow concurrent reprocessing.

`produce_event()` returns whether a message is durably staged. Delivery callbacks
remove acknowledged outbox records; queue or broker failures retain them.
Workers retry up to 100 pending records for their topic at each fetch cycle,
including after restart. Original wire payloads and ingestion times are preserved.
Statistics separate `queued`, acknowledged `published` during the cycle, and
`pending_delivery`. Published can include recovered messages from earlier cycles.

Delivery is at-least-once: a crash after broker acceptance but before local
acknowledgement can resend an event. Multiple producers recovering a shared
outbox can also resend it; the transactional writer now audits these redeliveries
without replacing the first stored Silver version.
Docker workers persist the outbox in the `ingestion-data` volume. Host workers
use `data/delivery-outbox.sqlite3`; `OUTBOX_PATH` can override it. Host and Docker
outboxes are separate unless configured to share storage. Removing the volume
loses pending messages. SEC's critical profile requires confluent-kafka rather
than silently falling back without its idempotence guarantee.

Recovery tests use a real temporary SQLite outbox and mocked Kafka callbacks;
they do not claim a live broker outage test.

## Continuous Bronze/Silver storage

Run `python -m lakehouse.sync` for the continuous writer, or add `--once` for
one total batch (not a historical per-topic replay). The stable default group
is `lakehouse-writer-v1`; commits follow atomic DuckDB transactions. The
configurable 500-message / 2-second defaults are provisional and untuned.

The writer acquires DuckDB ownership before joining Kafka; a second process
using the same file fails startup. Separate-process readers require stopping
that writer. Docker uses the shared `ingestion-data` volume; host data is
separate by default. Existing databases are backed up and migrated at startup.

Bronze replay detection uses the primary-key constraint atomically with
`ON CONFLICT DO NOTHING RETURNING`; it does not perform a separate pre-read.
The 100k/1m replay baselines and their `EXPLAIN ANALYZE` plans are recorded in
`docs/bronze-replay-baseline.json` and `docs/bronze-replay-baseline-1m.json`.

Bronze retains raw bytes and topic/partition/offset for every consumed message,
including invalid payloads. Outcomes audit rejections, conflicts, enrichment
and provenance. Invalid transport envelopes are quarantined with raw bytes and
cause the writer to halt before offset commits. Unresolved faults block restart
before Kafka consumer construction. List them with `python -m lakehouse.sync
--list-transport-faults`; resolve one with `--resolve-transport-fault FAULT_ID
--resolution-note "..."`. Valid payload rejections remain normal Bronze outcomes.
Silver preserves the first stored version. SEC identity and
filing-text differences under the same accession are always conflicts;
supplementary ticker/model annotations may be audited as enrichment.

See [storage behavior, verification and readiness limits](docs/storage-progress.md)
for field rules, deployment commands, migration evidence and live crash tests.
The captured-feed provenance and sustained-load targets remain open before
calling the entire data system ready for NLP.

Measure the active database directly with `python -m scripts.audit_data_readiness`.
The JSON report separates headline-prototype readiness from continuous-data
readiness. Create the fixed, source-balanced human review sheet with
`python -m scripts.export_nlp_label_sample`; labels remain blank until reviewed.

Every new worker event records headline and publication-time provenance. RSS and
GDELT API headlines are source text; GDELT GKG fallback headlines are URL slugs,
carry `nlp_eligible=false`, and are counted separately by the readiness audit.
Invalid source timestamps use ingestion time safely and are marked
`published_time_provenance=ingestion_fallback` rather than appearing source-verified.

Capture real cross-source provenance with
`python -m scripts.capture_syndication_fixture`. The report records live source
URLs, timestamps, the best candidates and whether a pair actually passed the
production 0.75 exact-Jaccard and polarity gates.

## Tier 3 Semantic Dedup — Measured Status (calibrated 2026-09)

All numbers below are measured, not asserted: `scripts/calibrate_tier3.py`
(63-pair labeled corpus, 7 bands, full P/R/F1 sweep in
`scripts/calibration_results.json`) and `python benchmark.py --tier3`.

- **Threshold**: `SEMANTIC_COSINE_THRESHOLD=0.76` (config.py). At τ=0.76 with
  gates active: **precision 1.000, recall 0.150, F1 0.261, 0 false merges** on
  the corpus (n=20 hand-authored positive pairs — a small-sample estimate,
  not a population recall rate). The cosine bands of same-event rewrites
  (0.25–0.92) and same-company-different-event pairs (0.44–0.74) overlap — no
  threshold is separable, so precision comes from the gates, not the cutoff.
- **Entity gate**: `utils/entity_extract.py` (~110 aliases incl. central
  banks). Coverage: 58% of corpus titles, 37% of live lakehouse titles
  (29% with the old regex). Semantics: no entities on either side → gate
  bypassed; both sides → merge allowed iff entity sets overlap. Gate
  false-reject rate on true duplicates: **0%** measured.
- **Polarity guard**: antonym-pair rule (`utils/polarity.py`) with inflection
  + irregular-form normalization and a 3-token negation window. Blocks
  beats/misses-class inversions that score up to 0.92 cosine; measured 0%
  false rejects on same-event pairs. Known accepted cost: cross-subject
  mixed pairs may over-block — measured **1 of the 3 hand-picked B7
  guard-cost pairs** (a curated adversarial band, not a production rate),
  pinned by `test_known_cross_subject_overblock_is_documented`.
- **Scan cap**: `SEMANTIC_SCAN_LIMIT=1000` newest-first. Worst case at a
  5,000-entry window: Tier 3 p50 366ms → **78ms**, full `check_dedup`
  407ms → **101ms** (8.5 events/s single-threaded). Parity with brute-force
  scan is test-enforced; the documented limitation is misses to canonicals
  older than the newest 1,000 window entries (Tier 2's 24h LSH window still
  covers them).
- **Embedding versioning**: `embedding_model` stamped on every insert;
  pre-existing rows backfilled at init; `query_similar_news(embedding_model=...)`
  is an explicit opt-in filter (never default-on).
- **Known limitation**: recall 0.15 on low-overlap paraphrases (n=20 pairs) —
  Tier 3 is a precision-first safety net; Tier 2 LSH remains the primary dedup tier.
