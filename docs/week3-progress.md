# Week 3 implementation progress

This is the week 3 slice of Phase 0 (roadmap weeks 1–5).

| Planned work | Status | Implementation / evidence |
|---|---|---|
| Exact Tier 2 Jaccard verification | Implemented | Shared shingles; exact set intersection/union; boundary regressions |
| Explicit Tier 2 polarity protection | Implemented | Shared antonym/negation guard; long-headline regressions |
| RSS/GDELT syndication clustering | Implemented | Shared V2 buckets; live Redis cross-source tests |
| Original canonical IDs | Implemented for sequential V2 processing | Duplicate records retain root IDs; semantic targets resolve V2 cluster IDs |
| Deterministic candidate verification | Implemented | All candidates checked in sorted batches of 256 |
| Complete event handoff | Implemented | Public embeddings/scores; complete Pydantic wire parsing; SQLite-independent DuckDB roundtrip and PiT tests |
| Recoverable Kafka delivery failures | Implemented | Durable SQLite outbox; topic-specific retries; acknowledgement-only deletion |
| Distinguish queued and published | Implemented | Published counts actual callbacks; pending count exposed |
| Recover after crash before outbox staging | Implemented | Short Redis reservation promoted only after durable staging |

Validation commands:

```powershell
python -m unittest discover -s tests -v
python -m compileall -q workers utils lakehouse models
docker compose config --quiet
```

Tests use actual Redis and temporary DuckDB/SQLite files. Kafka failures are
simulated through callbacks. No production feeds or topics are populated by
the new tests. Existing legacy tests clear Redis test databases 14/15.

Validated on 2026-09-14: the full 84-test suite passed, followed by three
additional worker tests (87 passing tests total). Python compilation and
Compose configuration validation passed. Docker printed a warning that it
could not read the user's credential config, but Compose validation exited
successfully. Containers were not rebuilt or restarted during this change.

## Remaining Phase 0 work for weeks 4–5

1. Replace the bounded replay sync with a continuous micro-batch writer.
2. Use a stable Kafka consumer group and commit offsets after DB transactions.
3. Populate Bronze with raw messages and topic/partition/offset provenance.
4. Make Silver replay-safe without changing original ingestion history;
   define how revised content is versioned rather than blindly replaced.
5. Persist malformed events and parsing errors for review and recovery.
6. Package the writer with persistent storage; define supported reader access.
7. Validate live broker failures, writer restart, replay, delayed events,
   and end-to-end PiT queries. Measure lag, throughput, latency, and memory.

## Limits to carry forward

- Keyword polarity guards cannot prove universal semantic safety.
- Concurrent Redis LSH lookup/insertion can create multiple canonical IDs;
  atomic distributed clustering is separate hardening work.
- All-candidate checks need benchmarks for crowded buckets and memory use.
- V2 is a new candidate window: restart workers together; coverage repopulates.
- Outbox delivery is at-least-once; acknowledgements can be lost across crashes,
  and multiple producers can resend shared pending records.
- The 60-second reservation is configurable and should exceed processing time.
- Older records and clusters are not retroactively repaired by this change.
- Docker and host outboxes use separate storage by default.

FinBERT belongs to Phase 0.5 after the storage and validation checks above.
