# Week 3 implementation progress

This is the week 3 slice of Phase 0 (roadmap weeks 1–5).

| Planned work | Status | Implementation / evidence |
|---|---|---|
| Exact Tier 2 Jaccard verification | Implemented | Shared shingles; exact set intersection/union; boundary regressions |
| Explicit Tier 2 polarity protection | Implemented | Shared antonym/negation guard; long-headline regressions |
| RSS/GDELT cross-source clustering behavior | Implemented for synthetic fixtures | Shared V2 buckets; real-feed syndication provenance remains unvalidated |
| Original canonical IDs | Implemented for concurrent V2 processing | Redis assignment lease serializes lookup/insertion; duplicates retain root IDs |
| Deterministic candidate verification | Implemented | All candidates checked in sorted batches of 256; live regression with only qualifying match after candidate 256 |
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

## Next storage milestone implementation

The continuous writer, transaction/offset boundary, raw Bronze evidence,
immutable Silver, rejections and field-level audits are implemented and verified.
The writer is packaged in Compose with persistent storage and enforces ownership
before Kafka construction. See [storage progress](storage-progress.md) for the
complete behavior, migration checks and real broker crash/rebalance tests.

These changes close the storage implementation slice. Remaining Phase 0
readiness requires captured RSS/GDELT pair provenance, sustained feed-rate/resource
measurement and explicit throughput/
latency targets. The 500-message / 2-second batching defaults remain provisional.

Transport-coordinate corruption is now a durable stop condition: the batch and
fault evidence are persisted, no Kafka offsets are committed, and unresolved
faults block consumer construction after restart. Resolution requires an audited
operator note. `scripts.audit_data_readiness` now reports the measurable Phase 0
gates, and `scripts.export_nlp_label_sample` produces the fixed human-label sheet.

## Limits to carry forward

- Keyword polarity guards cannot prove universal semantic safety.
- The global Redis assignment lease prevents concurrent V2 root creation but can
  add queueing latency during bursts; its load impact remains to be measured.
- All-candidate checks have a timing/payload-byte baseline; crowded-bucket
  readiness and full resident-memory measurement remain open.
- V2 is a new candidate window: restart workers together; coverage repopulates.
- Outbox delivery is at-least-once; acknowledgements can be lost across crashes,
  and multiple producers can resend shared pending records.
- The 60-second reservation is configurable and should exceed processing time.
- Older records and clusters are not retroactively repaired by this change.
- Docker and host outboxes use separate storage by default.

FinBERT belongs to Phase 0.5 after the storage and validation checks above.

## Real provenance capture attempt

`python -m scripts.capture_syndication_fixture` fetched 90 live RSS entries on
2026-09-15. GDELT DOC returned HTTP 429 and the Investing.com RSS endpoint failed
TLS, so the report contains no qualifying pair and the real RSS/GDELT gate remains
open. See `docs/real-syndication-validation.json`; rerunning replaces the evidence
with the latest observed result without weakening the 0.75 production threshold.
