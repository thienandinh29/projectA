# Checkpoint 1 follow-up — 2026-09-28

Implemented three limited changes:

- `scripts.drain_delivery` retries the existing SQLite outbox without fetching
  news, then checks writer commits on every configured topic partition. Timeout,
  unknown offsets, retention gaps and unmonitored outbox topics do not certify a
  completed drain. Stop collectors first; keep the writer running until success.
- Collection provenance is captured once per producer run at first observation
  and carried inside observation snapshots. The exporter lists the runs attached
  to exported first versions separately from its own environment. Older records
  remain unknown. Application hashes cover the listed files, including feed/query
  definitions; declared Git revisions are not treated as verified identity.
- The September 15 host audit is named and labeled as historical evidence.

This adds no database migration or new service. Run metadata is repeated in wire
snapshots to make each delivery self-contained; it does not change content/version
identity. It is not a complete session-coverage ledger: attempts that capture no
observations still require the later coverage work. First-version export provenance
does not enumerate all redelivery runs; their payloads remain in Bronze.

## Verification

The following command passed **74 tests** locally on September 28, 2026:

```text
python -m unittest tests.test_checkpoint1_delivery tests.test_delivery_recovery tests.test_worker_handoff tests.test_research_pipeline tests.test_data_readiness tests.test_storage tests.test_writer tests.test_pit_leakage -q
```

Regressions include 600 staged messages delivered across multiple batches after
a simulated broker failure and process restart; original-byte preservation;
writer lag and unavailable-offset refusal; unmonitored topics; legacy observation
hash compatibility; and collection provenance surviving storage/export despite
different exporter settings. Delivery tests use mocked Kafka callbacks/offsets;
they are not a new live Docker outage/recovery experiment.

Compose configuration validation and `git diff --check` passed. Docker emitted
local config-file permission warnings during configuration validation; no live
Docker collection or authoritative readiness audit was performed for this change.

Before the next collection session, rebuild collector, writer and tools images
together, then use the guarded commands in `docs/research-protocol.md`. A successful
drain includes records durably rejected by the writer; inspect the audit as well.
These fixes improve snapshot evidence, not dataset size or research readiness.

## Review against the roadmap

- Checkpoint 1: the diff adds an executable quiet-snapshot boundary and separates
  collector evidence from exporter evidence. The authoritative volume and schema
  stay unchanged; the renamed host report cannot be mistaken for a current audit.
- Checkpoint 2: run identity and settings are an initial contribution only.
  Request outcomes, empty/failed attempts, session end and interruption evidence
  still need work. This diff does not mark that checkpoint complete.
- Checkpoint 3: mocked recovery tests establish local behavior, not live operation.
  Scheduled Docker collection, live recovery and count reconciliation remain open.
- Checkpoints 4–5: eligibility review, annotation, frozen predictions and research
  evaluation are unchanged. No new model or research-readiness claim is introduced.

Compatibility: existing observation hashes remain valid and schema v5 is retained.
The exported manifest is now version 2 with separate provenance sections. Rebuild
the writer together with collectors: an older strict payload validator will reject
the newly added optional collection metadata field.
