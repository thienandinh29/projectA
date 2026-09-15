# Continuous data storage milestone

The next data milestone implements one continuous transactional writer for RSS,
GDELT and SEC. This completes the storage implementation slice, rather than all
Phase 0 readiness or NLP work. Existing feed workers remain responsible for
deduplication and publication.

## Replay and deduplication boundary

Feed reissues pass through the worker's existing dedup tiers (SEC uses exact
dedup); a Kafka redelivery or outbox retry preserves its previously computed
payload and bypasses those tiers in the writer. Bronze's topic/partition/offset
key provides durable delivery idempotency; Silver's news ID or SEC accession
provides first-version storage identity. These are deliberately separate from
Redis's expiring exact markers and clustering windows. The writer never calls
Redis or infers a new cluster. Bronze captures published Kafka evidence, not
feed items that workers suppressed before publication.

Replaying the same offset changes nothing. An already stored event at a new
offset adds Bronze evidence and one outcome, with no Silver replacement.
Conflicts, enrichment and provenance are independently recorded; one outcome
can contain both conflicts and enrichment, with status `conflict` taking priority.

## Comparison rules

Comparisons use the original validated event snapshot (`first_event`) rather
than DuckDB's rounded FLOAT projections. Text comparisons collapse whitespace
but retain case, punctuation and numbers; null and blank text are equivalent.
Timestamp comparisons use UTC instants, so equivalent timezone representations
are not changes.

| Fields | Classification |
|---|---|
| Title, publication time, duplicate flag, canonical cluster reference | Conflict when changed; novel null/self cluster references are equivalent |
| News full text or same-source snippet | Missing to populated: enrichment; different nonempty text: conflict; missing on reissue: provenance |
| RSS/GDELT cross-source snippets | Provenance, since feed summaries can differ |
| SEC filing text, including snippet | Any normalized difference is a conflict, including missing to populated or populated to missing |
| SEC CIK, form type, company name under the same accession | Conflict after CIK zero-padding, form uppercasing, and company whitespace normalization, including adding/removing identity fields |
| Event time | Missing to known: enrichment; known to different known: conflict; omitted on reissue: provenance |
| Ticker annotations, embedding, semantic score, schema version | Supplementary enrichment audit; ticker order, duplicates and case are ignored |
| Source, URL, ingestion time and remaining metadata | Provenance audit |

**SEC filings participate in supplementary annotation enrichment only; changes
to filing identity or filing text are always audited as conflicts and never
silently enrich or replace Silver.** A different accession is a different filing;
a payload whose accession disagrees with its event ID is rejected. Malformed
identity-field types are rejected rather than aborting the whole batch.

Enrichment audits do not automatically populate missing Silver fields. A future
versioned annotation channel must define its own point-in-time availability.

## Writer ownership, transactions and deployment

`LakehouseWriter` obtains the canonical path's writable DuckDB connection and
completes migration before constructing/subscribing a Kafka consumer. DuckDB's
process lock makes a second process targeting that file fail before joining the
consumer group. Exactly one writer process per file is supported; separate
files require separate consumer groups if each is intended to receive all data.

The stable default group is `lakehouse-writer-v1`. Automatic commits and offset
storage are disabled. Each batch atomically writes Bronze, processing outcomes,
and new Silver rows. Only after DuckDB commits does the writer synchronously
commit each consumed partition's maximum offset plus one. Transaction failures
roll back and stop; offset commit failures stop and replay the durable batch on
restart. Revocation/loss discards buffered records for revoked partitions, leaving
their offsets uncommitted. Graceful shutdown flushes pending owned records.

Malformed payloads with valid Kafka coordinates are durable Bronze rejections.
Malformed transport coordinates are written to `lakehouse_transport_faults` with
their raw bytes and a deterministic fault ID. The batch transaction commits its
inspectable evidence, then the writer emits an ERROR and halts before any Kafka
offset is committed. An unresolved fault blocks restart before consumer creation.
Operators use `--list-transport-faults` and resolve a fault with
`--resolve-transport-fault FAULT_ID --resolution-note "..."`; the timestamp and
note are appended to the resolution audit. A recurring resolved fault reopens,
updates its last-seen time and increments its occurrence count. Valid records
stored in the halted batch replay idempotently after recovery.

Batch size **500** and interval **2 seconds** are provisional configurable
defaults, not tuned throughput or latency targets. CLI overrides and
`LAKEHOUSE_BATCH_SIZE` / `LAKEHOUSE_BATCH_SECONDS` allow adjustment. Normal mode
continues through idle periods; `--once` handles one total batch or exits after
10 seconds idle. The deprecated `sync_redpanda_to_lakehouse(limit_per_topic=...)`
wrapper now uses a stable group and one total batch, rather than per-topic
replay limits. A fresh group is required for deliberate offset replay.

Host startup: `python -m lakehouse.sync`. Docker startup:
`docker compose --profile workers up -d --build lakehouse-writer`.
Docker uses `/app/data/lakehouse.duckdb` on the shared `ingestion-data` volume;
host storage is separate by default. PiT query helpers use native DuckDB rows without an undeclared pandas
dependency. Batch logs record inserted/replayed/rejected
counts, conflicts/enrichment, elapsed batch time, explicit next offsets, broker
lag, and storage delay measured from original ingestion time. They do not
represent source publication latency.

Stop the writer before using a separate process to open that DuckDB file for
queries, including a read-only connection. Same-process queries are possible;
an external query service or exported snapshots are a future milestone.

## Migration and legacy data

Versioned migration fails startup on any DDL failure and rolls back schema
changes. Existing databases receive a standalone `.duckdb.before-v4-*.bak`
backup created through DuckDB before migration. A post-migration checkpoint
persists DDL before Kafka begins, including on first startup. Versions newer
than supported are rejected. `schema.sql` supplies the base tables; the
versioned migration adds raw-byte transport fields, wire snapshots and audits.
The current migration version is 4; version 3 added the transport-fault quarantine
table and version 4 adds its operator resolution note.

Legacy Silver timestamps/content are retained. The pre-existing initialization
behavior still stamps NULL embedding-model labels with the original pipeline's
sole MiniLM model; this is a documented exception to untouched legacy columns,
not regenerated vectors. Missing legacy wire snapshots or raw Kafka bytes
cannot be reconstructed. Legacy Bronze records have `legacy_unprocessed`
outcomes and `legacy_bytes_unavailable` provenance. Comparisons against legacy
Silver use persisted projections, with reduced provenance/precision evidence.

The existing host database was validated **on a temporary copy**: 276 news rows,
106 SEC rows, zero Bronze rows. Original columns were retained apart from 276
legacy model-label stamps; the pre-migration backup retained all original values.
The source database's file hash was unchanged. It will migrate when its writer
is started. See [migration evidence](storage-migration-validation.json).

## Verification and limits

`test_storage.py` covers binary/null malformed payloads, invalid transport
quarantine, explicit aware timestamps,
source/topic mismatch, finite vectors and JSON, replay/redelivery, immutable
Silver, SEC conflict/enrichment boundaries, mixed audit categories, transaction
rollback, migration/backup and delayed-event point-in-time queries.

`test_writer.py` covers commit ordering and errors, partition offsets, revoke/lost
callbacks, batch size/time, idle mode, graceful stop, polling failure, and a real
second process failing before consumer construction.

Opt-in [live broker tests](../tests/test_writer_live.py) use unique disposable
topics/groups and temporary databases. They exercise mixed sources and partitions,
malformed messages, an actual group rebalance and graceful writer stop, then
restart recovery; hard process exits during an open DB transaction and after DB
commit but before Kafka commit; explicit broker offsets; first-version conflicts,
enrichment and stable restarted PiT queries. Both live tests passed on local
Redpanda. The final full suite ran 117 tests: 115 passed and the two opt-in live
tests were skipped in discovery. Compilation and Compose validation passed.
A full image build was attempted and stopped during slow dependency downloads;
the image itself and container runtime are not verified by these checks. See [crash/replay evidence](storage-live-validation.json).

Run:

```powershell
python -m unittest discover -s tests -v
$env:RUN_LIVE_STORAGE_TESTS='1'
python -m unittest tests.test_writer_live -v
python -m scripts.validate_storage_migration
python -m compileall -q workers utils lakehouse models scripts tests
docker compose config --quiet
```

Tier 2's new live Redis regression places its only qualifying candidate after
the first 256 of 272 candidates. It proves verification continues into the next
batch. The Federal Reserve rewrite fixture and mixed broker source events are
synthetic behavioral tests; they do **not** validate a captured real RSS/GDELT
syndication pair.

The [pre-storage Tier 2 baseline](tier2-baseline.json) measured 30 samples per
candidate size on this Windows host, using actual Redis retrieval:

| Candidates | p50 ms | p95 ms |
|---:|---:|---:|
| 0 | 2.50 | 4.28 |
| 15 | 3.98 | 5.33 |
| 256 | 13.32 | 14.74 |
| 272 | 15.11 | 16.61 |
| 1,000 | 44.73 | 236.30 |
| 5,000 | 224.92 | 269.94 |

The baseline separates retrieval, record fetch, decode, polarity and exact
Jaccard timings and reports serialized record bytes. It does not measure full
resident memory/Redis allocator overhead, combined Tier 3 performance, or
production sustainable throughput. At 5,000 candidates exact Jaccard averaged
9.17 ms, while total Tier 2 p95 was 269.94 ms. The 1,000-candidate p95 spike is
retained in the evidence. Measurements establish a baseline; they do not close
the crowded-bucket performance gap.

The Bronze replay benchmark uses a temporary disk-backed DuckDB database seeded
with 100,000 rows. It records 30 replay and 30 new-message samples plus an
`EXPLAIN ANALYZE` plan for the retired SELECT pre-read. On the current host,
replay p50/p95 were 13.76/15.65 ms;
new-message p50/p95 were 31.99/61.61 ms. The plan reported a sequential scan
for the old composite-key pre-read. This is a baseline, not a production target.
The million-row comparison is also recorded below. The atomic `ON CONFLICT ... RETURNING` path is now used
for replay detection, so the old pre-read is no longer on the write path.
The million-row run is in [the 1m baseline](bronze-replay-baseline-1m.json):
replay p50/p95 were 9.12/11.05 ms and new-message p50/p95 were 31.99/57.81 ms.
The diagnostic SELECT plans report a sequential scan; they do not describe the
measured atomic INSERT path. The timings are a baseline rather than production
throughput evidence.

Remaining readiness work: capture genuine feed-pair provenance, observe sustained
real feed rates and resource use,
set throughput/latency targets and tune configurable batches against those rates.
Multi-writer DuckDB, source suppression audits, historical annotations and a
reader service are outside this milestone. Start FinBERT/NLP after the data
readiness checks establish reliable historical input; the storage tests alone
do not declare the whole collection system complete.

Run `python -m scripts.audit_data_readiness --output docs/data-readiness.json`
against the database used by the writer. The automated gates require current
schema, complete Bronze outcomes, all three sources, no unresolved transport
fault, a fresh record, a 24-hour collection span and valid canonical references.
Real syndication provenance and human sentiment labels remain explicit manual
gates. Generate the review sheet with
`python -m scripts.export_nlp_label_sample`.
New worker payloads label title and publication-time provenance. GDELT GKG URL
slugs remain auditable discovery records but carry `nlp_eligible=false`; the
readiness report excludes them from its headline-prototype count.
`python -m scripts.capture_syndication_fixture` creates the still-required real
RSS/GDELT provenance report. A run with no qualifying pair remains negative
evidence and does not close the gate.
