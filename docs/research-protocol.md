# Paper 1 research data contract and runbook (v1)

## Study boundary

Paper 1 evaluates fixed model artifacts. Predictions are immutable once created.
Labels, eligibility decisions, and story-grouping judgments may arrive later and
change which predictions qualify for a report, but they do not trigger training
or prediction replacement.

The required evaluation conditions give equal weight to each article, equal
total weight to each reviewed story, and weight only the first observed eligible
headline from each reviewed story. Their exact cohort rules will be frozen after
the development pilot. Continual adaptation code is outside Paper 1.

## Data paths and authoritative storage

Research path:
`RSS/GDELT parsed item -> durable observation outbox -> research topic ->
Bronze + immutable research version -> post-commit availability marker ->
dated snapshot -> eligibility, annotation, grouping, frozen prediction -> report`.

The authoritative database is identified as
`docker-volume:ingestion-data:/app/data/lakehouse.duckdb`. The writer and
`research-tools` service mount that same named volume. The host file
`data/lakehouse.duckdb` remains legacy development material and must not be used
as the Paper 1 collection database.

The legacy path uses Redis deduplication and existing Silver tables. Its flags,
canonical IDs, embeddings, and inferred historical availability are excluded
from strict research exports. SEC records and synthetic GKG titles remain audit
context and are excluded from the headline sentiment corpus.

## Temporal and version meaning

`observed_at` is sampled while a parsed item is captured, before operational
deduplication. `source_time` preserves a parseable source timestamp together
with its kind and original text. An unknown source time remains unknown.

The writer commits Bronze evidence, research versions, and processing outcomes,
then samples the clock and seals new versions in a separate write.
`available_at = max(post-data-commit clock, observed_at)`. An unsealed version
cannot enter a strict snapshot. Replay after a sealing failure records recovery
availability and never invents an earlier time.

Content and provenance hashes exclude observation time. Repeated delivery of an
identical version retains its first captured item, while a correction at the
same URL appends a new version. Bronze stores the exact Kafka payload received,
not the publisher's original HTTP response bytes.

## First-time database procedure

Set the code identity before building or running collection containers:

```powershell
$env:COLLECTION_CODE_REVISION = git rev-parse HEAD
$env:COLLECTION_WORKTREE_STATE = if (git status --porcelain) { 'dirty' } else { 'clean' }
```

Resolve the configuration and inspect storage without creating or migrating the
database:

```powershell
docker compose --profile tools config --quiet
docker volume inspect ingestion-data
docker volume create ingestion-data
docker compose --profile tools run --rm --no-deps research-tools -m scripts.inspect_research_database
```

The volume is external so Compose cannot remove the research database during a
stack teardown. If the volume does not exist, record that result and create it
once with the command above. If it already exists, `docker volume create` leaves
it unchanged. Record a missing database file before initialization.
Initialize a new database by briefly starting the single writer, then stopping
it before inspection. If a database already exists, save the inspection output
first; starting the writer may migrate it. A migration of a populated database
creates a timestamped `.bak` database in the same volume. Inspect that backup
with the same command and an explicit `--db-path` before continuing.

Never run two writers against this DuckDB file.

## Collection commands

Run regression and configuration checks before collection:

```powershell
python -m unittest tests.test_research_pipeline tests.test_worker_handoff tests.test_storage tests.test_writer tests.test_data_readiness tests.test_pit_leakage -q
docker compose --profile workers --profile tools config --quiet
```

Start the two Paper 1 headline sources and the writer:

```powershell
docker compose --profile workers up -d --build lakehouse-writer worker-rss worker-gdelt
docker compose --profile workers logs --tail 50 lakehouse-writer worker-rss worker-gdelt
```

The writer consumes `financial.research.observations`. Source-cycle, request,
coverage, and interruption evidence is completed in Checkpoint 2. Scheduled
live-session validation is completed in Checkpoint 3.

## Consistent snapshot procedure

1. Stop `worker-rss` and `worker-gdelt` so no new collection attempts begin.
2. Wait for their durable outboxes and the broker backlog to drain; record any
   remaining backlog. Stop the writer only after draining finishes.
3. Run the readiness audit with the writer stopped.
4. Export to a new directory using an explicit timezone-aware cutoff.
5. Compare manifest counts with the audit, retain both outputs, then restart only
   services that were intentionally running before the snapshot.

```powershell
docker compose --profile workers stop worker-rss worker-gdelt
docker compose --profile workers stop lakehouse-writer
docker compose --profile tools run --rm --no-deps research-tools -m scripts.audit_data_readiness
docker compose --profile tools run --rm --no-deps research-tools -m scripts.export_research_dataset --cutoff 2026-10-01T00:00:00Z --output-dir /app/data/research/snapshot-001
```

The exporter refuses to overwrite an existing snapshot. Its manifest records
the authoritative database identity and schema, research-version counts, cutoff,
dataset hash, environment versions, collection revision, and collection tree
state. A dirty state must be accompanied by an archived source tree or patch;
a commit identifier alone does not reproduce uncommitted and untracked code.

## Checkpoint 1 pass condition

Checkpoint 1 passes when the workers, writer, audit, and exporter name the same
Docker database; the pre-initialization state and schema-v5 initial counts are
recorded; and audit/export tests plus a real empty snapshot complete. An empty
snapshot proves the storage procedure, not live collection. Real-arrival and
recovery evidence belongs to Checkpoints 2 and 3.
