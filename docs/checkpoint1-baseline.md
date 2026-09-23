# Checkpoint 1 database baseline

Recorded on September 23, 2026 after aligning Paper 1 to a frozen-model study.

## Pre-initialization finding

Docker had no volume named `ingestion-data` and no authoritative research
database. The only existing project volumes belonged to Redpanda and Redis.
Therefore there was no collection database to migrate, back up, or merge. The
host `data/lakehouse.duckdb` was left unchanged as legacy development material.

## Authoritative database

| Property | Recorded value |
| --- | --- |
| Database identity | `docker-volume:ingestion-data:/app/data/lakehouse.duckdb` |
| Docker volume | `ingestion-data` (local, external to Compose lifecycle) |
| Schema version | 5 |
| Bronze rows | 0 |
| Legacy news rows | 0 |
| Legacy SEC rows | 0 |
| Research versions | 0 |
| Message outcomes | 0 |

Because this was a new empty database, initialization required no migration and
created no pre-migration backup. Future populated migrations must retain and
open the timestamped `.bak` file before collection resumes.

## Validation snapshot

The `research-tools` image was built as
`sha256:cfd011cd0913a9558b9473567e9c3b893c8db11d3fadd48f63a65d57d5bc7019`.
The validated snapshot is stored inside the authoritative volume at
`/app/data/research/checkpoint1-empty-20260923-v2` with cutoff
`2026-09-23T00:00:00Z`.

- Exported rows: 0
- Dataset SHA-256: `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
- Source commit: `487e73b3d26cad6a772c62c9f0217d067567ef28`
- Source state: dirty
- Python: 3.11.16
- DuckDB: 1.5.5

The dirty source state is explicit because Checkpoint 1 was implemented on top
of an existing working tree with uncommitted and untracked research work. This
empty validation snapshot is operational evidence only; it is not a paper
dataset and makes no live-collection claim.

## Result

The workers, single writer, readiness audit, and exporter now resolve the same
database identity through Docker configuration. The audit correctly reports
that collection is not working yet: no sources are present, freshness and span
checks fail, and live-session evidence remains for Checkpoints 2 and 3.
