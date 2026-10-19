# Checkpoint 0 — verified baseline, October 13

Complete. Research storage was inspected read-only at 01:07 UTC / 08:07 Bangkok.
Docker process state was reconfirmed between 01:07:54 and 01:08:00 UTC.
Machine-readable evidence: `checkpoint0-baseline-20261013.json`.

## Current process and storage state

RSS/GDELT collectors and the lakehouse writer remain exited with code 0. Broker
and Redis are running and healthy; console is running. No collectors/writer were
restarted. Docker volume `ingestion-data` exists. Authoritative database identity
remains `docker-volume:ingestion-data:/app/data/lakehouse.duckdb`, schema version 5.

| Evidence | Verified value |
| --- | --- |
| Bronze raw rows | 2,413 |
| Processing outcomes | 2,413 |
| Stored research versions | 529 |
| Unsealed research versions | 0 |
| Silver news / SEC records | 137 / 0 |
| Delivery outbox pending rows | 0 |
| Frozen snapshot versions / article IDs / exact titles | 41 / 38 / 38 |
| Eligibility included / excluded / uncertain / pending | 18 / 1 / 1 / 21 |
| Eligibility history decisions, including revisions | 25 |
| Sentiment labeled / unresolved | 14 / 4 |
| Sentiment history decisions / unsealed decisions | 18 / 0 |
| Sentiment negative / positive / neutral | 7 / 5 / 2 |

The snapshot and eligibility selection reproduce successfully. Their exact hashes
match prior evidence. Snapshot, manifest, policy and current sentiment-guide hashes
were checked. DuckDB, the three SQLite ledgers and protected snapshot/selection
files had identical byte hashes before and after this inspection.

## Verified recovery copies

Full baseline and consistent SQLite backups are stored in the external volume:

```text
/app/data/research/checkpoint0-baseline-20261013T010717687653Z/
  baseline.json
  eligibility.backup.sqlite3
  annotations.backup.sqlite3
  delivery-outbox.backup.sqlite3
```

SQLite's backup API was used with read-only source connections. Each backup passed
`integrity_check`, and its logical SQL contents match the source. Backup file byte
hashes can differ from source byte hashes because SQLite backup headers may differ;
both hashes and matching logical-content digests are recorded. These copies protect
the upcoming ledger change, and reside in the same volume rather than off-device
disaster-recovery storage. Original source files were unchanged.

Starting code revision: `6e55c56008198b0e365c03d738ac31448956e276`.
The tree contains the new planning documents. Current research/scripts/docs source
was mounted read-only in the tools runtime; no schema migration was performed.

## Limits and next checkpoint

This is a baseline inspection and backup check, not a full storage readiness audit
or Kafka delivery drain. Current Kafka offsets/expired historical delivery ranges
were not checked. An empty outbox and stored evidence do not reconstruct expired
broker history. No new data, reviews, event grouping, predictions or scores were
created. The 41-version snapshot remains assisted development material.

CP1 can now implement eligibility post-content-commit release evidence against
temporary files/copies first. Preserve the 25 legacy decisions and immutable saved
exports; their pre-commit recording timestamps remain a disclosed limitation until
the new timing/compatibility contract is implemented and verified.
