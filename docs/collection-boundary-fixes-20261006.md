# Collection boundary fixes — October 6, 2026

The idle-period check failed after the inactive Kafka writer group's commits
expired, even though every high watermark still equaled the recorded baseline.
The previous exporter also selected eligible old versions without enforcing that
baseline. Both issues are now covered by code and regression checks.

## Behavior

- A missing commit is allowed only when that partition's broker high watermark
  still equals its baseline. The drain reports the expired idle commit explicitly.
  Existing numerical commit regression, missing commits for new messages, writer
  lag, invalid broker ranges, retention gaps and changed partition sets fail.
- A drain report can be saved with `--output`, with a baseline hash, database
  identity, checked UTC time, and per-partition progress. Existing reports are
  never overwritten. The client does not subscribe, reset offsets, or commit.
- Prospective exports require an active baseline and successful saved drain
  report. After stopping the writer, each post-baseline delivery range must have
  every expected raw Bronze record and matching processed outcome. Research
  outcomes must refer to a stored version. Missing records, stale reports,
  mismatched identities/hashes, unresolved faults and unsealed versions fail.
- Selection requires first-version transport offset and observation time at or
  after the baseline, and sealed availability at or before the cutoff. Unchanged
  pre-baseline versions redelivered later remain excluded. Corrections captured
  after the boundary may qualify. Original publication time is preserved and may
  precede the boundary.
- Manifest v3 reports prospective/development scope, exclusions, boundary hash,
  and storage reconciliation. `--development` explicitly exports eligible pilot
  material without asserting prospective transport completeness.

## Verification

Local regression command: **87 tests passed**.

```text
python -m unittest tests.test_collection_boundary tests.test_checkpoint1_delivery tests.test_delivery_recovery tests.test_research_pipeline tests.test_worker_handoff tests.test_storage tests.test_writer tests.test_data_readiness tests.test_pit_leakage -q
```

The eight new boundary tests also passed inside the rebuilt Docker tools image.
Fixtures exercise nonempty ranges, dropped Bronze rows, missing outcomes/versions,
stale/failed reports, boundary/cutoff equality, redelivery, and delayed observation.
Compose configuration and code whitespace checks passed.

Live idle verification saved in external Docker volume `ingestion-data`:

- `/app/data/research/idle-fix-20261006-drain.json`
- `/app/data/research/idle-fix-20261006-snapshot/manifest.json`
- `/app/data/research/idle-fix-20261006-snapshot/dataset.jsonl`

The drain reported zero pending messages and zero post-baseline deliveries on
RSS, GDELT, SEC and research topics. All four commits were missing; the earlier
RSS/research commits were explicitly marked expired at the unchanged boundary.
The prospective snapshot exported **0 rows**, excluded **99 old eligible versions**,
and retained **149 research versions**, **249 Bronze rows**, and **249 outcomes**.
No source collection was started, and the writer and collectors stayed stopped.

## Limits and next work

Live verification covered an empty prospective range. Nonempty success/failure
was exercised in fixtures, not a new live ingestion or outage experiment. Session
start/end evidence and GDELT source-outcome reporting remain the next milestone.
The snapshot is not paper-ready.

Stored range reconciliation assumes these topics use delete retention and no
transactional/compaction gaps. The baseline and reports are trusted local evidence;
operator resets and topic recreation require a fresh documented investigation.
An expired commit with an advanced high watermark still fails until storage and
transport progress are investigated. Hashes identify evidence; they do not archive
source code or dependencies. Rebuild all service images together before the next
collection session, since this verification rebuilt tools only.
