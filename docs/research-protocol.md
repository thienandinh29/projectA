# Paper 1 research data contract and runbook (v1)

## Study boundary

Paper 1 evaluates fixed model artifacts. Predictions are immutable once created.
Labels, eligibility decisions, and story-grouping judgments may arrive later and
change which predictions qualify for a report, but they do not trigger training
or prediction replacement.

The required evaluation conditions give equal weight to each eligible source-item
version, equal total weight to each reviewed story, and weight only the first
durably available eligible headline from each reviewed story. This clarifies the
earlier article/first-observed wording; metadata-only repeats are not silently
removed from the baseline. Proposed shared-cohort and label-blind anchor rules are
in `research-counting-draft-v1.md`. Their final rules will be frozen after the
development pilot. Continual adaptation code is outside Paper 1.

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

New eligibility judgments use the v2 post-content-commit release contract in
`eligibility-release-contract-v2.md`: missing markers exclude strict selection.
Original legacy recording timestamps are retained and cannot supply release proof.
Archived v1 replay requires an explicit development opt-in; present-time legacy
confirmation does not establish historical availability. CP1 verified a backup
copy; CP2 passed Docker checks and applied live confirmations at
`2026-10-13T01:26:25.835474+00:00`. Frozen policy/guide bytes remain unchanged.
Current selection paths and cutoff are in `checkpoint2-release-rollout-20261013.md`.

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
python -m unittest tests.test_collection_sessions tests.test_collection_boundary tests.test_checkpoint1_delivery tests.test_delivery_recovery tests.test_research_pipeline tests.test_worker_handoff tests.test_storage tests.test_writer tests.test_data_readiness tests.test_pit_leakage -q
docker compose --profile workers --profile tools config --quiet
docker compose --profile workers --profile tools build lakehouse-writer worker-rss worker-gdelt research-tools
if ($LASTEXITCODE -ne 0) { throw 'Build failed' }
```

Start the two Paper 1 headline sources and the writer:

```powershell
docker compose --profile workers up -d --build lakehouse-writer worker-rss worker-gdelt
docker compose --profile workers logs --tail 50 lakehouse-writer worker-rss worker-gdelt
```

The writer consumes `financial.research.observations`. RSS and GDELT save session
and request evidence in the shared delivery SQLite ledger. SEC session evidence
is outside the current Paper 1 headline-source milestone. Scheduled live-session
validation is completed in Checkpoint 3.

For a bounded two-hour session, start only the writer, then run the collectors
without their automatic restart policy. The duration limits starting new cycles;
an in-progress bounded HTTP request/cycle may finish after that time.

```powershell
docker compose --profile workers up -d lakehouse-writer
docker compose --profile workers run -d --no-deps --name paper1-rss-session worker-rss python -m workers.rss_worker --duration-seconds 7200
docker compose --profile workers run -d --no-deps --name paper1-gdelt-session worker-gdelt python -m workers.gdelt_worker --duration-seconds 7200
docker compose --profile tools run --rm --no-deps research-tools -m scripts.inspect_collection_sessions
```

Do not run these named session containers alongside the regular collectors.
Collector ownership is exclusive per topic and shared ledger directory. For the
snapshot, wait for these bounded containers to exit or stop them by their actual
names with sufficient grace (for example `docker stop --time 180 ...`). Stopping
the regular Compose services does not stop containers created by `compose run`.
The writer remains running until the successful saved drain. Retain session
containers/logs as evidence rather than removing them before inspection.

Each request records endpoint, feed/query/filter configuration, UTC start/result
times, HTTP status when available, returned/selected item count and error. A valid
empty feed/API response is `empty`; HTTP, timeout, parsing or shape failures are
`failed`. Every DOC retry is separate. GKG success does not erase DOC failures and
does not supply eligible sentiment headlines. RSS feeds with usable entries and
parser warnings retain their warning rather than losing those entries.

From 2026-10-19 a DOC HTTP 429 is one failed attempt (first 500 response characters
kept) and waits for the next cycle; only 5xx and timeouts retry, 10 s apart.
RSS panel v2 from 2026-10-19: Yahoo Finance removed (HTTP 404); Guardian Business,
BBC Business, NYT Business and Seeking Alpha Market Currents added. CNBC, MarketWatch
and Investing.com unchanged (Investing.com's TLS connection drops at 15 s from this
host; failures stay recorded). Sessions before this date used panel v1.

Session termination appends a normal or failed event. A killed process leaves an
unfinished session/request. A subsequent worker holding the same exclusive topic
lock appends interruption detection; this never invents its actual stop time.
Inspection itself is read-only and does not infer process liveness. A closed
collector can still have pending deliveries and source failures; closure is not
a successful drain or proof of continuous coverage. Cross-host collection sharing
this ledger is not supported by this local file-ownership design.

## Consistent snapshot procedure

1. Stop all collectors (including SEC if enabled) so no new collection attempts begin.
2. Run the bounded delivery-only drain with the writer still running. It retries
   persisted bytes with all-replica acknowledgements, without fetching sources or
   re-entering dedup. It checks committed writer offsets against every configured
   topic partition. Only exit code 0 permits stopping the writer for a snapshot.
3. Run the readiness audit with the writer stopped.
4. Export to a new directory using an explicit timezone-aware cutoff, the active
   baseline, and the saved successful drain report. With the writer stopped, the
   exporter reconciles delivery ranges against Bronze and processing outcomes.
5. Compare manifest counts with the audit, retain both outputs, then restart only
   services that were intentionally running before the snapshot.

```powershell
docker compose --profile workers stop worker-rss worker-gdelt worker-sec
if ($LASTEXITCODE -ne 0) { throw 'Collector stop failed' }
docker compose --profile tools run --rm --no-deps research-tools -m scripts.drain_delivery --timeout 120 --baseline-path /app/docs/collection-baseline-20260928T084655Z.json --output /app/data/research/session-001-drain.json
if ($LASTEXITCODE -ne 0) { throw 'Delivery incomplete: leave writer running, repair and retry' }
docker compose --profile workers stop lakehouse-writer
if ($LASTEXITCODE -ne 0) { throw 'Writer stop failed' }
docker compose --profile tools run --rm --no-deps research-tools -m scripts.audit_data_readiness
if ($LASTEXITCODE -ne 0) { throw 'Audit failed' }
docker compose --profile tools run --rm --no-deps research-tools -m scripts.export_research_dataset --cutoff 2026-10-06T12:00:00Z --drain-report /app/data/research/session-001-drain.json --output-dir /app/data/research/snapshot-001
```

The exporter refuses to overwrite an existing snapshot. Its manifest records
the authoritative database identity and schema, research-version counts, cutoff,
dataset hash and exporter provenance separately from collection provenance.
Manifest v3 distinguishes `prospective_since_baseline` from an explicit
`--development` export. Prospective selection requires the first stored version's
offset at or after its partition baseline, observation time at or after the
baseline time, and sealed availability at or before the cutoff. A pre-baseline
version redelivered later remains development data; a new corrected version may
qualify. A source publication date may precede the baseline without exclusion.
All eligible versions before this boundary are retained in the database.

The stopped-writer check requires every delivery offset from each baseline to
its reported high watermark to have raw Bronze bytes and a matching processed
outcome. Research outcomes must also refer to a stored research version. Missing
evidence, unresolved transport faults, unsealed versions, database identity
mismatch, a report for another baseline, or database deliveries beyond the report
prevent export. This assumes the configured topics use delete retention without
transactional offset gaps or compaction. It does not establish successful source
coverage or final sentiment eligibility. Keep collectors stopped throughout.
Choose a fresh report and snapshot path for each run.
Each RSS/GDELT collector creates one run identity before fetching, including empty
and failed sessions. Its actual
application-file hashes and effective allowlisted settings travel with observations
and survive outbox retries. The manifest lists runs referenced by exported first
versions; older versions remain unknown. Run and request metadata are excluded from content
identity: restarting a collector must not manufacture a new article version.
Feed definitions and query constants are covered by application-file hashes.
Each new observation also carries its request/run ID and publisher domain in
`request_context`. The first stored version retains its original link; repeated
captures have their own links in Bronze without multiplying versions. The CLI
exporter reads the shared ledger (`--session-ledger` can override its path),
archives `collection-sessions.json`, hashes it in the manifest, and verifies the
exported first-version links. Missing/inconsistent evidence fails export. Old
versions without request links remain explicitly unknown. This separate ledger
contains operational source history, including empty/failing requests that have
no DuckDB observation. Keep it backed up with the DuckDB file. Its snapshot may
contain events after the research cutoff: it is audit evidence, never model input.

Quality faults discovered before protocol freeze can invalidate a terminated
collection run using `scripts.inspect_collection_sessions --invalidate-run RUN_ID
--reason "Concrete fault and evidence"`. This appends a dated audit event; no
Bronze record, observation, first-seen time, or original snapshot is deleted or
rewritten. Later exports exclude first versions from those runs and separately
report their count and run IDs. This is a disclosed quality decision at export
time, not a backdated label or feature. Archive the reasons and freeze inclusion
rules before final testing. Previously exported faulty artifacts must be marked
invalid separately and must not be used for experiments.

RSS HTTP-byte parsing preserves the response headers and final URL, including
relative-link resolution and `title_detail.base`. Changing fetch methods without
this context can manufacture content versions. A regression compares the old
URL-fetch and new HTTP-byte parse results for exactly this case.
`COLLECTION_CODE_REVISION` and `COLLECTION_WORKTREE_STATE` are labeled declarations,
not verified build identity. Without Git, Git fields are null, not an empty-diff hash.
Archive the matching source and freeze dependencies before experiments; hashes
alone cannot recreate code. Rebuild tools with collectors before each code rollout;
Compose service images are built separately, not guaranteed identical by name.

A failed drain keeps pending messages durable and exits nonzero. Repair broker or
writer availability and rerun it. Missing topics, missing outbox, unavailable offsets,
or unprocessed retention gaps cannot certify completeness. The active baseline
allows a topic with no post-baseline messages to remain complete even when its
older writer commit expired during inactivity. Such partitions are explicitly
marked `commit_expired_at_idle_boundary`; no Kafka position is reset. Once new messages exist, the writer must commit
through the broker high watermark. A changed partition set, regressed commit/high
watermark, or retention passing an unprocessed commit fails. Use the same outbox mount, topic
configuration and writer group as collection. Drain success verifies the broker
boundary and current commits; the stopped-writer export check separately verifies
stored deliveries, including recorded rejections. Neither establishes research quality. Keep the audit
and drain output with each snapshot. Independent outbox paths need separate drains.

`docs/data-readiness-legacy-host-20260915.json` is historical host evidence only.
`docs/checkpoint1-baseline.md` records the dated Docker initialization baseline.
Neither is a live readiness claim; generate a new Docker audit for each snapshot.
The active prospective boundary is recorded in
`docs/collection-baseline-20260928T084655Z.json`. Earlier records are retained as
development data and excluded from final evaluation.

## Checkpoint 1 pass condition

Checkpoint 1 passes when the workers, writer, audit, and exporter name the same
Docker database; the pre-initialization state and schema-v5 initial counts are
recorded; and audit/export tests plus a real empty snapshot complete. An empty
snapshot proves the storage procedure, not live collection. Real-arrival and
recovery evidence belongs to Checkpoints 2 and 3.
