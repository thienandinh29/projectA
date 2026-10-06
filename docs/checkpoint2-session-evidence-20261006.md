# Checkpoint 2: collector sessions and request evidence

RSS/GDELT session and request evidence is implemented and verified with live
delivery, restart fixtures, and a corrected prospective snapshot. Continuous
collection and real GDELT DOC coverage have not passed their gates.

## Implemented behavior

- Append-only session starts, normal/failed exits, planned stop requests and
  interruption detection in the persistent shared delivery SQLite ledger.
- Exclusive local ownership per source topic. A second collector fails rather
  than declaring a live owner's session interrupted. Inspection is read-only.
- Separate HTTP/request outcomes, feed/query/filter configuration, returned or
  selected counts, response errors and parsing warnings. Valid empty responses
  remain distinct from failed requests. Each DOC retry is recorded separately.
- New observations carry request/run IDs and publisher domain before live
  deduplication. These fields do not change content identity. Bronze retains
  repeated delivery evidence; stored versions preserve their first observation.
- Exports archive/hash the session ledger, verify first-version request links,
  and disclose legacy unknown links, unfinished evidence and faulty-run exclusions.
- Bounded collectors accept `--duration-seconds`; the configured duration is
  recorded. Collection stops starting cycles at that deadline. An in-progress
  request/processing cycle can finish later. Closing a collector does not prove
  a successful drain or successful source coverage.

The ledger remains in `/app/data/delivery-outbox.sqlite3` alongside the authoritative
DuckDB volume. It is independent operational evidence; request failures and empty
responses have no news row to store in DuckDB. Back up both databases. No DuckDB
schema migration was required (schema remains v5).

## Live-discovered parser fault and correction

The first smoke capture switched from `feedparser.parse(url)` to fetching bounded
HTTP bytes, but initially omitted the response URL/headers. An unchanged Yahoo
headline differed from its older stored version only in
`source_item.title_detail.base` (feed URL versus empty string). This manufactured
version identities. The first 89-row export is **invalid for research**.

The corrected parser supplies response headers and the final URL, preserving
relative links and parser context. A regression compares URL-fetch and HTTP-byte
parse results directly. A valid empty XML feed with a content-type warning is
recorded as empty with its warning, while unrecognized/malformed feeds fail.

Faulty RSS run `7f71934f-704d-4025-8f30-f78adf0fb5e6` was invalidated with a dated
append-only reason. No raw delivery, version, original manifest or dataset was
deleted or rewritten. The original snapshot has a separate
`INVALID_FOR_RESEARCH.json` marker. Subsequent exports exclude its 89 first
versions and report the exclusion separately from baseline exclusions.
These are development-stage quality decisions before final protocol freeze;
they must never be concealed or used to tune held-out results.

## Corrected verification

**99 local regression tests passed**; **27 boundary/session/worker tests passed
inside the rebuilt Docker environment**. All four service images were rebuilt
together. Tests cover a second collector, real process death, unfinished requests,
restart interruption detection, planned signals, startup failure, malformed DOC
responses, exhausted 429 retries, valid empty feeds, old observation compatibility,
stable identity across request IDs, missing/wrong request evidence, source/topic
mismatch, faulty-run exclusion and preserved raw versions.

Corrected live source cycle:

| Source request | Outcome | Returned/selected items |
| --- | --- | ---: |
| Yahoo Finance RSS | HTTP 200 success | 49 |
| CNBC RSS | HTTP 200 success | 30 |
| MarketWatch RSS | HTTP 200 success | 10 |
| Investing.com RSS | Read timeout | Unknown |
| GDELT DOC | Three HTTP 429 failures | Unknown |
| GKG manifest/download | Separate audit-only success | 50 selected rows, capped |

Both collectors exited normally. RSS reported 89 live exact duplicates, but
pre-dedup research capture still persisted their source observations. Source
failures remained visible. A successful saved drain preceded stopping the writer.

The corrected snapshot contains **20 provenance-eligible RSS versions**, with
20 distinct article IDs and no pre-baseline version for those article IDs.
All 20 have verified request links. This is not a claim of 20 independent events
or final sentiment/language eligibility. It excludes 99 pre-baseline versions and
89 first versions from the faulty run. There are no unfinished sessions/requests
or unsealed versions in this snapshot.

At that quiet boundary the database held **621 Bronze rows**, **621 outcomes**
and **358 sealed research versions**. The stored ranges reconcile with RSS next
offset 443 and observation next offset 433. GDELT/SEC news topics had no new
messages. Invalid and synthetic records remain archived.

Artifacts in external Docker volume `ingestion-data`:

- `/app/data/research/checkpoint2-corrected-20261006-drain.json`
- `/app/data/research/checkpoint2-corrected-20261006-audit.json`
- `/app/data/research/checkpoint2-corrected-20261006-snapshot/manifest.json`
- `/app/data/research/checkpoint2-corrected-20261006-snapshot/dataset.jsonl`
- `/app/data/research/checkpoint2-corrected-20261006-snapshot/collection-sessions.json`
- `/app/data/research/checkpoint2-corrected-20261006-snapshot/headline-trace.json`

The earlier `checkpoint2-smoke-20261006-snapshot` is retained as invalid debugging
evidence. Its manifest remains the original artifact, not a current validity claim.

## Learning example and remaining gate

`docs/checkpoint2-corrected-headline-trace-20261006.json` traces a CNBC headline:
source publication **2026-10-05 05:00:01 UTC**, corrected observation
**2026-10-06 03:37:13.876974 UTC**, durable availability
**2026-10-06 03:37:16.979632 UTC**. These are three different events on the same
UTC clock. Publication time does not make the headline available earlier.

The next operational check is a bounded two-hour RSS/GDELT session, then drain,
stop the writer, audit and export using new paths. Actual windows, failures and
gaps must be reviewed; a long historical record span is not continuous operation.
GDELT DOC availability and stale/reissued feed items still need source-panel
investigation. GKG never substitutes for real sentiment headlines. Three
provisional sessions, pilot eligibility/annotation, duplicate review, protocol
freeze and final sample-size planning remain. The data is **not paper-ready**.

## Two-hour session started (not yet certified complete)

Started October 6 at approximately **03:41 UTC / 10:41 Bangkok time**:

- `paper1-rss-session01-20261006`
- `paper1-gdelt-session01-20261006`
- the sole Compose `lakehouse-writer`

Collectors use `--duration-seconds 7200` and Docker restart policy `no`.
They stop starting cycles around 05:41 UTC / 12:41 Bangkok time; an in-progress
cycle may finish later. The writer remains running for the final delivery drain.
No final snapshot or continuity claim has been made for this session.

After both actual named containers exit, inspect their status/ledger, then:

```powershell
docker compose --profile tools run --rm --no-deps research-tools -m scripts.drain_delivery --timeout 120 --output /app/data/research/session01-20261006-drain.json
if ($LASTEXITCODE -ne 0) { throw 'Drain failed: leave writer running' }
docker compose --profile workers stop --timeout 30 lakehouse-writer
if ($LASTEXITCODE -ne 0) { throw 'Writer stop failed' }
docker compose --profile tools run --rm --no-deps research-tools -m scripts.audit_data_readiness --output /app/data/research/session01-20261006-audit.json
if ($LASTEXITCODE -ne 0) { throw 'Audit command failed' }
$sessionCutoff = [DateTimeOffset]::UtcNow.ToString('o')
docker compose --profile tools run --rm --no-deps research-tools -m scripts.export_research_dataset --cutoff $sessionCutoff --drain-report /app/data/research/session01-20261006-drain.json --output-dir /app/data/research/session01-20261006-snapshot
```

Use an actual stored cutoff for that snapshot. Do not reuse the earlier smoke
drain after more deliveries arrived. Do not inspect DuckDB concurrently with its
writer. Preserve the stopped named session containers/logs until review.
