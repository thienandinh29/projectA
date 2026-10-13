# Source-quality review — October 6, 2026

This is the first active task in `TODAY.md`. The reviewer used the immutable
corrected 20-version snapshot and an archived read-only view of the live SQLite
request ledger. It did not open the writer-owned DuckDB database. The current
two-hour collection run was still unfinished at the review boundary.

## What the evidence says

Review boundary: approximately **04:09 UTC / 11:09 Bangkok time**. Counts below
refer only to the active two-hour run, not the earlier smoke runs.

| Endpoint | Recorded attempts | Result |
| --- | ---: | --- |
| Yahoo Finance | 8 | 8 successful requests; 49 items each |
| CNBC | 8 | 8 successful requests; 30 items each |
| MarketWatch | 8 | 8 successful requests; 10 items each |
| Investing.com | 8 | 2 successful requests (10 items each), 6 read timeouts |
| GDELT DOC | 6 | 6 HTTP 429 failures; no valid headline response |
| GKG fallback | 2 archive downloads | 50 selected audit rows each, capped; excluded from sentiment headlines |

These are request/item-return counts, **not unique new headlines**. The same feed
can return the same articles on every poll. A failure has an unknown item count,
not a successfully observed empty feed. Investing.com is intermittent rather than
uniformly unavailable. GKG successes do not cancel the DOC failures.

The maximum RSS interval between request starts within the active run was roughly
323 seconds. The configured 180-second sleep happens after a fetch cycle; actual
attempt intervals also include fetching and processing. A polling interval does
not guarantee a fixed sampling cadence. This measurement does not by itself
establish an outage or continuous coverage.

## Publication versus collection freshness

For the **selected frozen 20-version snapshot** (not all feed items):

| Feed | Selected versions with source-reported publication time | Publication-to-observation gap |
| --- | ---: | --- |
| CNBC | 10 | 22.62–166.60 hours; median 132.65 hours |
| MarketWatch | 10 | 4.12–7.39 hours; median 6.19 hours |

These gaps combine feed history/backlog and when our collector first observed the
item. They are not measured HTTP network latency, and they cannot explain the
publisher/feed's update behavior on their own. Receiving an old headline today
does not make its publication date today's date. No source timestamp was invented
or shifted to make an item look fresh.

The valid CNBC learning example was published **October 5 at 05:00:01 UTC**,
observed **October 6 at 03:37:13.876974 UTC**, and durably available at
**03:37:16.979632 UTC**. Its publication, capture and availability are separate
events on one UTC clock. A cutoff between observation and durable availability
cannot include it under our current temporal contract.

## Saved, repeatable evidence

Docker volume directory:
`/app/data/research/source-quality-review-20261006-v2` contains the report and the
exact request/session ledger used to generate it. The report records dataset,
manifest and ledger hashes and actual reviewer code identity. Earlier exploratory
output is preserved separately; the v2 reviewer computes intervals per run rather
than combining gaps between different smoke and active sessions.

```powershell
docker compose --profile tools run --rm --no-deps research-tools -m scripts.review_source_quality --snapshot-dir /app/data/research/checkpoint2-corrected-20261006-snapshot --output-dir /app/data/research/source-quality-review-NEXT
```

Choose a fresh output path. A new run reads a new ledger boundary; use the archived
`collection-sessions.json` via `--ledger` to repeat the original statistical
summary. Code identity and review time remain specific to that regeneration.
Three new quality-review tests pass locally and in Docker; the session regression
tests also pass. Tests distinguish session-specific polling intervals, unknown
counts, unfinished requests, GDELT discovery versus RSS publication, and negative
clock gaps without hiding them.

## What to do next

Keep the source panel unchanged during this documented run. Investigate GDELT DOC
availability and source freshness before deciding the next collection panel;
record any change as a new run/configuration. Version English/financial/headline
eligibility separately from transport success. Today's data remains development
material and is not the final held-out test. Learning check: explain why an HTTP
200 response containing a repeated old article is a new observation rather than
evidence of a new story.
