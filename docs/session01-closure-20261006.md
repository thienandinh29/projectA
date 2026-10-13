# Collection session 01 — actual closure and remaining gaps

The planned 7,200-second RSS/GDELT session was closed safely, with all deliveries
drained and the writer stopped before audit/export. Both collectors still ran at
05:54 UTC despite their start around 03:41 UTC. They received an explicit graceful
SIGTERM request at 05:55:11 UTC and exited with code 0; their ledger records a
SIGTERM closure, not automatic duration completion.

RSS's recorded start-to-close span was roughly 2 hours 14 minutes. A maximum
4,694-second gap between RSS attempt starts (roughly 78 minutes) is preserved in
the source review. GDELT's last DOC attempt began at 04:31:21 UTC, followed by no
further DOC attempts before shutdown. The source review's maximum between-attempt
interval does not include that final no-attempt period. These are coverage gaps;
neither their cause nor the number of missed headlines has been established.

Do not call this continuous two-hour collection. The timer uses a monotonic clock;
how host/VM suspension interacts with the planned UTC duration needs a separate
check before another uninterrupted-run claim. No unverified timer fix was applied.

## Quiet snapshot evidence

Artifacts in the shared Docker volume:

- `/app/data/research/session01-20261006-drain.json`
- `/app/data/research/session01-20261006-audit.json`
- `/app/data/research/session01-20261006-snapshot/`
- `/app/data/research/session01-20261006-source-review/`
- `/app/data/research/session01-20261006-summary.json`

The named stopped collectors are retained for their logs. The drain was saved at
05:55:42.748871 UTC with zero pending deliveries and zero writer lag across all
four monitored topics. The writer was then stopped. Audit found 2,413 Bronze rows
and 2,413 outcomes, storage schema 5, zero unresolved transport faults and zero
unsealed research versions. The total 529 research versions include audit-only
and invalidated versions and are not the paper sample.

Export cutoff: **2026-10-06T05:56:46Z**. The prospective-since-baseline snapshot
contains **41 provenance-eligible headline versions / 38 article identities**, all
with verified request links and zero unfinished sessions/requests. It excludes
99 pre-baseline versions and 89 first versions from the invalidated parser run.
Its dataset hash is
`2913b3515cbee1b762dd4f091a30a1ebf44c7194e14281d203b85a297c13f4da`.

The snapshot includes 20 earlier corrected-smoke versions plus 21 first versions
from this longer RSS run. It is not 41 headlines collected exclusively in session
01, and article identities do not establish independent economic events. Human
eligibility at closure comprised 2 included, 2 excluded pending clarification of
the review criteria, and 37 unreviewed versions. The user later authorized both
corrections; the selection at 06:05:37.976203 UTC has 4 included and 37 pending.
The operational JSON retains its original closure-time counts. No sentiment
labels were assigned.

## Source outcomes within the longer session

| Endpoint | Attempts | Outcomes | Reported items across attempts |
| --- | ---: | --- | ---: |
| CNBC RSS | 18 | 18 successful HTTP 200 | 540 |
| MarketWatch RSS | 18 | 18 successful HTTP 200 | 180 |
| Yahoo RSS | 18 | 16 successful HTTP 200; 2 failed HTTP 404 | 784; two counts unknown |
| Investing RSS | 18 | 7 successful HTTP 200; 11 timeouts | 70; eleven counts unknown |
| GDELT DOC | 11 | 10 failed HTTP 429; 1 failed HTTP 200 with invalid JSON | All counts unknown |
| GKG downloads | 4 | Four successes, audit-only, capped at 50 each | 200 selected audit rows |

These repeated returned-item counts are not unique news counts. A successful GKG
fallback does not supply authentic sentiment headlines. HTTP 200 with invalid
JSON remains a failed request. Yahoo's new 404 behavior needs feed investigation
before the next session; no feeds or model settings were silently changed here.

Machine-readable source and termination summary:
`docs/session01-operational-summary-20261006.json`. Current storage is inspectable,
but source coverage, assisted eligibility review, annotation, validated grouping,
novelty feasibility, final-test separation and sample-size planning remain open.
The collector images declare an earlier dirty source revision and carry actual
application-file hashes; this run is development evidence, not the required clean
experiment freeze. None of the readiness flags make this dataset paper-ready.
