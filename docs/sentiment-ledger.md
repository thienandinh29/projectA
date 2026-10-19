# Development sentiment ledger

`research/annotations.py` and `scripts/review_sentiment.py` record human judgments
separately from collection, operational deduplication and eligibility. Use the
authoritative Docker snapshot/selection; the host's legacy DuckDB is not input.
The SQLite annotation file resides in the same external research volume and must
be backed up together with snapshots and the eligibility/session ledgers.

## Workflow

Rebuild `research-tools` after changing code. The preparation command verifies
the snapshot content/provenance, eligibility-selection hashes and the ability to
reproduce inclusion from saved decisions. It writes a new blank JSON template
containing exact displayed titles and snapshot/selection/guide hashes.

```powershell
docker compose --profile tools build research-tools
docker compose --profile tools run --rm --no-deps research-tools -m scripts.review_sentiment prepare --snapshot-dir /app/data/research/session01-20261006-snapshot --eligibility-selection /app/data/research/eligibility-batch08-reviewed-20261006 --guide-file /app/docs/sentiment-annotation-dev-v1.md --limit 3 --output /app/data/research/sentiment-practice-template.json
```

The default ordering is availability, observation and version ID. `--version-id`
may select an explicit teaching subset, but requires a disclosed `--selection-reason`.
Teaching subsets are not representative samples for accuracy estimates. Preparation
generates zero judgments. Complete every field in `annotations` using actual human
answers and retain the `inputs`/hashes. No caller-provided timestamps are accepted.

```powershell
docker compose --profile tools run --rm --no-deps research-tools -m scripts.review_sentiment record --snapshot-dir /app/data/research/session01-20261006-snapshot --eligibility-selection /app/data/research/eligibility-batch08-reviewed-20261006 --guide-file /app/docs/sentiment-annotation-dev-v1.md --reviews /app/data/research/sentiment-practice-completed.json --annotator project-owner
```

Use a new file for a correction with `supersedes_decision_id` set to the current
decision in that annotator/version/guide chain. Another annotator starts their own
chain; their answer cannot silently supersede yours. Changed guide bytes produce
a new guide hash and do not inherit old labels.

## Commit, interruption and recovery

One transaction persists the whole validated batch. Only after it commits does
the tool sample `label_available_at` and append separate immutable release markers.
The timestamp confirms persisted content, not atomic publication to all clients
at that instant; markers are committed separately. Selection requires a marker.
All SQLite writes use FULL synchronous mode, subject to filesystem/host guarantees.

A failure before the first commit rolls back the batch. A failure during sealing
leaves committed judgments visible in history but unusable for selection. The
error tells the operator to recover, not resubmit. Recovering does not replace or
backdate the contents; it records current availability and `release_kind=recovery`.
Clock regression fails instead of inventing a release time. A pending judgment
must be recovered before adding a revision in its chain.

```powershell
docker compose --profile tools run --rm --no-deps research-tools -m scripts.review_sentiment recover
```

The ledger tables prevent normal SQL update/delete operations. This is audit
protection against accidental edits, not security against an administrator who
can change a database file/schema. Keep backups and hashed exports.

## Selection and limits

The `select` action requires an explicit annotator and a timezone-aware past cutoff:

```powershell
docker compose --profile tools run --rm --no-deps research-tools -m scripts.review_sentiment select --snapshot-dir /app/data/research/session01-20261006-snapshot --eligibility-selection /app/data/research/eligibility-batch08-reviewed-20261006 --guide-file /app/docs/sentiment-annotation-dev-v1.md --annotator project-owner --cutoff 2026-10-07T00:00:00Z --output /app/data/research/sentiment-before-review-example
```

That example cutoff cannot include labels recorded later. Choose the actual desired
cutoff and a fresh output path for each selection. Eligibility is also replayed
from archived decisions at that cutoff; a later eligibility review cannot supply
earlier membership. Only archived decisions are available, not future updates to
the live eligibility ledger. Sealed data and judgments must precede the cutoff
strictly; later corrections do not change earlier selections. A latest
unresolved judgment removes that version from three-class label output, while its
full released history and unresolved status remain in the report. Unsealed revisions
do not replace earlier released decisions. Labels are exported in the legacy
`SentimentLabel` format; no model fitting, prediction or scoring occurs here.

Multi-annotator adjudication, agreement statistics, representative pilot sampling,
final-test rules, prediction timestamps and the full evaluation harness remain open.
Eligibility release v2 now persists new judgments before sampling release evidence.
Archived v1 selections require the explicit `--allow-legacy-eligibility` CLI flag
(or `allow_legacy=True` in the loader) for disclosed development replay. Add that
flag to the examples above because their eligibility selection is v1. Current
templates/reports name the eligibility timing contract. Frozen old files and the
annotation ledger are not rewritten. See `eligibility-release-contract-v2.md` for
new releases, recovery, legacy confirmation and rollout status.
