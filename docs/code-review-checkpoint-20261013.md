# Development code review — October 13, 2026

Scope: pending eligibility reviews, source-outcome summaries, sentiment annotation
ledger/CLI, their tests, saved development review summaries, roadmap and checklist.
These changes do not implement FinBERT inference or certify a final research sample.

## Review and corrections

- Snapshot/selection loaders check hashes, exact version identity and archived
  eligibility reproduction. Cutoff selection excludes unavailable data and later
  judgments. Changed content does not automatically inherit a review or label.
- Sentiment judgments and release markers are append-only. Contents commit before
  the release clock is sampled; unsealed judgments cannot enter selection. Recovery
  uses present time and preserves earlier content, labels and cutoffs. Annotators
  have separate sentiment histories; unresolved is not neutral.
- Source summaries separate failed/unfinished requests, empty successes, repeated
  item returns and audit-only fallback. Attempt intervals are not a continuous
  coverage certificate; source-reported time is not independently verified truth.
- Added Git attributes to preserve exact policy/guide and JSON evidence bytes on
  Windows and Linux. This prevents checkout line endings from changing hashes.
  Frozen policy/guide contents were not edited.
- Clarified eligibility's pre-commit timestamp limitation in its API documentation,
  new selection reports and the roadmap. This is disclosure, not a timing fix or
  a migration of historical review records.
- The existing export test now isolates its local database-identity default from
  Docker's explicitly configured production volume identity.

## Verification

Passed locally:

```text
python -m unittest tests.test_annotations tests.test_eligibility tests.test_source_quality_review tests.test_research_pipeline
38 tests: OK

python -m unittest tests.test_collection_sessions tests.test_collection_boundary
20 tests: OK
```

The same 38 focused tests passed in the existing research-tools Docker runtime
with current research/scripts/tests/docs directories mounted read-only. This
verified the current source without resolving unpinned dependencies again. Tests
used temporary databases. No collectors or production writer were started.
This is 58 local tests and 38 Docker executions, not a full integration/load test.

Frozen guide byte hashes:

- Eligibility v1: `438f7b10fd5bf529d9135362c3108944c7f5f1d588cfa46d13c346b51f3ec0ed`.
- Sentiment v1: `4f5b13fa5f5660ba1e6e5015bee11712fe58561b8b92466d474412bec1916653`.

## Remaining gates

Eligibility still samples its timestamp before content commit. Existing saved
development selections used cutoffs after successful returns; arbitrary cutoffs
near a write cannot claim post-commit availability. Harden this before final freeze,
preserving old records. Eligibility currently has one shared version/policy revision
chain, rather than independent multi-reviewer adjudication.

Collection gaps/GDELT failures, 21 pending eligibility versions, one uncertain
eligibility case, independent review, real financial same-event rewrites and missed
pairs, a representative larger pilot, model provenance/contamination, immutable
predictions, scoring, sampling uncertainty and the final protocol remain open.
The frozen 41-version snapshot and the 18 reviewed sentiment examples are assisted
development material. No new data labels, group assignments, predictions or scores
were created by this review. Databases, raw collection files and outboxes remain
outside this code/documentation commit.
