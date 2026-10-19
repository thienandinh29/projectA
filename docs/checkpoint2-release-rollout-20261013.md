# CP2 — verification and live release rollout

Complete, October 13, 2026. The live eligibility contract was applied at 08:26
Bangkok / 01:26 UTC after regression checks passed. Machine-readable evidence:
`checkpoint2-release-rollout-20261013.json`.

## Verified behavior

- 70 tests passed in the rebuilt research-tools image: eligibility (8), release
  compatibility (12), annotations (13), source quality (3), research pipeline (14),
  and collection/session boundaries (20). Tests use temporary databases/mocked
  source responses; their GDELT failure logs are simulated, not new feed collection.
- 20 collection/session boundary checks passed locally. Together with CP1's 50
  local checks, these cover the same 70 distinct checks in both environments.
- The rebuilt image's four changed production modules exactly match CP1 code
  hashes. Dependency installation reused its cached layer. Environment/package
  versions and image identity are recorded; this is not a final dependency lock.
- Release-cutoff equality excludes the judgment. Interrupted writes/revisions,
  present-time recovery, stale revisions, clock regression, legacy opt-in and
  immutable evidence behavior pass their regression checks.

Docker image ID:
`sha256:5ed7cb0c5e2fca6e0ce24007aeedf599db9b69642b1eaa7c817342ef5c5dcfa4`.
No collectors or production writer were started; they remain stopped. The rebuilt
image is for research tools; collector/writer images were not rebuilt or restarted.

## Live changes and preservation

A consistent pre-rollout eligibility backup passed integrity and logical-content
comparison. Rollout preserved all 25 original decision rows and the original policy.
It registered release requirements and confirmations for the 20 latest legacy
decisions (18 included, 1 excluded, 1 uncertain), not their superseded predecessors.

Confirmation availability: `2026-10-13T01:26:25.835474+00:00`.
This establishes persisted contents now; it does not prove their original recording
times were post-commit availability, add new human judgments, or establish label
quality. Before and at that release timestamp, strict selection leaves all 41
versions pending. After release, it yields 18 included / 1 excluded / 1 uncertain /
21 pending. All source versions precede that time; it is the review-release evidence
that changes strict membership.

The eligibility file hash changed because new schema/evidence was appended. Original
decision/policy column contents were checked unchanged. DuckDB, annotation ledger,
delivery/session outbox, frozen snapshot and checked legacy exports retained their
exact byte hashes. Repeated confirmation added nothing. Recovery found no pending
release requirements, and the live SQLite integrity check returned `ok`.

Historical sentiment selection reproduced its earlier versions/counts through an
explicit legacy replay, and its label export bytes matched exactly. The new v2
eligibility selection yields the same current 14 labels / 4 unresolved cases, with
7 negative / 5 positive / 2 neutral. No annotation was added or rewritten.

## Current strict development selections

```text
Research-volume directory:
/app/data/research/checkpoint2-release-rollout-20261013T012624497947Z/

eligibility-v2-selection/
sentiment-v2-eligibility-selection/
historical-sentiment-replay/
eligibility.before-release-v2.sqlite3
releases.json
environment.json
rollout.json
verification.json
docker-tests.txt
```

Current selection cutoff: `2026-10-13T01:26:25.843919+00:00`.
Eligibility-selection hash:
`b2fc6d09739faffb0ef1720cdd123ff741e6ba4d107cc6de201798debc64917e`.

Use this v2 eligibility selection for subsequent sentiment development workflow;
no legacy flag is required. Future eligibility reviews require a fresh selection
after their own release. The original v1 archive remains immutable and requires
`--allow-legacy-eligibility` only for its explicit development replay. Old and new
timing semantics cannot be treated as equivalent at historical cutoffs.

## Remaining work

CP3 defines how source versions, headlines, articles and reviewed events count.
CP4 reviews a small new batch from the 21 pending eligibility versions. Financial
rewrite validation, independent annotation, representative sampling, source gaps,
model provenance, immutable predictions and final scoring remain open. No Kafka
offset/drain verification, new collection, model inference or score was performed.
This is a completed timing/compatibility checkpoint, not paper-ready data.
