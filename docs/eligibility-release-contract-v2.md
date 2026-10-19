# Eligibility release contract v2

New eligibility judgments use post-content-commit release evidence. This changes
storage timing and selection, not the frozen eligibility policy or human answers.
CP1 implemented the contract and verified a real backup copy. CP2 then passed the
70-check Docker regression run and applied the contract to the live ledger. The
original 25 judgments/columns remain unchanged; 20 latest legacy decisions now
have explicit present-time confirmation evidence. Details and current selection:
`checkpoint2-release-rollout-20261013.md`.

## New writes and recovery

`append_reviews` validates a whole batch, then commits its judgments and release
requirements with SQLite FULL synchronous mode. It next samples the UTC clock
and persists separate immutable release markers. Strict selection requires data
and release time strictly before the cutoff. Missing releases are visible in
history but cannot enter selection. A pending revision leaves its released
predecessor available until that revision is released.

An error after the content transaction tells the operator to recover, not resubmit:

```powershell
docker compose --profile tools run --rm --no-deps research-tools -m scripts.review_eligibility recover --ledger /app/data/research/eligibility.sqlite3
```

Use current source: rebuild the tools image before operational use, or explicitly
mount current research/scripts/docs code as the verification did. The old image
does not acquire the new behavior simply because files on the host changed.

Recovery samples the present clock and preserves earlier content and release
markers. Clock regression fails explicitly. Separate markers confirm contents
were already persisted; they are not an atomic publication timestamp shared by
all clients. Clock/filesystem guarantees remain disclosed assumptions.

## Original columns and new evidence

Original `eligibility_decisions` and `eligibility_policies` records are retained.
The SQL column named `decision_available_at` keeps its original recording-time
meaning, including for new writes. Do not query that raw column as v2 availability.

Two append-only tables provide the new evidence:

- `eligibility_release_requirements`: decision ID, exact data availability and
  origin (`new` or explicitly registered `legacy`). New requirements commit with
  their judgment contents, making interrupted new writes distinguishable from v1.
- `eligibility_releases`: decision ID, `released_at` and release kind (`initial`,
  `recovery`, `legacy_confirmation` or `legacy_recovery`).

`read_decisions` returns `recorded_at` from the original column and derives
`decision_available_at` from a release marker. It exposes the availability
contract, origin and release kind. Unconfirmed v1 records have no derived
availability. Read-only inspection does not migrate an old ledger.

## Legacy confirmation and historical replay

Strict selection does not use pre-commit timestamps as release proof. Existing
unconfirmed v1 judgments therefore stay pending under the strict contract, even
though they remain valid disclosed development history.

`confirm-legacy` can register the latest old decision for each exact version and
policy, confirm its persisted contents at the actual current time, and append
release evidence. It does not add human judgments, correct labels, confirm
annotation quality or prove historical availability. Superseded legacy records
and all original columns stay untouched. Interrupted confirmation uses recovery.

Historical CP1 copy example; CP2's live confirmation has already been performed:

```powershell
docker compose --profile tools run --rm --no-deps research-tools -m scripts.review_eligibility confirm-legacy --snapshot-dir /app/data/research/session01-20261006-snapshot --policy-file /app/docs/headline-eligibility-dev-v1.md --ledger /app/data/research/checkpoint1-copy-verification-20261013T012003096137Z/eligibility-copy.sqlite3
```

The explicit API timing contract `legacy_precommit_v1` reproduces historical
development selection using original recording times. New-origin v2 judgments
cannot enter that replay. New CLI selections use report kind
`development_eligibility_selection_v2` with `post_content_commit_v2` and archive
only released evidence known before the cutoff. Do not edit old v1 export files.

## Sentiment integration

`load_selection` accepts v2 by default. Loading a v1 archive requires explicit
`allow_legacy=True`. The sentiment CLI exposes `--allow-legacy-eligibility` for
prepare/record/select against an archived v1 development selection. New templates
and reports name the eligibility timing contract; missing contract fields in old
templates mean legacy v1, not strict release evidence.

For example, reproduce an earlier assisted development selection with the flag:

```powershell
docker compose --profile tools run --rm --no-deps research-tools -m scripts.review_sentiment select --snapshot-dir /app/data/research/session01-20261006-snapshot --eligibility-selection /app/data/research/eligibility-batch08-reviewed-20261006 --allow-legacy-eligibility --guide-file /app/docs/sentiment-annotation-dev-v1.md --annotator project-owner --cutoff 2026-10-12T10:08:22.199055+00:00 --output /app/data/research/legacy-replay-NEXT
```

Use a fresh output directory. This is disclosed development replay, not strict
historical availability proof. The existing sentiment ledger and released labels
remain unchanged. A strict v2 eligibility selection can use those same persisted
labels at a later valid cutoff without creating or backdating labels.

## CP1 evidence and limits

50 local tests passed, including 12 new release/legacy regression tests. The real
backup-copy check preserved all 25 original decisions, added 20 present-time
confirmations to the latest decisions, reproduced the original 18/1/1/21 eligibility
counts after release, and preserved historical 14 labeled / 4 unresolved sentiment
results. At release-time equality strict selection excluded all 20 confirmations.
Frozen snapshot/selection hashes and every checked live source file were unchanged.

Details: `checkpoint1-copy-verification-20261013.json`. Full artifacts are in
`/app/data/research/checkpoint1-copy-verification-20261013T012003096137Z/`.
This does not close independent review, source coverage, duplicate validation,
research sample size, model provenance or final experiment readiness.

CP2 verification additionally passed 70 tests in the rebuilt Docker image and
20 local collection/session boundary tests. The live ledger passed its integrity
check; repeated confirmation added nothing and recovery found no pending releases.
Historical v1 label bytes reproduce through explicit development replay. All
checked source files except the intentionally extended eligibility ledger retained
their exact hashes. Original eligibility columns retained their exact contents.
