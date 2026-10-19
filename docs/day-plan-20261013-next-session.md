# Six-hour data work plan — next session

Prepared October 13, 2026, Asia/Bangkok. Starting GitHub commit:
`6e55c56008198b0e365c03d738ac31448956e276`. The system date is still October 13;
this is the next work session on the same calendar date. Track progress in
`../TODAY.md`. Time budgets are estimates; saved evidence determines completion.
Creating this plan does not execute the planned work.

Goal: close the eligibility timestamp gap before adding reviews, then clarify
research counting and review a manageable amount of existing development data.

| Elapsed time | Work | Checkpoint and saved result |
| --- | --- | --- |
| 00:00–00:30 | Inspect Docker/process state, authoritative storage, ledger/snapshot hashes and the timing bug. | CP0: dated baseline; identify changes since the saved checkpoint. |
| 00:30–02:00 | Implement post-content-commit eligibility releases and explicit recovery; preserve legacy history. | CP1: code diff and documented compatibility/availability rules. |
| 02:00–02:45 | Test boundaries, interrupted writes, recovery, revisions and downstream selection locally and in Docker. | CP2: passing checks and old-history/export comparisons. |
| 02:45–03:00 | Break. | — |
| 03:00–03:45 | Define counting rules using the real three-version case and reviewed different-event examples. | CP3: saved contract, worked examples and open decisions. |
| 03:45–04:30 | Review 6–9 pending eligibility versions chronologically, in small batches. | CP4: actual human decisions and a new hashed selection with counts. |
| 04:30–05:00 | Screen up to three genuine financial candidate event pairs. | CP5: supporting evidence, judgments or an explicit shortage of positives. |
| 05:00–05:15 | Break. | — |
| 05:15–06:00 | Review the diff, save checks/evidence, inspect exports and explain one record journey. | CP6: closure note, updated checklist and next priorities. |

## CP0 — Preserve a reliable baseline

Inspect the authoritative Docker volume, not the host's legacy DuckDB. Verify
actual collector/writer state before opening DuckDB independently. If processes
are running, record that fact and follow the safe stop/drain runbook before a
quiet inspection. Preserve offsets and old evidence.

Record revision/tree state, schema, counts, snapshot/manifest hashes and both
eligibility/annotation ledger states. Back up affected SQLite evidence consistently
before migration. Saved counts are comparisons, not current runtime health proof.
No new collection is required in this session.

Pass: dated evidence identifies actual files and process state; differences and
unverifiable expired broker history remain disclosed.

## CP1 — Make eligibility availability explicit

Primary files: `research/eligibility.py`, `scripts/review_eligibility.py`, annotation
selection integration and their tests. Currently `decision_available_at` is sampled
before the eligibility content transaction commits.

For new judgments, persist a content transaction first, then sample and persist an
immutable release marker. Use FULL synchronous writes and document clock/filesystem
assumptions. A separately committed marker confirms persisted contents; it does
not establish an atomic publication instant shared by all clients.

Unreleased judgments stay inspectable and cannot enter strict selection. Failure
after content commit must instruct recovery, not resubmission. Recovery uses its
present time. A pending revision retains its released predecessor until release.

Define compatibility before changing live evidence. Preserve original legacy
decisions, IDs, policy hashes and timestamps. Their pre-commit recording time must
not become claimed post-commit proof. If legacy content needs a new confirmation,
sample the actual current time and record explicit provenance; do not invent an
earlier release. Saved v1 development exports remain immutable. Reproducing them
uses their disclosed legacy semantics, distinguished from new strict selection.

Pass: new writes enforce the release contract and legacy contents/exports remain
preserved and reproducible. Frozen guide/policy bytes are not edited in place.

## CP2 — Verify before recording new reviews

Required meaningful checks:

- Release-cutoff equality is excluded; a later cutoff can include the judgment.
- A separate connection reads committed content before release sampling.
- Failure before content commit rolls back the entire batch.
- Failure after content commit leaves inspectable, unreleased decisions.
- Recovery uses present time without overwriting or backdating earlier evidence.
- An unreleased revision retains its released predecessor at applicable cutoffs.
- Clock regression, stale revisions and incorrect version/hash fail explicitly.
- A legacy ledger copy preserves original records; saved exports retain their
  bytes and disclosed historical interpretation.
- Annotation selection consumes the intended eligibility contract without
  silently treating legacy recording times as verified release times.

Run focused tests locally and in Docker, with collection/session boundary checks
where affected. Use temporary databases or copies for crashes/migrations. Explicitly
rebuild or bind current source in Docker; do not test a stale image.

Pass: saved results and history comparisons demonstrate these behaviors. Repair
failures before CP4; disclosure alone cannot close the timing bug.

## CP3 — Define what the research counts

Distinguish source-item version, URL/article identity, distinct headline text and
reviewed underlying event. Publisher timestamp changes can create another stored
version without a new FinBERT text input. Shared topic/sentiment is not event identity.

Preserve raw versions. Specify how metadata-only repeats, changed titles, new facts
and cross-source rewrites enter baseline and controlled cohorts. Resolve the
roadmap's article-version versus article wording explicitly before implementation;
do not silently change the denominator. Keep the planned article/version-weighted,
equal-total-story-weight and first-observed-eligible-story-headline reports distinct.

Define first-seen ordering and information permitted at each cutoff. Record actual
group-review timing and disclose later grouping used only for retrospective scoring.
Later labels/groups cannot justify historically executable predictions. No automatic
label/prediction transfer across versions is introduced by this discussion.

Pass: draft contract, worked examples and explicit open choices. The three-version
Anthropic case is pending eligibility and is a teaching example, not a validated
financial duplicate positive. Model inference/scoring is not needed for this task.

## CP4 — Expand eligibility carefully

Prepare exact titles/versions/hashes from the frozen snapshot. Select the next
6–9 pending versions by availability, observation and version ID. Review English,
financial relevance and genuine-headline status independently. Record actual human
reasons and assistance. Revisit the uncertain technology case only if evidence
supports a judgment; uncertainty is an acceptable outcome.

Use the hardened timing contract for new reviews and save selection hashes/counts.
Excluded/uncertain records stay archived. This block creates no automatic sentiment
labels and need not finish all 21 pending versions or the independent pilot.

Pass: actual human batch outcomes are saved and earlier exports/history remain
unchanged. If human judgments are unavailable, mark preparation complete but
review pending; never substitute assistant judgments as independent human evidence.

## CP5 — Screen duplicates honestly

Inspect up to three financial candidate pairs with specific event identity, exact
versions and source provenance. Separate same event from identical information;
updates retain their own content, labels and availability. Shared month, publisher,
weekday or sentiment alone is insufficient evidence.

The currently eligible cohort has no extra merges under reference rules. If no
defensible financial rewrite positive exists, record that and specify the next
source/collection/retrieval need. Do not manufacture positives or label teaching
examples as real observations. New retrieval would be available now, not backdated
to a publisher timestamp.

Pass for the session: honest screening evidence or documented sample shortage.
Independent review, missed-pair sampling, precision/recall and validated syndication
remain separate research gates; assisted screening does not close them.

## CP6 — Save closure

Record changes/reasons, performed checks, actual reviews, before/after counts/hashes
and limits. Update `TODAY.md` with evidence paths. Use new artifact paths rather than
overwriting frozen snapshots/guides. This request saves a plan locally; a later
GitHub save regenerates `changes.txt` from the final reviewed implementation diff.

Learning target: explain why recording a review before a cutoff does not make it
available if its content commit/release happens after the cutoff. Then distinguish
three source versions from one headline text and an independently reviewed event count.

If timing work takes longer, complete its verification and defer later tasks openly.
FinBERT training/inference, continual learning, performance claims and a final-test
freeze are outside today's completion criteria. Larger representative sampling,
source coverage, independent annotation, model provenance, statistical planning,
scoring and literature feasibility remain future gates.
