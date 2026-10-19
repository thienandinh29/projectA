# Today's checkpoint — October 13, 2026

## Active work — source versions and research counting

- [x] Resume from the saved snapshot and archive the October12 checklist in
  `docs/day-checkpoints-20261012-final.md`.
- [x] Start Docker Desktop after its API was unavailable; recheck collectors and
  writer stopped with exit code 0. No collection restart.
- [x] Inspect three versions of one URL: headline/summary identical, parsed
  publisher timestamps changed, headline digest equal, source version IDs different.
- [x] Verify snapshot/manifest/eligibility/annotation hashes unchanged and included18
  counts still 14 labeled / 4 unresolved.
- [x] Complete the assisted learning check: three source-item versions can contain
  one distinct headline text. Headline-only FinBERT receives text, not timestamps;
  it does not automatically merge records. Sending all three can mean three calls.
- [ ] Review article/headline/event counting rules using real examples, preserving
  updates and availability rather than deleting raw evidence.
- [ ] Obtain genuinely reviewed financial same-event rewrite/update pairs and
  independent hard-negative/missed-pair review; current examples do not close that gate.

Today's lesson: `docs/source-version-learning-20261013.md`.
Evidence: `docs/source-version-learning-case-20261013.json`.
Case remains pending eligibility and is not added to the sentiment cohort.
The current source-item version hash includes source metadata, so a changed
publisher timestamp can yield another stored version with identical headline text.
This is an observation about existing identity rules, not a code/schema change.

Before the requested GitHub commit preparation, no fitting, inference, score,
new label, grouping change, commit or push occurred.
Open work still includes eligibility timing hardening, source coverage failures,
representative larger pilot, independent annotation/adjudication, model/checkpoint
provenance, prediction logs and final protocol/testing freeze.

### Completion check after the learning discussion

Today's source-version inspection and assisted explanation are complete. The full
data/research work for today is not complete: the counting-rule review is partial,
and financial same-event rewrite validation remains open. This checklist update
checks saved evidence; it does not repeat the earlier live Docker/database audit.

Next checkpoint: document how source versions, URL/article identities, distinct
headline text and reviewed events enter each evaluation condition. Preserve all
raw versions and availability; keep article-level, story-weighted and first-seen
story results separate. Explicitly handle metadata-only revisions and changed
headline text before implementing any selection or prediction-reuse policy.
The current three-version example has pending eligibility, so it cannot be used
as a financial duplicate positive or added to the labeled evaluation cohort.

The saved snapshot remains 41 versions / 38 article IDs / 38 exact titles.
Eligibility remains 18 included / 1 excluded / 1 uncertain / 21 pending; the 18
included first sentiment reviews are complete (14 class labels / 4 unresolved).
These are development counts, not a completed representative research sample.

### Code review and GitHub save checkpoint

- [x] Review pending eligibility, sentiment and source-quality tooling against the
  development scope and cutoff rules; retain explicit research-readiness limits.
- [x] Pass 38 focused tests locally and in Docker, plus 20 local collection-session
  and collection-boundary regressions (58 local tests total).
- [x] Protect hashed guide/policy and JSON evidence bytes from Git line-ending
  conversion. Do not change frozen guides or historical database records.
- [x] Review and stage the code/documentation for the requested GitHub save.

changes.txt contains the staged patch, excluding itself. The resulting commit and
remote-push verification are reported with the GitHub link in the conversation;
they do not close the remaining research checkpoints.

Review details and remaining limitations: docs/code-review-checkpoint-20261013.md.

## October 12 historical checklist and evidence

## Completed: first sentiment review of currently included18

14 labeled / 4 unresolved / 0 awaiting first review.
The last Medicare-rebate judgment was human-approved unresolved and sealed at
17:08:22 Bangkok / 10:08:22 UTC. Original answers/reasons are preserved. Earlier
cutoffs and prior 17 decisions remain unchanged. All judgments disclose assistance.
This closes the small development exercise, not the full dataset or research pilot.

Closure: docs/annotation-first-review-closure-20261012.json.
Current selection: /app/data/research/sentiment-teaching-batch06-final-current-20261012.
Unresolved cases stay visible and excluded from three-class labels.

## Active next work: duplicate-story review

- [x] Read-only inventory: 41 versions / 38 article IDs / 38 exact title strings.
- [x] Inspect two multi-version articles: unchanged titles, changed source timestamps.
- [x] Compare reference grouping: all41=38 groups; included18=18 groups under each
  article/exact/lexical method. No distinct-article lexical pair reaches 0.75.
- [x] Check six forward-prefix replays and preserved dataset/ledger hashes.
- [ ] Human review of candidate event pairs with reasons and real review timing.
- [ ] Gather verified same-event rewrites and hard negatives; independently search
  missed pairs. Do not claim validated syndication or recall from lexical candidates.
- [ ] Review additional21 eligibility records and the uncertain eligibility case.
- [ ] Plan representative larger collection/pilot and independent annotation.

Next explanation/exercise: docs/duplicate-review-next-20261012.md.
Audit summary: docs/duplicate-inventory-20261012.json.
Full audit: /app/data/research/duplicate-inventory-20261012.json.
The current included cohort has no extra merges under the reference rules; this is
not proof of no semantic duplicates. It cannot yet demonstrate a duplicate-control
performance effect. Reference groups are not ground-truth independent events.

Collectors and writer remain stopped; no collection restart, code changes, training,
prediction, scores, commit or push occurred. October7 tests remain historical evidence;
today's work used live ledger/export and read-only grouping checks.

Before final freeze: eligibility availability hardening, source failures/gaps, real
duplicate validation, independent adjudication, representative sampling/power,
checkpoint provenance/contamination, immutable predictions and complete scoring.
History: docs/sentiment-ledger-checkpoint-20261007.md.
Previous checklist: docs/day-checkpoints-20261012-before-duplicate-review.md.


## First duplicate-pair learning response — recorded at 17:27 Bangkok

The human judged the jobs-report announcement and inflation-report announcement
as different events. The original source/weekday rationale is preserved. Verified
source context identifies both article publishers as www.cnbc.com; "Fed" is the
organization discussed in the second title, not a different article publisher.
Assistant feedback identifies the clearer event-topic distinction: employment
versus inflation. That feedback is not falsely attributed as a new human statement.
Different publishers or posting weekdays alone do not establish different events.

Response: /app/data/research/duplicate-pair01-response01-20261012.json.
Post-content-persistence availability marker:
/app/data/research/duplicate-pair01-response01-availability-20261012.json.
Local evidence: docs/duplicate-pair01-review-20261012.json.
Decision 618a7f85-bc07-4800-8952-54ef14ac5022 became available at
2026-10-12T10:27:39.062761+00:00 after response contents were flushed/fsynced.
The marker commits separately. Dataset, manifest, eligibility, annotation ledger
and original audit hashes remain unchanged; no cluster or classifier label changed.
This is an assisted development response, not independent adjudicated ground truth
or an estimate of grouping precision/recall.
Next learning check: two publishers report the same specific employment release
on different days; decide whether that alone creates a different underlying event.


## Second candidate-pair response — recorded at 17:50 Bangkok

The human judged the two September-employment articles different and noted their
different outcomes/tones. Original reasoning is preserved, not replaced. Captured
feed context identifies the first as a BLS nonfarm-payrolls/unemployment release;
the second headline identifies an ADP private-sector jobs release. Both article
publishers are CNBC. The stronger difference is underlying report identity.

Assistant feedback: shared month/topic and different sentiment alone do not prove
same or different event. Different financial subjects/angles can produce different
sentiments about one event. Keep same-event relation separate from same-information
relation; new facts or another angle must remain as their own input version, label,
provenance and availability even if assigned to one event family.

Response: /app/data/research/duplicate-pair02-response01-20261012.json.
Availability marker: /app/data/research/duplicate-pair02-response01-availability-20261012.json.
Local evidence: docs/duplicate-pair02-review-20261012.json.
Decision 594d9970-87d1-487d-b3ed-c4eb820a7418 became available at
2026-10-12T10:50:39.355732+00:00 after content flush/fsync. The marker is separate.
The captured summary was used only for event identity, not to change the headline-
only sentiment labels. Dataset, manifest, annotation/eligibility ledgers and audit
hashes stayed unchanged. No live dedup, cluster or label changed.

This is an assisted development response with source-context feedback, not independent
adjudicated ground truth or measured duplicate recall. We still need genuine same-event
rewrites, updates and independently searched missed pairs, not only negative examples.
Teaching examples of profits rising while shares fall are hypothetical and cannot be
recorded as real-feed validation or added to experiment samples.
