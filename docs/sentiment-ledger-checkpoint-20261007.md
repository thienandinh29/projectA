# Sentiment-ledger development checkpoint — October 7, 2026

Verified at 08:43 UTC / 15:43 Bangkok. Detailed summary:
`sentiment-ledger-verification-20261007.json`. Full code hashes, Python version
and installed-package versions are archived in the Docker volume at
`/app/data/research/sentiment-ledger-verification-20261007.json`.

## Completed

- Saved working development guide v1 while preserving the original draft.
- Added a separate append-only sentiment ledger and prepare/record/recover/select
  workflow. Human judgments bind exact content, input selection and guide hashes.
- Kept unresolved/mixed cases distinct from neutral, assisted judgments disclosed,
  revisions explicit, and independent annotators in separate chains.
- Persisted contents before availability sampling; excluded unsealed records and
  tested present-time recovery without replacing or backdating earlier evidence.
- Replayed archived eligibility membership at the requested annotation cutoff.
- Passed 38 tests locally and in rebuilt Docker: 13 annotation, 8 eligibility,
  3 source-review and 14 existing research-pipeline checks.
- Isolated the local-file export regression's environment after Docker revealed
  its assumption about `RESEARCH_DATABASE_ID`. The production volume identity and
  exporter behavior are unchanged. Final tests passed in both environments.
- Prepared three blank, deliberately selected teaching examples (jobs-report
  announcement, Microsoft stock outlook, Intel stock decline). This is not
  representative sampling or an accuracy estimate.

## Runtime evidence

At 08:40 UTC, both named session collectors and the writer remained exited with
code 0. Docker Desktop was started after the first API check failed. No collector
or writer was restarted by this annotation workflow. Broker/Redis resumed with
Docker's configured policies.

The frozen snapshot still has 41 versions and 38 article IDs. Latest saved human
eligibility: 18 included, 1 excluded, 1 uncertain, 21 unreviewed. The snapshot bytes,
manifest and eligibility ledger remained unchanged during preparation. A read-only
DuckDB check found 2,413 Bronze rows and 529 research versions, consistent with the
saved closure evidence; this was not a new full readiness audit.

Blank template: `/app/data/research/sentiment-teaching-batch01-20261007.json`.
Empty selection: `/app/data/research/sentiment-unlabeled-20261007`, cutoff
`2026-10-07T08:43:05.027023+00:00`: all 18 eligible versions pending, zero labels.
At that preparation checkpoint, the annotation ledger had not been created because
no formal judgment had been recorded. Earlier practice answers and invented examples
were not imported. The later approved first batch is documented below.

## Completion boundary and next step

Next: obtain fresh human subject/sentiment/reason judgments for the exact teaching
versions, disclose assistance, append them, and verify earlier versus later cutoff
selections. No model fitting or scoring was run. Current work is uncommitted and
has not been pushed to GitHub.

The release timestamp confirms already-persisted contents; the marker commits in a
separate transaction. It does not establish an atomic publication instant or UTC
clock synchronization. Existing eligibility's generic timestamp is still sampled
before its content commit; saved historical selections use post-return cutoffs,
but that contract requires hardening before final protocol freeze.

Representative pilot selection, independent annotation/adjudication, source/gap
recovery, checkpoint contamination/provenance, frozen prediction logs, full scoring
and final-test rules remain open. The image rebuild resolved unpinned dependency
ranges anew; its environment was recorded, not made into a final experiment lock.

## First human response, clarification initially pending

The user supplied unresolved / positive / negative for the three teaching versions,
with reasons, but used "financial subject" as the subject text for each. Exact
answers are preserved in the ignored local draft
`data/research/sentiment-teaching-batch01-response01-pending-20261007.json`.
The preservation timestamp is not a formal label-availability timestamp.

Clarification is needed: identify specific subjects, and distinguish item1's
absence of expressed direction (normally neutral under the working guide) from
an unresolved interpretation. The original unresolved answer is not replaced by
the assistant's proposed correction. No formal labels were appended at this step.

## Human-approved first batch recorded

The user explicitly authorized the proposed specific subjects and item1's neutral
correction ("alright change foe me"). Three first formal decisions were appended
to `/app/data/research/annotations.sqlite3`, all released at
`2026-10-07T08:52:36.822308+00:00` / 15:52:36 Bangkok:

| Subject | Sentiment | Decision ID |
| --- | --- | --- |
| Jobs-report announcement | neutral | `8dcae5dc-0d5b-4c5d-8d77-020d46ca6b69` |
| Microsoft's stock | positive | `6a8aa7a1-9088-42ed-9c3d-2d35513f7bf6` |
| Intel's stock | negative | `d78713ba-da1b-4c60-b6f4-9d58075ea9e0` |

Original reasons were retained. The initial unresolved answer remains in the
preserved response; it was corrected with human authorization before any formal
release, so it is not represented as a superseded ledger decision. The raw response
and approval evidence were also archived in the authoritative Docker volume.
All three judgments disclose assisted teaching and assistant-derived context flags;
they are not independent blinded annotation. The Microsoft outlook is flagged as
a forecast and attributed opinion. The jobs-report announcement does not forecast
employment outcomes. No model predictions, training or performance scores were created.

Live checks confirmed:

- The earlier 08:43:05 cutoff still has 18 pending and zero released labels.
- A cutoff exactly equal to release time still has zero labels (strictly-before rule).
- The post-release cutoff has 3 labeled / 15 pending, matching the confirmed subjects
  and sentiments, with every exported availability preceding its cutoff.
- All three records are sealed. Snapshot bytes, manifest, eligibility ledger and
  original blank template remain unchanged.

Detailed evidence: `sentiment-teaching-batch01-recorded-20261007.json` and Docker
`/app/data/research/sentiment-teaching-batch01-recorded-summary-20261007.json`.
The new current selection is
`/app/data/research/sentiment-teaching-batch01-current-20261007`. The previous empty
selection is retained. This is a small deliberately selected development exercise,
not completion of the annotation pilot or final research evaluation.

## Second teaching batch prepared

At `2026-10-07T09:09:06.454468+00:00`, the next three unlabeled included versions
were selected in availability/observation/version order. The template is
`/app/data/research/sentiment-teaching-batch02-20261007.json` and local preparation
evidence is `sentiment-teaching-batch02-preparation-20261007.json`.
Subject, status, sentiment and reason are still blank. No new judgments were
generated; counts remain 3 labeled / 15 pending. Read-only checks preserved the
prior annotation history, snapshot, manifest and eligibility ledger exactly.


## October 12: batch02 human response, clarification pending

The user supplied negative / unresolved / negative, with subjects "population
news", "job's news" and "labor market news". The exact answer is preserved locally
in data/research/sentiment-teaching-batch02-response01-pending-20261012.json, bound
to the saved dataset/selection/guide/template hashes and exact content versions.

Proposed subjects are public finances, men's position in the labor market, and
the labor market. Item2's "losing ground" explicitly conveys worsening position,
so negative is proposed under the guide. No correction is applied without the
human's confirmation; the original unresolved judgment remains preserved.
No formal records were appended, and the live Docker ledger was not inspected.
The October 7 preparation date must not become availability for a later label.


## October 12: batch02 confirmed and recorded

The human answered "alright" to the proposed specific subjects and changing
item2 to negative after the explanation of "losing ground". The original pending
answer and its unresolved judgment are retained, with approval evidence, locally
and in the authoritative Docker volume. No earlier released decision was replaced.

Three initial formal decisions were sealed at
2026-10-12T08:48:30.425681+00:00 / 15:48:30 Bangkok:

| Subject | Sentiment | Decision ID |
| --- | --- | --- |
| Public finances | negative | b97ce266-b9eb-4a31-b7ac-bda2fd6ad41e |
| Men's position in the labor market | negative | e82809d2-7da0-4b37-8bea-b177b1588f5c |
| Labor market | negative | 201ba571-0c88-49fc-b4d7-c580ac12b115 |

Item2's formal reason is the human-approved assistant clarification; the original
reason remains in the raw response. Context flags and the clarification are disclosed
as assistance. Item1 is flagged as a forecast; item3's overall stated message is
unfavorable rather than inferred from "increased" alone. None is an independent
blinded annotation or a statement validating the publisher's factual claims.

Live checks found six sealed records, with the original three histories unchanged.
October 7, immediately-before-recording and exact-release cutoffs still contain
3 labeled / 15 pending; the current cutoff contains 6 labeled / 12 pending.
The new labels carry October 12 availability, never the template's October 7 date.
Dataset, manifest, eligibility ledger and original template hashes are unchanged.
All named collectors and the writer were rechecked stopped with exit code 0.

Detailed summary: sentiment-teaching-batch02-recorded-20261012.json.
Docker summary: /app/data/research/sentiment-teaching-batch02-recorded-summary-20261012.json.
The recording/import code was unchanged; this session used live consistency and
cutoff checks. The earlier 38-test result remains dated October 7.
No model fitting, predictions, scores or GitHub push were performed.


## Active batch03 — prepared October 12 at 15:52 Bangkok

The next three pending included versions were selected in availability/observation/
version order: core inflation, private-sector jobs, and an inflation-report announcement.
Template: /app/data/research/sentiment-teaching-batch03-20261012.json.
Evidence: docs/sentiment-teaching-batch03-preparation-20261012.json.
Human subject/status/sentiment/reason fields remain blank. Counts stay 6 labeled /
12 pending. Prior annotation history, dataset, manifest and eligibility ledger
were checked unchanged. Next: obtain human subjects, sentiments and reasons.


### Batch03 first human response — clarification pending

Original answers (positive / positive / unresolved) and original topic descriptions
are preserved in data/research/sentiment-teaching-batch03-response01-pending-20261012.json.
No formal labels were appended. Clarify specific subjects, item1's comparison
with expectations rather than a demonstrated decline over time, and item3's
neutral announcement versus an unresolved interpretation. Proposed subject/perspective:
core inflation relative to expectations (inflation pressure), private-sector
employment, and the inflation-report announcement. The original judgments are
not automatically replaced. The last verified formal count remains 6 labeled /
12 pending from the batch03 preparation.


## October 12: batch03 confirmed and recorded

After the user answered "alright" to the proposed subjects, clarified reason for
item1 and neutral correction for item3, three first formal decisions were appended:

| Subject | Sentiment | Decision ID |
| --- | --- | --- |
| Core inflation relative to expectations (lower inflation pressure) | positive | ebbd3f1b-1500-422a-bf50-65d635be23d9 |
| Private-sector employment | positive | 6aee7484-4fbe-47c5-b499-69201a4defa9 |
| Inflation-report announcement | neutral | 85590472-3725-48f2-a240-67515d94b796 |

All became available at 2026-10-12T09:05:11.913220+00:00 / 16:05:11 Bangkok.
Original answers/reasons, including item1's claimed inflation reduction and item3
unresolved, are preserved locally and in Docker. The adopted reasons for items1
and3 are assistant-provided, human-approved clarifications. All judgments disclose
assistance. Inflation positivity is explicitly perspective-specific; the headline
compares with expectations and does not prove a fall from the previous month.

Live checks verified nine sealed decisions with the original six unchanged.
The October 7 cutoff still selects 3 labels; batch02, immediately-before and
exact-release cutoffs still select 6; the current cutoff selects 9 labeled / 9 pending.
All exported availability times precede their cutoff strictly. Dataset, manifest,
eligibility ledger and original blank template hashes are unchanged.
Original pending judgments were clarified before first formal release; they are
not falsely represented as superseded released ledger decisions.

Detailed evidence: sentiment-teaching-batch03-recorded-20261012.json and Docker
/app/data/research/sentiment-teaching-batch03-recorded-summary-20261012.json.
This remains assisted development annotation; no independent agreement, model
performance, factual validation of source claims, or final-test completion is claimed.
No code changed; actual record/import/export checks were performed.


## Active batch04 — prepared October 12 at 16:08 Bangkok

The next three unlabeled included versions, in availability/observation/version
order, concern consumer optimism, Canadian import restrictions, and Australian
interest-rate policy. Human subject/status/sentiment/reason fields remain blank.
Template: /app/data/research/sentiment-teaching-batch04-20261012.json.
Evidence: docs/sentiment-teaching-batch04-preparation-20261012.json.
Counts remain 9 labeled / 9 pending. Existing annotations, snapshot, manifest and
eligibility history were verified unchanged. Next: collect human judgments,
including explicit financial perspectives for policy-related headlines.


### Batch04 first human response — financial perspective clarification pending

Original human answers are negative / negative / negative, with topic descriptions
and reasons preserved in data/research/sentiment-teaching-batch04-response01-pending-20261012.json.
No formal labels were appended. Proposed subjects are consumer confidence, affected
Canadian products' access to the US market, and Australian interest-rate policy.
Item2 negativity is limited to the explicitly restricted access perspective.
For item3, the guide gives rates no universal polarity; a numerical rise alone
does not identify whose financial outlook is favorable or unfavorable. Unresolved
is proposed pending human confirmation, with the original negative answer intact.
The last verified formal counts remain 9 labeled / 9 pending.


## October 12: batch04 recorded, including one unresolved judgment

The user answered "alright" to the proposed subjects and changing item3 from
negative to unresolved. Two labeled judgments and one unresolved judgment were
sealed at 2026-10-12T09:19:35.009624+00:00 / 16:19:35 Bangkok:

| Subject | Status | Sentiment | Decision ID |
| --- | --- | --- | --- |
| Consumer confidence | labeled | negative | 5f68f194-d3c3-49e5-841d-ea065d0ae94a |
| Affected Canadian products' US market access | labeled | negative | df5aafcf-761d-481a-b536-896463f357bb |
| Australian interest-rate policy | unresolved | null | 1a34d3e7-2520-4754-9842-987898626ea5 |

The original negative / negative / negative response and reasons remain preserved
locally and in Docker. Clarified reasons for items2 and3 and contextual flags are
assistant-provided and disclosed as assistance. Import sentiment is limited to
restricted market access. The interest-rate headline does not select an affected
financial perspective; unresolved does not mean neutral or a fourth model class.

Live verification found 12 sealed review records, preserving the prior 9 exactly.
Earlier batch03, immediately-before, and exact-release cutoffs retain 9 labeled /
9 pending. The current report has 11 labeled / 1 unresolved / 6 pending. Only the
11 labeled versions enter labels.jsonl. The unresolved version remains in the report
with null sentiment and in all 12 released history records. No judgment was lost
or silently converted to neutral. Dataset/manifest/eligibility/template hashes are
unchanged. No model predictions, fitting, scoring or code changes occurred.

Detailed evidence: sentiment-teaching-batch04-recorded-20261012.json and Docker
/app/data/research/sentiment-teaching-batch04-recorded-summary-20261012.json.
The previous full TODAY checklist is preserved as
day-checkpoints-20261012-before-batch04.md; the active checklist now separates
labeled, unresolved and pending counts explicitly. These are assisted development
reviews, not blinded ground truth, representative performance or final-test completion.


## Active batch05 — prepared October 12 at 16:28 Bangkok

The next three pending versions, ordered by availability/observation/version, concern
mortgage borrowers/overspending, Nvidia's stock outlook, and Western Digital/Seagate
shares. All human judgment fields remain blank. Counts stay 11 labeled / 1 unresolved /
6 pending. Prior annotations, the unresolved case, dataset, manifest and eligibility
ledger were verified unchanged.
Template: /app/data/research/sentiment-teaching-batch05-20261012.json.
Preparation evidence: docs/sentiment-teaching-batch05-preparation-20261012.json.
Next: obtain the human's specific financial subjects, sentiments and reasons.


## October 12: batch05 recorded from the human's direct answers

The user supplied negative / positive / positive with reasons for all three.
These polarities were recorded unchanged, with spelling normalized. Specific subject
strings were assistant-normalized from the exact headline and user description;
that assistance and authorship are disclosed rather than inventing a separate
confirmation message for the rewritten subject strings.

Three initial decisions were sealed at
2026-10-12T09:42:39.947604+00:00 / 16:42:39 Bangkok:

| Subject | Sentiment | Decision ID |
| --- | --- | --- |
| Household/mortgage financial outlook | negative | 47a33e7b-bef5-4f29-990f-31af2d38aa00 |
| Nvidia's stock | positive | fcd4cb5d-d792-46b8-838a-11e2a35f64cf |
| Western Digital and Seagate shares | positive | ebe068c9-f7ed-4145-844a-dc2c56c3fa80 |

The exact human response and original reasons remain archived locally and in Docker.
The loan officer is the speaker of a household-finance warning; the recorded subject
identifies the financial outlook. Item3 says shares bounce back as analysts downplay
the threat. The learner's original "after" wording is preserved, with a note that it
does not verify chronology or causality. All context flags and normalization disclose
assistant help; no independent blinded annotation is claimed.

Live checks found 15 sealed review records, preserving the prior 12 exactly.
Earlier batch04, before-recording and exact-release cutoffs retain 11 labeled /
1 unresolved / 6 pending. The current selection contains 14 labeled / 1 unresolved /
3 pending. The unresolved interest-rate judgment remains unchanged, visible in the
report/history with null sentiment, and excluded from labels.jsonl. All exported
availability times precede the cutoff. Snapshot, manifest, eligibility ledger and
original template hashes are unchanged.

Evidence: sentiment-teaching-batch05-recorded-20261012.json and Docker
/app/data/research/sentiment-teaching-batch05-recorded-summary-20261012.json.
Previous TODAY contents are preserved in day-checkpoints-20261012-before-batch05.md.
No code changes, fitting, predictions, scores or GitHub push occurred.


## Active batch06 — final pending teaching batch, prepared October 12 at 16:46 Bangkok

The final three versions awaiting first review in the current 18-version included
cohort were selected in availability/observation/version order. They concern
alternative stock-market outcomes, a Medicare-rebate promise/hidden cost, and
investor opportunity amid a bond-market rout. Human judgment fields remain blank.
Template: /app/data/research/sentiment-teaching-batch06-20261012.json.
Evidence: docs/sentiment-teaching-batch06-preparation-20261012.json.
Counts remain 14 labeled / 1 unresolved / 3 pending. All prior annotations, the
unresolved case, frozen dataset, manifest and eligibility ledger were verified intact.
Next: obtain human subjects/perspectives, sentiments and reasons. Mixed directions
require discussion of dominance or an unresolved status, not automatic neutral.
Completion of this batch will finish first review of the currently included cohort,
not the full 41-version eligibility inventory, larger pilot or final-test protocol.


## October 12: batch06 items1 and3 recorded; rebate judgment remains pending

The user judged items1 and3 "uncertain" because each expresses competing directions.
Those intentions were preserved as unresolved status with null sentiment under the
guide, without changing polarity or generating a neutral/fourth model class.
Specific subjects/context flags were assistant-normalized and disclosed.

| Item | Subject | Status | Decision ID |
| --- | --- | --- | --- |
| 1 | Stock-market outlook | unresolved | b4fbe881-1140-4139-b5f7-648130b100f7 |
| 3 | Investor and bond-market outlook | unresolved | 95ee5dcd-385b-4070-ac84-1f6de3e97bfe |

Both became available at 2026-10-12T09:59:28.741107+00:00 / 16:59:28 Bangkok.
The two-item completed file references the original three-item template hash; the
original blank template and full human response remain preserved. Item2's positive
judgment/reason "it help the economic" is retained but not formalized while asking
whether the promised payment/hidden cost supports a dominant financial interpretation.

Live checks found 17 sealed reviews with prior 15 decisions exactly preserved.
Earlier and exact-release cutoffs retain 14 labeled / 1 unresolved / 3 pending;
current selection has 14 labeled / 3 unresolved / 1 pending. Item2 has no formal
decision. All unresolved versions remain in the report and full history, excluded
from the 14-row labels.jsonl. Snapshot/manifest/eligibility/template hashes unchanged.

Detailed evidence: sentiment-teaching-batch06-partial-recorded-20261012.json and
Docker /app/data/research/sentiment-teaching-batch06-partial-recorded-summary-20261012.json.
This is assisted development, not independent blinded annotation or a complete
larger pilot/final test. No model fitting, predictions, scores or code changes occurred.


## October12: included18 first review closed; next data task is duplicate review

The human answered "alright next today work" to the proposed unresolved Medicare
rebate interpretation. The original positive answer remains preserved; the first
formal decision 6ff51f0e-c79e-48c3-bbb4-7aee14382698 was sealed at
2026-10-12T10:08:22.193069+00:00. The cohort now has 14 labeled / 4 unresolved /
0 pending, with all18 formal review records and the prior17 unchanged. Earlier and
exact-release cutoffs preserve the earlier state; class-label output stays at14.
Full closure inventory and scope limitations are archived with their source hashes.

A subsequent read-only duplicate audit found 41 versions, 38 article IDs, two
multi-version articles, and 38 exact titles. The repeated versions have unchanged
titles and changed publisher timestamp fields. They are in the pending eligibility
part of the snapshot, not validated financial syndication examples. Included18
reference article/exact/lexical counts all equal18; all41 counts equal38. No
distinct-article pair meets lexical threshold0.75. Six prefix checks passed.
No human duplicate decisions, missed-pair recall or independent-event count was
established. All source/eligibility/annotation hashes remained unchanged.

Evidence: annotation-first-review-closure-20261012.json,
duplicate-inventory-20261012.json and duplicate-review-next-20261012.md.
Next step is a human candidate-pair exercise, then genuine rewrites/hard negatives
and a representative larger pilot. The first sentiment review is complete only
for currently included18; additional21 eligibility and one uncertainty remain open.
No experiment score, model training or final-test completion is claimed.
