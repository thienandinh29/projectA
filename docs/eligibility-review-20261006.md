# Eligibility checkpoint — October 6, 2026

The source-quality learning check is complete: fetching the same URL and unchanged
headline again is another observation of the same article. A story can also span
different articles from different publishers; that later grouping needs validation.

The eligibility tool is implemented and tested. Human eligibility decisions are
**not complete**: the initial selection of the verified 20-version snapshot produces
20 pending decisions. A subsequent first human review is uncertain; see the dated
entry below. The frozen input and initial selection are unchanged.

Policy: `docs/headline-eligibility-dev-v1.md`. For each exact version, review English,
financial relevance and genuine-headline status independently. Provide a reason.
Sentiment annotation is a separate later step. Repeated news stays in the candidates
for the duplicate-control experiment.

## Saved artifacts

In the shared Docker volume:

- Input: `/app/data/research/checkpoint2-corrected-20261006-snapshot`.
- Blank reviews: `/app/data/research/eligibility-review-20261006.jsonl`.
- Pending selection: `/app/data/research/eligibility-pending-20261006`.
- Selection cutoff: `2026-10-06T04:19:28Z` (11:19:28 Bangkok).
- Future actual decisions: `/app/data/research/eligibility.sqlite3`.

The selection contains `selection.json`, empty `eligible.jsonl`, `decisions.jsonl`
and the exact hashed policy. It is explicitly development evidence, not a final-test
dataset. No live DuckDB access or collector restart was required.

## Workflow

The research-tools image was rebuilt after adding these tools. After any source
change, rebuild it again before invoking them. Use a fresh output path for each
template or selection; existing frozen artifacts are never overwritten.

Prepare a blank review template (already done today):

```powershell
docker compose --profile tools run --rm --no-deps research-tools -m scripts.review_eligibility prepare --snapshot-dir /app/data/research/checkpoint2-corrected-20261006-snapshot --policy-file /app/docs/headline-eligibility-dev-v1.md --output /app/data/research/eligibility-review-20261006.jsonl
```

Read each matching title from the input dataset. Save only completed human records
in a separate `eligibility-completed-20261006.jsonl`, replacing null criteria with
`yes`, `no`, or `uncertain` and adding reasons. Blank templates cannot be recorded.
Do not insert sentiment labels into these records. The following example requires
that completed file and the actual reviewer's identifier; it has **not been run**:

```powershell
docker compose --profile tools run --rm --no-deps research-tools -m scripts.review_eligibility record --snapshot-dir /app/data/research/checkpoint2-corrected-20261006-snapshot --policy-file /app/docs/headline-eligibility-dev-v1.md --ledger /app/data/research/eligibility.sqlite3 --reviews /app/data/research/eligibility-completed-20261006.jsonl --reviewer YOUR_ACTUAL_REVIEWER_ID
```

Keep returned decision IDs. Corrections must set `supersedes_decision_id` to the
latest decision under that policy. The ledger blocks updates/deletes and rolls back
an entire batch when any review is invalid or stale. It records actual system time;
caller-supplied historical availability is rejected. Treat the local system clock
as an assumption, not an independently authenticated timestamp.

After reviewing, run `select` with a new output path and an explicit aware cutoff
later than the saved decisions. Selection requires both research-data availability
and decision availability strictly before that cutoff. Pending, uncertain and
excluded versions remain visible in the selection report. A new content version
never silently inherits the old version's judgment. Editing the policy changes its
hash and requires new reviews.

## Verification and remaining work

- 25 local tests passed: eligibility (8), source quality (3), existing research
  pipeline (14).
- 11 Docker tests passed: eligibility (8) and source quality (3).
- Live frozen-snapshot `prepare` and `select` succeeded: 20 pending, zero included.
- Tests cover immutable history, historical cutoff/boundary behavior, uncertainty,
  exact content/version checks, stale revisions, batch rollback, policy changes,
  unavailable data and invalidated snapshots.

Next: review the remaining candidates in a small human-reviewed batch.
The collection session has now passed drain/stop/audit/export; its polling gaps and
source failures are documented in `docs/session01-closure-20261006.md`.
No final-test protocol, final labels, independent-event count or paper readiness is
asserted by this eligibility checkpoint.

## First actual human review

The human user answered English=yes, financially relevant=yes and genuine
headline=uncertain for the traced CNBC headline. These are user judgments, not
assistant-generated labels. The stored reason restates that uncertainty without
inventing an explanation for it.

- Anonymous reviewer identifier: `project-owner` (the human user in this chat).
- Exact version/hash: `6a20838e0a78b624a065de3c8f66ce08d93d237546e0c1c8a10476253c1bad39`.
- Decision: `ce36fbc8-996b-4f78-b81d-211cbcfbdfe0`.
- Actual decision availability: `2026-10-06T04:24:12.783118+00:00`.
- Completed input: `/app/data/research/eligibility-completed-20261006-first.jsonl`.
- New selection: `/app/data/research/eligibility-first-review-20261006`.
- New cutoff: `2026-10-06T04:24:13Z`; result: 1 uncertain, 19 pending, 0 included.

Inspecting the frozen source item confirms `title_provenance=source_feed` and that
its saved `title` equals the displayed headline. The stored request ID is
`74840a36-f8aa-49dd-8d9d-e1e999248f79`. This supports authentic feed-headline
provenance; it does not verify the headline's substantive economic claims. The
assistant has not replaced the human's uncertain decision. Any change must append
a human-authorized revision referencing the decision above.

## Human-authorized revision

The user explicitly changed genuine headline to yes after the feed-title evidence
was shown. English=yes and financially relevant=yes remain unchanged. This is an
assisted human development review, not an independent second annotation.

- Revision: `3cfb8907-fc04-40d1-9aba-4b386dfd25a8`.
- Superseded decision: `ce36fbc8-996b-4f78-b81d-211cbcfbdfe0`.
- Actual revision availability: `2026-10-06T04:26:59.236349+00:00`.
- Completed input: `/app/data/research/eligibility-completed-20261006-revision01.jsonl`.
- New selection: `/app/data/research/eligibility-revised-review-20261006`.
- New cutoff: `2026-10-06T04:26:59.255035Z`; result: 1 included, 19 pending.

Live checks verified two ledger entries for this exact version, byte-for-field
preservation of the original stored decision, uncertain status at the old cutoff
`2026-10-06T04:24:13Z`, and included status after the revision. Original frozen
input and selection directories were preserved. Eligibility remains separate from
sentiment labels, independent-event grouping and final-test approval.

## Next human review batch prepared

`/app/data/research/eligibility-batch02-20261006.json` was saved at
`2026-10-06T04:29:44.738947+00:00`. It contains the first three pending versions
ordered by `available_at`, `observed_at`, `version_id`, with exact source-title
matches and saved request evidence. No sentiment predictions, subsequent returns
or handpicked difficulty scores were used to select them.

1. `650217b328e5b6a5b50f391d5fbf218cad4a5f3c698bfc225ad92335d5445b62`.
2. `8d29ede45e8d848c3b80a23ee70f0e8be63483dbc7d9b302a1701330d0ca7764`.
3. `37672e741f83bab6def7e4591a5e39281729205e5cb936c12ecba69e22758675`.

Each is a different content version with its own headline; this does not establish
three independent economic events. The prepared batch keeps its original blank
fields; the actual human answers were subsequently saved separately below.

## Original batch02 human answers before criteria clarification

All three actual human answers and reasons were appended at
`2026-10-06T05:54:09.683665+00:00`, under reviewer `project-owner`:

| Item | English / financial / genuine | Decision ID |
| --- | --- | --- |
| 1 | yes / no / yes | `c5066ea7-93bf-49bb-9a07-fa15e5b95eba` |
| 2 | yes / yes / no | `2561b9cc-4f3d-4488-bc06-399e05072703` |
| 3 | yes / yes / yes | `2022f040-3a94-44bf-9726-78d39224ddb9` |

The user's original reasons, including wording, are retained in
`/app/data/research/eligibility-completed-20261006-batch02.jsonl` and the ledger.
The new frozen selection `/app/data/research/eligibility-batch02-reviewed-20261006`
uses cutoff `2026-10-06T05:54:10Z` and shows 2 included, 2 excluded, 16 pending
for the original 20-version snapshot. The newer 41-version snapshot has the same
four reviewed versions, plus 37 pending.

Two answers need clarification against the existing policy: macroeconomic/labor
market news is financially relevant within this scope, and a long sentence can
be an authentic publisher headline. All three exact strings match saved feed-title
fields. No human decision was overridden by the assistant. Any revision requires
the human's instruction and appends another decision instead of replacing these
records. The exclusions are provisional development judgments, not validated final
annotation decisions or evidence that the excluded records violate the policy.

## Batch02 corrections explicitly authorized by the human

After clarification of macroeconomic relevance and publisher-title provenance,
the user instructed "alright change it all for me". Items 1 and 2 were revised;
item 3 already had yes/yes/yes and required no additional ledger entry. This is
assisted human development review, not independent second-annotator agreement.

| Item | Revised criterion | New decision ID | Superseded decision |
| --- | --- | --- | --- |
| 1 | financially_relevant=yes | `b1990550-06fa-435c-8f93-3f1a87ef76e8` | `c5066ea7-93bf-49bb-9a07-fa15e5b95eba` |
| 2 | genuine_headline=yes | `e3135326-e241-4925-8d98-68612dcd4946` | `2561b9cc-4f3d-4488-bc06-399e05072703` |

Both revisions became available at `2026-10-06T06:05:37.950263+00:00`. Completed
inputs are saved separately in
`/app/data/research/eligibility-completed-20261006-batch02-revision01.jsonl`.
The new selection `/app/data/research/eligibility-batch02-revised-20261006` uses
the newer 41-version snapshot and cutoff `2026-10-06T06:05:37.976203Z`:
**4 included / 37 pending**, with all three batch02 headlines included.

Live verification confirmed every previous stored decision remained unchanged,
only two revisions were added atomically, and the third decision remained
`2022f040-3a94-44bf-9726-78d39224ddb9`. Re-selecting the same newer snapshot at
the earlier cutoff `2026-10-06T05:54:10Z` still gives 2 included / 2 excluded /
37 pending. The earlier selections remain immutable. No sentiment labels, final
test eligibility, additional candidate judgments or protocol changes were made.

## Batch03 awaiting human review

`/app/data/research/eligibility-batch03-20261006.json` was prepared at
`2026-10-06T06:11:06.158822+00:00` from the newer frozen 41-version snapshot.
The next three pending versions, in the same deterministic order, are:

1. `f5f0a58b34698f039b50219ab8277e71193132f3b9480687516a62263d9b6909`.
2. `50722746cb907c895ced4934d461426b08298f777cc660447987211b7bfe1261`.
3. `11a2ada1e3964db64d1124eeb5df5d66692c39a2042edd78ac333bb97534bed3`.

All exact headline strings match their saved CNBC feed-title fields. The batch
contains source/request identities and input dataset, manifest and policy hashes;
human eligibility and reason fields remain blank. No model predictions, market
returns or handpicked topic filters determined this batch. Preparation is not
human review: counts remain 4 included / 37 pending, and no sentiment labels exist.

## Batch03 actual answers, numeric-headline clarification pending

At `2026-10-06T06:20:57.900626+00:00` the three human answers were appended:

| Item | English / financial / genuine | Decision ID |
| --- | --- | --- |
| 1 | yes / yes / no | `9b3d2037-aaa0-437b-8e5e-53d2f55096e6` |
| 2 | yes / yes / yes | `17ee1a97-0383-4c43-9639-d3a6bc738646` |
| 3 | yes / yes / yes | `75209fef-8366-4d42-ab93-5dbba3620186` |

Original reasons are preserved in the ledger and
`/app/data/research/eligibility-completed-20261006-batch03.jsonl`. The user also
expressed uncertainty about item1's answer. Their no answer has not been silently
converted to uncertain or yes. The factual source evidence was rechecked: the
exact CNBC feed title includes `3.0%`. The unchanged policy contains no rule
excluding numerical values, percentages, currency signs or long headline strings.
Genuine-headline review concerns meaningful publisher-supplied title provenance.
The truth of the quoted inflation claim is a separate question not established by
these eligibility checks.

The saved selection `/app/data/research/eligibility-batch03-reviewed-20261006`
uses cutoff `2026-10-06T06:20:57.914142Z` and has 6 included, 1 excluded pending
clarification and 34 unreviewed versions. This is development/pilot review evidence,
not validated final annotation. Next: the human decides whether to revise item1;
any authorized change appends another decision preserving this original answer.

## Batch03 numeric-headline revision authorized and verified

The human explicitly authorized changing item1's genuine-headline answer to yes.
Revision `6827f4b6-84cc-4891-8aa3-7663c92280fb` supersedes
`9b3d2037-aaa0-437b-8e5e-53d2f55096e6` and became available at
`2026-10-06T06:22:50.548587+00:00`. English and financial relevance remain yes.
Items2 and3 were already yes/yes/yes and received no unnecessary revision entries.

Saved input: `/app/data/research/eligibility-completed-20261006-batch03-revision01.jsonl`.
New selection: `/app/data/research/eligibility-batch03-revised-20261006`, cutoff
`2026-10-06T06:22:50.569408Z`, result **7 included / 34 pending**. All three
headlines in batch03 are now included under the unchanged development policy.

Live checks verified that all preceding decisions remained unchanged, exactly one
revision was appended, and re-selection at the previous cutoff still yields
6 included / 1 excluded / 34 pending. The original no answer and earlier frozen
selection remain preserved. This assisted human pilot correction is not independent
annotator agreement or a sentiment label; the remaining 34 candidates are unreviewed.

## Batch04 prepared for human review

At `2026-10-06T06:24:35.677908+00:00`, the next three pending versions were saved
to `/app/data/research/eligibility-batch04-20261006.json`:

1. `c9d6a6e73099d577f56d61cb4de6110bd83def1fd936bd341a4aafe144d8bf42`.
2. `3fcffe4ed3688d6a05b2a80e7cd8ab37db9290c14a079c20e9d1b4cb83876c15`.
3. `8f80d13b91a89d9dd37fc39daea5e74ce713e1f2ceab1e20ffeb1c097862d0a9`.

Selection still uses availability, observation and version ID order. All titles
match saved CNBC feed-title strings. Exact identities, input dataset/manifest/
policy hashes and request evidence are included. No human judgments, sentiment
labels or additional inclusion decisions were generated. Current counts remain
7 included / 34 pending.

## Batch04 actual human reviews saved

All three received explicit human yes/yes/yes judgments and reasons at
`2026-10-06T06:30:54.470095+00:00`, under reviewer `project-owner`:

| Item | Decision ID |
| --- | --- |
| 1 | `9f872bcb-d4c6-4c69-85b7-ddbf7e73f97c` |
| 2 | `c764caf8-0d55-40a1-8aba-bf8809ab4ae9` |
| 3 | `f63232d7-68a4-4c1f-a957-a653be3fc24f` |

Completed inputs: `/app/data/research/eligibility-completed-20261006-batch04.jsonl`.
Original reasons, including the user's wording, are retained in the ledger.
The new selection `/app/data/research/eligibility-batch04-reviewed-20261006` uses
cutoff `2026-10-06T06:30:54.484960Z` and contains **10 included / 31 pending**.

Live checks verified that exactly three new decisions were added, all prior
decisions remained unchanged, and selection at the batch-preparation cutoff still
gives 7 included / 34 pending. No sentiment labels or independent-event counts
were created. These remain assisted human development reviews. Relevant meaning
comes from consumer confidence, trade restrictions and monetary policy, rather
than a keyword-only rule; genuine-headline evidence comes from saved publisher
title fields rather than judging how the string looks or sounds.

## Batch05 prepared, awaiting human judgments

`/app/data/research/eligibility-batch05-20261006.json` was saved at
`2026-10-06T06:33:28.065356+00:00`. The same deterministic ordering selects:

1. `4a954e9fcf6437b132a77befc24b20e95f7982ad1781183e428ae595fada7a61`.
2. `cdc2ce8feb4a2c75081bbf8bec8af2401a33fb31a57d30e54becdbca3c75fd2f`.
3. `1437c07a646531c9128de47cdbaafc0ea9d65317bc2d92c81a15cf998b8a2ebe`.

These MarketWatch titles exactly match saved feed-title strings. The prepared
batch contains version/content hashes, input dataset/manifest/policy hashes,
timestamps and request identities. Eligibility fields and reasons remain blank.
No handpicked examples, model predictions or downstream returns influenced the
batch. Preparation leaves counts at 10 included / 31 pending. Publisher identity
alone cannot establish financial relevance; each exact headline still needs review.

## Batch05 human reviews saved after restoring Docker access

The user answered yes/yes/yes for items1 and3 and yes/no/yes for item2, with explicit
reasons. The first import attempt at approximately 09:27 UTC failed because the
Docker engine was unavailable; no ledger import occurred then. Answers were
preserved locally in `data/research/eligibility-completed-20261006-batch05-queued.jsonl`.
Docker Desktop was started with its CLI. All named collectors and the writer were
verified still stopped, exit code 0 and no automatic restarts.

The exact queued bytes were mounted read-only, copied into the Docker research
volume, validated against the frozen batch version/hash identities and imported
under reviewer `project-owner` at `2026-10-06T09:31:15.066465+00:00`:

| Item | English / financial / genuine | Decision ID |
| --- | --- | --- |
| 1 | yes / yes / yes | `ed40fe47-41fb-491c-ae11-80bf32a104cc` |
| 2 | yes / no / yes | `1784d75d-48cb-4268-b997-6a0a2327710d` |
| 3 | yes / yes / yes | `afac905b-6b50-4167-bab6-34bfc6ffbe88` |

Volume input: `/app/data/research/eligibility-completed-20261006-batch05.jsonl`.
Selection: `/app/data/research/eligibility-batch05-reviewed-20261006`, cutoff
`2026-10-06T09:31:15.083149Z`, counts **12 included / 1 excluded / 28 pending**.
The excluded health/safety headline and its original source evidence remain stored;
this is a research selection decision, not deletion.

Live checks verified exactly three new decisions, preservation of all prior
decisions, and unchanged 10 included / 31 pending at the batch-preparation cutoff.
Import availability was not backdated to the earlier preparation or local queue.
The user's wording is preserved. No sentiment labels or final-test claims were
created. The reasons are consistent with headline-level mortgage/personal-finance,
health/safety and equity-investment subjects. Keywords are clues; classification
uses the full headline meaning and the fixed policy.

## Batch06 prepared for human review

At `2026-10-06T09:36:56.860439+00:00`, the next three pending versions were saved
to `/app/data/research/eligibility-batch06-20261006.json`:

1. `c95ac4958ee5a1a0a525792db1d749c71dbc5fb433e8d1967ef688a32eb71f41`.
2. `ac41e9a94980764867bd8fbd31dc8777b722a801b7289e4e347abf8351557d49`.
3. `861764a9f5b764a6921f83c14475e2e0e53472b39aa5b4c6d5f523351fa9e0fa`.

All titles exactly match their saved MarketWatch feed-title fields. Selection
still uses available_at, observed_at and version ID order, without sentiment
predictions, subsequent market outcomes or handpicked topics. The artifact includes
exact content/input/policy hashes and source/request evidence. All human judgments
remain blank; counts stay 12 included / 1 excluded / 28 pending. Ambiguous financial
relevance may be marked uncertain under the existing policy; no judgment is invented.

## Batch06 actual human reviews, one uncertainty retained

The human explicitly marked financial relevance uncertain for item1 and all three
criteria yes for items2 and3. Original reasons were preserved and appended under
reviewer `project-owner` at `2026-10-06T09:43:31.246661+00:00`:

| Item | English / financial / genuine | Decision ID |
| --- | --- | --- |
| 1 | yes / uncertain / yes | `0ced963e-a68e-4e4f-8aa5-f65fc73da626` |
| 2 | yes / yes / yes | `995fc69f-a332-4bdc-b942-d073737bae21` |
| 3 | yes / yes / yes | `c7b1acab-dbcb-4f37-ad49-700c4bb2ac7d` |

Completed input: `/app/data/research/eligibility-completed-20261006-batch06.jsonl`.
Selection: `/app/data/research/eligibility-batch06-reviewed-20261006`, cutoff
`2026-10-06T09:43:31.264117Z`, result **14 included / 1 excluded / 1 uncertain /
25 pending**. The uncertainty is an eligibility state, not a neutral sentiment
label, and the exact version is retained for later adjudication. No financial
context was inferred from the article body, model predictions or the publisher name.

Live checks verified three new decisions, preservation of all prior records, and
unchanged 12 included / 1 excluded / 28 pending when selecting at the earlier batch
preparation cutoff. These remain assisted human development judgments. No ground-
truth syndication validation, model experiments or final-test eligibility was claimed.

## Batch07 prepared; initial exercise boundary made explicit

`/app/data/research/eligibility-batch07-20261006.json` was prepared at
`2026-10-06T09:46:52.499442+00:00` from the next three pending versions:

1. `e2edbb824043b883e1330f6c39abcf359e9c809f8b65585bb32430fe7f400bd0`.
2. `500be8fcff9e499cd134eaf9c5b506852f5bf57b4aa802fa037d6c97758e5077`.
3. `8839d140fe7571e1571b065ed747d9f45a2e06ba11825ed2fd09e07ec5194eac`.

All titles match saved MarketWatch feed-title strings. Selection uses the unchanged
availability/observation/version order and preserves input/content/policy hashes
and request evidence. Human judgments remain blank. Counts stay 14 included /
1 excluded / 1 uncertain / 25 pending.

Checking the original corrected 20-version snapshot shows 16 reviewed versions
(14 included, 1 excluded, 1 uncertain) and 4 pending. After this three-version batch,
one original version will still need review. We will finish that initial exercise
and move to drafting the sentiment guide. The 21 versions added by the longer
session, the uncertain technology headline and the intended larger annotation
pilot still require work. Finishing this exercise will not establish independent
events, meet the 50–100-example pilot aim, certify the full cohort or freeze a
paper-ready final test set.

## Batch07 actual judgments, rebate relevance uncertain

At `2026-10-06T09:54:42.871192+00:00`, reviewer `project-owner` explicitly provided:

| Item | English / financial / genuine | Decision ID |
| --- | --- | --- |
| 1 | yes / yes / yes | `a455d9b8-c485-44a5-bdef-447361f55102` |
| 2 | yes / uncertain / yes | `3544755b-62a8-4c1e-b9b3-05b7908080a6` |
| 3 | yes / yes / yes | `1517403d-063a-40ce-9aff-f68f1d263461` |

Original reasons are preserved in the ledger and
`/app/data/research/eligibility-completed-20261006-batch07.jsonl`. New selection
`/app/data/research/eligibility-batch07-reviewed-20261006` uses cutoff
`2026-10-06T09:54:42.899945Z`: **16 included / 1 excluded / 2 uncertain / 22 pending**.
The original20 now have 16 included / 1 excluded / 2 uncertain / 1 pending.

Live checks confirmed exactly three new decisions, intact prior records and unchanged
counts at the batch-preparation cutoff. Item2 remains uncertain; no assistant judgment
has replaced it. The clarification concerns meaning: rebate checks and their costs
are financial payments/costs within the public-finance scope. A dollar sign alone is
insufficient for eligibility, and these checks do not verify the political claim or
the payment's legality, feasibility or existence. Any revision needs the human's
explicit judgment and an appended record. The technology uncertainty remains open.

## Batch07 rebate relevance revised by the human

The user explicitly authorized item2's financial relevance to change from uncertain
to yes after clarification of rebate payments/costs within the existing policy.
Revision `53415277-5883-4b08-91ed-e998c83c2a1d` supersedes
`3544755b-62a8-4c1e-b9b3-05b7908080a6` and became available at
`2026-10-06T09:57:46.256089+00:00`. Other criteria and the two already-included
headlines in this batch received no additional changes.

Completed input: `/app/data/research/eligibility-completed-20261006-batch07-revision01.jsonl`.
New selection: `/app/data/research/eligibility-batch07-revised-20261006`, cutoff
`2026-10-06T09:57:46.273349Z`, counts **17 included / 1 excluded / 1 uncertain /
22 pending**. The remaining uncertain technology headline is a separate case.

Live checks verified one new revision, all previous records intact, and unchanged
16 included / 1 excluded / 2 uncertain / 22 pending at the previous cutoff. Original
uncertainty and earlier frozen selections are preserved. This is an assisted human
pilot correction, not independent second-annotator agreement, a sentiment label,
or verification of the political claim. One original20 version still needs review
before the initial exercise proceeds to drafting the sentiment annotation guide.

## Batch08: last pending original20 version prepared

At `2026-10-06T10:00:37.552123+00:00`, the sole pending version in the original
20-version snapshot was saved to `/app/data/research/eligibility-batch08-20261006.json`:
`a9ab679ed5d0832d8f3a918c75f52e022028d80a74e7074dec58740da137c5c3`.

The displayed title exactly matches the saved MarketWatch feed-title field. The
artifact binds the new41 input dataset/manifest, original20 dataset, current policy,
content version and request evidence. Human judgments remain blank. The initial20
have 17 included / 1 excluded / 1 uncertain / 1 pending; the full41 have 17 included /
1 excluded / 1 uncertain / 22 pending. Preparing this final exercise item does not
assign eligibility or sentiment and does not resolve the technology uncertainty.

## Batch08 human judgment; original20 exercise complete

The user answered yes / yes / yes, with the original reason:
`"investor, bon-market rout" already tell me`.
Reviewer `project-owner` decision `c5ab3dd0-76d6-45d3-adc1-d0c3795c1948`
was recorded at `2026-10-06T10:05:29.642052+00:00` for version
`a9ab679ed5d0832d8f3a918c75f52e022028d80a74e7074dec58740da137c5c3`.

Completed input: `/app/data/research/eligibility-completed-20261006-batch08.jsonl`.
Frozen selection: `/app/data/research/eligibility-batch08-reviewed-20261006`,
cutoff `2026-10-06T10:05:29.669185Z`. The full41 have **18 included / 1 excluded /
1 uncertain / 21 pending**. All original20 have a first human review: 18 included,
1 excluded, 1 uncertain. The uncertain technology item still needs adjudication.

Live checks verified exactly one new decision, preservation of prior decisions,
and unchanged counts at the earlier preparation cutoff. The saved CLI selection
returned the same current counts. These are assisted eligibility judgments, not
sentiment labels, independent events, or completion of the larger annotation pilot.

Next learning step: `sentiment-annotation-dev-v1-draft.md`. Its proposed target
and mixed/entity-dependent rules need human review before sentiment labeling.
No human sentiment label was inferred from this eligibility answer.
