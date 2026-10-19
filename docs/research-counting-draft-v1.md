# Paper 1 counting contract — development draft v1

October 13, 2026. CP3 delivers definitions, examples and explicit open decisions.
This is a proposed analysis contract, not a final protocol freeze or implemented
scorer. No model predictions, labels or story memberships are created here.
Changes after this version should be recorded in a new draft rather than silently
changing a contract used by a saved report.

## 1. Separate the counting units

| Unit | Meaning in this project | What it does not establish |
| --- | --- | --- |
| Kafka delivery | Topic/partition/offset; a new-offset redelivery is new Bronze delivery evidence | A new text or independent news event |
| Source-item version | One immutable `version_id`/`content_hash` over captured text and source material, including reported timestamps | An independent article or a new headline string |
| Article identity | Current `article_id`: hash of supplied URL, falling back to title when URL is absent | A canonical publisher page or independently verified event |
| Distinct headline text | Exact captured title string/UTF-8 digest used for inspecting text identity | The number of model calls, token sequences or news events |
| Reviewed story/event | A documented judgment about the specific underlying report, announcement or occurrence | A topic, sentiment class, URL family or automatically generated cluster |

Same Kafka offset replay must not create another Bronze row. A redelivery at a
different offset can create Bronze evidence without another logical source version.
Returning the same version across polls does not add independent texts; request
counts and Bronze deliveries therefore are not classification denominators.

RSS/GDELT currently hash the supplied URL as-is, with title fallback. URL aliases,
tracking parameters, redirects and reused live pages remain limitations; URL
identity is not canonicalization. The current 41-version snapshot has no missing
URLs, but future missing-URL identities must be flagged in reports.

Distinct exact strings can tokenize identically under a particular tokenizer.
Conversely, case/whitespace variants are distinct captured strings even when the
reference matcher normalizes them. Preserve raw captured titles and record any
future model-input transformation separately. FinBERT performs classification;
the pipeline and reporting rules determine counting and grouping.

## 2. Preserve the intended primary baseline

The roadmap's primary baseline is explicitly **one equal weight per eligible
source-item version**, including metadata-only versions. This resolves its earlier
ambiguous article/article-version wording without silently changing the denominator.
Call it a version-weighted baseline, not an independent-event sample or one row
per distinct article. Timestamp-only repeats are intentionally visible in this
baseline so their contribution can be measured and disclosed.

Retain every raw version and its own availability. Identical text does not
automatically transfer eligibility, sentiment labels or predictions to another
version. Any future inference cache must bind the exact model revision, tokenizer,
preprocessing, text and inference configuration, while retaining each version's
own input availability and prediction record. A later cached prediction cannot be
backdated as an earlier executed call. No cache/reuse policy is implemented by CP3.

An additional diagnostic may collapse metadata-only repeats within an article
before story weighting. Its inclusion and rules must be frozen before final testing;
it does not replace the three primary conditions below. If titles change A → B → A,
global unique-string counting would hide a reversion. Any such diagnostic must
preserve title-change episodes and specify its handling of context-only changes.

## 3. Define the shared cohort before comparing weights

Freeze the data window using research `available_at`, not publication time. Use a
named snapshot, its hash, and a specified eligibility/report cutoff. Eligibility
must use v2 releases unless the entire result is explicitly legacy development
replay. Actual late confirmations cannot supply historical availability.

For the paired primary comparison, use the same eligible versions whose story
membership has been reviewed under the declared grouping snapshot. This membership
cohort is selected without model predictions or sentiment polarity. Unknown or
uncertain event relations stay visible; do not silently assign them independent
singleton IDs. Report the excluded/uncertain grouping counts and selection bias.
Reviewed singleton groups describe the review procedure, not proof that every
possible related article was found. Missed-pair review remains required.

Also show coverage for the full eligible stream, including unreviewed-group
versions, as a separate diagnostic. Do not present a full-stream baseline and a
restricted grouped result as if they use the same population.

Each version requires its own frozen prediction and released three-class label to
be scored. Unresolved is not neutral. Missing predictions/labels, unresolved labels,
exclusions and group-review coverage must be counted separately by source/period.
Later labels may score earlier frozen predictions; they cannot become earlier
training information. Freeze actual-versus-simulated label-delay rules separately.

## 4. Three primary reporting conditions

Let `V` be the shared eligible, reviewed-membership version cohort. Let `S` be its
scorable subset at the declared report cutoff. These symbols do not assert that
today's data already has all required reviews or any predictions.

| Condition | Weight and selection | Interpretation |
| --- | --- | --- |
| A: version-weighted | Every version in `S` has weight 1 | Performance over stored eligible versions, including repeated texts |
| B: story-weighted | For story `g` with `m_g` scorable members in `S`, each receives `1/m_g` | Each scored story has equal total weight; versions keep their own labels |
| C: first available eligible headline | Anchor each story on the first member of `V` by `(available_at, observed_at, version_id)`; weight 1 only if that anchor is scorable | First durable headline coverage per story, with anchor missingness reported |

For a paired A/B/C result, restrict all three conditions to the same stories whose
preselected anchor is scorable. Apply the table's A/B weights to `S` within those
stories. This prevents missing first-headline labels from changing story coverage
only in C and being mistaken for a duplicate-control effect. Show unrestricted
A/B results and full-cohort anchor coverage separately. When varying simulated
label delays, keep the eligibility/group snapshot and preselected anchors fixed;
disclose how the shared scorable story subset changes at each reporting cutoff.

Condition C means first **durably available**, not earliest publisher timestamp,
nor first label-ready member. It clarifies the roadmap's former first-observed
wording and matches the availability-first reference ordering. Do not substitute
a later labeled/clearer headline when the fixed anchor lacks a label. Report that
story's missing/unresolved anchor and scored coverage; keep the other versions
available in conditions A/B. Group/cohort revisions create new report versions,
not replacements of earlier predictions or published selections.

For B, label delays/missingness can change `m_g` and hence within-story weights.
The total is one per scored story, not a claim of complete labels for that story.
Report member coverage. Equal story weighting also does not remove repeated-text
weight inside a story: three metadata repeats plus one update give the repeated
text 3/4 of that story's weight. The optional metadata diagnostic above addresses
that distinct question if selected before final testing.

Compute per-class measures/macro-F1 from each condition's weighted confusion
matrix, rather than averaging individual-example or story F1 scores. Fix absent
class/zero-denominator treatment and uncertainty resampling before final testing.
Reviewed groups/time blocks are uncertainty units; raw version counts do not
establish statistical independence or a power/sample-size result.

Illustration only, not observed or scored research data: if three scorable versions
`v1,v2,v3` belong to one reviewed story and arrive in that order, A weights are
`1,1,1`, B weights are `1/3,1/3,1/3`, and C weights are `1,0,0`. This illustrates
counting, not sentiment predictions or a measured improvement.

## 5. Timing and hindsight controls

Predictions bind exact versions, model/tokenizer/preprocessing revisions, probabilities
and actual inference time. A source version must be durably available before its
prediction cutoff. Model and any fitting inputs/labels must also meet their own
availability cutoffs. Model provenance/contamination and the prediction logger
remain future work; current data capture does not prove historical executability.

For report cutoff `R`, eligibility releases, labels and grouping judgments used
by the report must be available strictly before `R`. Keep captured-data time,
actual review/run time and simulated replay time separate. First-seen selection
uses available data in the fixed window and the stated reviewed cohort, without
consulting which anchor gets a convenient sentiment label.

Later human grouping is allowed as **retrospective reporting** of already frozen
predictions, with its real decision time and membership hash. It cannot justify
a claim that those groups guided historical training, collection or first-only
inference. A prospective grouping decision may use only evidence available at
its decision cutoff. Group corrections append new decisions; never backdate a
human judgment to an article's `available_at`.

Keep labels independent of later outcomes/model predictions. Grouping reviewers
should be blind to model errors and sentiment labels and use only the declared
captured context. A summary used for event identity must not silently become
additional information for a headline-only sentiment label.

## 6. What the current grouping code actually does

`research/protocol.py::assign_clusters` is a deterministic forward reference:

- It orders source versions by availability, observation and version ID.
- Article matching groups `article_id` families; that lookup does not expire.
- Exact matching lowercases and collapses whitespace, so it differs from exact
  UTF-8 string identity. Lexical matching uses production shingles/exact Jaccard
  plus a polarity guard, not semantic understanding or FinBERT sentiment.
- The 24-hour active window limits cross-article comparisons. Revisions sharing
  an article identity stay in their family even beyond that window.
- `cluster_decided_at` is assigned the replayed data availability time. It is not
  evidence of the actual algorithm execution time or a human review time.

These roots are `reference_cluster_id` concepts, not independently reviewed
`story_id` ground truth. Polarity words and URL reuse can split true related
articles or merge distinct events. The future grouping/report schema must keep
reference arrival, actual run time and human-review availability distinct.
The old four-arm window planner is outside Paper 1; it is not the implemented
three-condition frozen prediction/scoring harness.

Do not run reference clusters through condition B/C and name them verified stories.
Use explicitly labeled reference-control results separately until review/validation
supports the human story cohort. Tier 3's operational embedding matches are not
already validated research event memberships.

## 7. Grounded examples and current inventory

Read-only CP3 inspection at 08:34 Bangkok verified:

| Scope | Source versions | Article identities | Distinct exact titles |
| --- | ---: | ---: | ---: |
| Full frozen snapshot | 41 | 38 | 38 |
| Current eligible cohort | 18 | 18 | 18 |
| Anthropic three-version teaching case | 3 | 1 | 1 |

These are descriptive identities. **The number of independently reviewed stories
is not established.** The current eligible reference controls previously produced
18 groups, but that does not prove absence of semantic duplicates.

The Anthropic titles/summaries were unchanged while publisher timestamp fields
changed. Preserve all three versions. The case is still pending eligibility and
outside the labeled cohort, so it supplies no financial duplicate positive or
three scored examples. Its event relation has not been independently adjudicated.

The two September employment headlines describe BLS nonfarm-payrolls/unemployment
and ADP private-sector jobs reports. The saved assisted development review judged
different events; captured report identity supports the distinction. Same month
and different sentiment alone do not establish it. Those pair artifacts are
teaching evidence, not a completed production story-membership ledger or independent
gold labels. The jobs-report versus Fed-inflation announcement pair is likewise
an assisted different-event example. No real positive rewrite pair is added here.

Sources: `checkpoint3-counting-inventory-20261013.json`,
`source-version-learning-case-20261013.json`, `duplicate-pair01-review-20261012.json`,
`duplicate-pair02-review-20261012.json`, and `duplicate-inventory-20261012.json`.
Checked research files/ledgers retained identical hashes during CP3 inspection.

## 8. Decisions still needed before final freeze

- Validate real financial rewrite/update pairs, hard negatives and independently
  searched missed pairs; define a reviewed grouping ledger/adjudication procedure.
- Freeze how uncertain memberships and reused/missing URLs affect the common
  cohort, and how corrections/reports are versioned.
- Decide whether the metadata-repeat diagnostic is required and specify episode
  boundaries; do not use final-test scores to choose that rule.
- Freeze windows, label-delay cohorts, anchor missingness, per-period weighting,
  absent-class handling and event/time-block resampling. Do not casually renormalize
  story weights by period versus across the full stream; they answer different questions.
- Implement and test the frozen prediction log and weighted scoring harness using
  these definitions. Verify prospective versus retrospective model provenance.

CP3 is complete as a **documented development draft with checked examples**.
Implementation, human approval/final protocol freeze and research claims remain
future milestones. CP4 next reviews 6–9 pending eligibility versions.
