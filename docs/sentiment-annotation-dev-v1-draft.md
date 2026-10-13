# Development sentiment annotation guide — draft v1

Status: proposed for human review, not approved or frozen. Created October 6,
2026. No sentiment labels have been assigned under this guide. Eligibility uses
the separate, unchanged `headline-eligibility-dev-v1.md` policy.

## What we are labeling

Proposed target: the favorable, unfavorable or non-directional financial meaning
expressed by the exact headline about its principal financial subject. This is
a text interpretation task, not a prediction of returns or a measurement of the
whole economy's health. Confirm this target before recording pilot labels.

Identify the subject first: a named company/security, a market, households,
public finances or an economic indicator. Use the subject and direction actually
expressed in the headline. Do not silently switch from existing asset holders to
prospective buyers, or from borrowers to lenders, to obtain a preferred label.
If two perspectives compete and the headline supplies no clear principal
perspective, flag it for review rather than impose one.

## Label definitions

| Label | Rule | Invented teaching example |
| --- | --- | --- |
| Positive | A clearly favorable financial development or outlook for the identified subject | Company A reports rising profit and raises its outlook |
| Negative | A clearly unfavorable financial development or outlook for that subject | Company A reports falling profit and cuts its outlook |
| Neutral | The headline states financial information without a clear favorable or unfavorable direction | Company A will publish its results on Friday |

These examples are teaching material, not records in our dataset. Words such as
stock, investor, dollars or inflation establish topic context; they do not alone
determine polarity. A factual headline can still express positive or negative
financial meaning. Neutral does not mean uncertain, difficult, false or irrelevant.

## Difficult cases

- Mixed: record both directions and their subjects. Assign a three-class label
  only when the headline establishes a clear dominant financial message; otherwise
  leave the label unresolved pending human adjudication. Do not automatically
  map mixed sentiment to neutral.
- Entity-dependent: identify each entity and explain the competing implications.
  Do not assume all parties benefit or lose together.
- Macroeconomic policy/indicators: rate hikes, inflation changes, tariffs and
  employment numbers do not have a universal polarity. Use explicit evaluation,
  comparison or consequences in the headline. Flag unresolved perspectives.
- Forecasts, questions and quotations: label the expressed outlook when clear;
  retain a flag that it is a forecast, question or attributed opinion. Do not
  verify the claim by using later outcomes.
- Uncertain interpretation: leave the sentiment field empty and record the
  reason. This is an annotation status, not a fourth model class or neutral label.

Retain every difficult item in the review inventory. Unresolved labels cannot
enter scored three-class results yet; report their count and characteristics.
Before final testing, freeze adjudication and reporting rules so difficult cases
cannot be removed selectively after observing model errors.

## What the annotator may see

Read the exact stored title and its minimal source/version identifier. Do not
use article bodies, later news, realized market prices, FinBERT probabilities or
another annotator's decision to construct an independent headline-only judgment.
Vocabulary help may explain a word without supplying its label; record such help.
Today's assisted learning exercise is not blinded independent annotation.

## Proposed pilot record and timing

Record version ID, content hash, guide ID and guide hash, human annotator ID,
financial subject/perspective, positive/negative/neutral or unresolved status,
reason, and mixed/entity-dependent/forecast flags. Bind each decision to its exact
input; a changed headline requires its own review. Append revisions referencing
the previous decision rather than overwriting it.

The release time of a persisted human judgment must be recorded in UTC, separately
from publication, observation and DuckDB data availability. Never invent historical
labels or reuse a source publication timestamp as label availability. A reporting
cutoff must exclude judgments that were not yet released, including later revisions.

This richer record and its durable release mechanism are requirements for the
next sentiment-ledger implementation, not existing functionality. The legacy
`SentimentLabel` schema supports three classes and basic provenance, but does not
implement this review/adjudication ledger. The separate eligibility ledger is
not a sentiment ledger. Verify durable annotation availability before final freeze.

## Pilot sequence and model comparison

1. Human confirms or revises the proposed target and difficult-case rules.
2. Select exact versions from the saved included development selection; keep all
   current material outside the final test. Start with a small practice batch.
3. Record human judgments and disagreements; revise the draft using development
   evidence, preserving earlier guide versions and judgments.
4. Obtain independent human review of a subset before protocol freeze. Report
   single-annotator and assistance limits rather than claim agreement now.
5. Freeze guide, selection, adjudication rules and model configuration before
   final testing. Evaluate immutable model predictions against released labels.

The [ProsusAI FinBERT model card](https://huggingface.co/ProsusAI/finbert) describes
financial text classification into positive, negative and neutral, with Financial
PhraseBank fine-tuning. This does not establish that our broader headline target
matches its original annotation task. Document that possible task mismatch,
especially for macroeconomic and household news. The checkpoint revision, label
mapping and training-overlap assessment remain to be verified; no checkpoint is
frozen by this guide.
