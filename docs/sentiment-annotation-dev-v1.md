# Financial-headline sentiment — development guide v1

October 7, 2026. Working rules for the assisted development pilot, not a frozen
final-test protocol. The earlier draft is preserved separately. No practice
answer has been imported as a formal label.

## Target and allowed information

Label the favorable, unfavorable or non-directional financial meaning explicitly
expressed by the exact headline about its principal financial subject. Identify
that subject and perspective before choosing a label. This is interpretation of
the headline, not a prediction of returns or a measure of economy-wide health.

Use the stored headline and minimal source/version identity. Do not use article
bodies, later outcomes, market-price histories, model predictions or another
annotator's answers for an independent judgment. Disclose vocabulary explanations,
discussion or other assistance; assisted practice is not blinded annotation.

## Labels and unresolved cases

| Judgment | Meaning |
| --- | --- |
| Positive | A clearly favorable financial development/outlook for the identified subject |
| Negative | A clearly unfavorable financial development/outlook for that subject |
| Neutral | Financial information without a clear favorable/unfavorable direction |
| Unresolved | A label cannot yet be justified; sentiment remains null pending human review |

Unresolved is a review status, not a fourth model class. Mixed does not automatically
mean neutral. Words such as stock or investor alone do not determine polarity.
Direction must be linked to its subject: falling stock prices and falling company
costs can convey different implications. A factual statement can carry sentiment.

For mixed or entity-dependent headlines, record competing directions and subjects.
Choose a class only if the headline establishes a clear dominant financial message.
Otherwise keep it unresolved. Do not silently switch from existing holders to
buyers, borrowers to lenders, or companies to households to force a class.
Rates, tariffs, inflation and employment numbers have no universal polarity:
use explicit evaluations/comparisons/consequences, or flag unresolved perspective.
For questions, forecasts and attributed opinions, label the expressed outlook
when clear and retain the applicable flags. Do not verify claims using later news.

## Required human record

Each record contains exact version ID/content hash, subject/perspective, labeled
or unresolved status, sentiment (null if unresolved), reason, mixed/entity-dependent/
forecast/quotation/question flags, assistance disclosure, and a revision reference
when correcting a prior judgment. The ledger adds annotator identity, snapshot,
eligibility-selection and guide hashes, recording time, and availability evidence.
Changed versions and guide revisions require their own judgments.

A correction must reference the same annotator's latest decision for that version
and guide. Different annotators retain separate histories; the tool does not
automatically adjudicate them. No record is overwritten or deleted.

## Availability and reporting

Judgment contents commit first; a post-content-commit UTC clock is then sampled
and saved in a separate immutable availability marker. This is a conservative
confirmation of persisted judgment contents, not a publisher timestamp or an
estimate of when the human first formed the opinion. The clock assumes a valid
host UTC clock, not independently verified synchronization. An unsealed judgment
is excluded. Recovery samples its present clock and retains the original content.

Data and sealed judgments must both precede a label-aware cutoff strictly. A later
label cannot become earlier training information. It may score an already frozen
earlier prediction retrospectively when that distinction and the real timing are
reported. Scoring code/prediction logs remain separate work.

Keep difficult examples archived, report unresolved counts, and do not selectively
discard model errors. Freeze adjudication and reporting rules before final testing.
Current practice material is outside the held-out final test. Obtain independent
human review of a subset and disclose assistance/single-annotator limitations.

## Model-task boundary

The [ProsusAI FinBERT card](https://huggingface.co/ProsusAI/finbert) describes three-class
financial text sentiment with Financial PhraseBank fine-tuning. It does not establish
that our broader headline target matches that task. Document possible task mismatch
for macroeconomic/household news. Checkpoint revision, label mapping and training
overlap still require verification. This guide does not freeze a model or establish
paper readiness.
