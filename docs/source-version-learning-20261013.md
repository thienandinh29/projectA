# Source-item versions versus headline inputs — October 13

Read-only inspection at 00:31 UTC / 07:31 Bangkok verified three captured versions
of one article identity. Each has the identical title:

> Anthropic tells Australia it’s open to laws requiring reporting of AI agent hacks

| Version | Parsed publisher-reported time (UTC, October 6) | Stored availability (UTC, October 6) |
| --- | --- | --- |
| 1 | 03:30:45 | 03:54:51 |
| 2 | 04:18:34 | 04:33:05 |
| 3 | 05:30:41 | 05:51:18 |

The version IDs differ. The exact UTF-8 headline digest is identical across all
three. The version-bearing fields that changed are publisher-reported time,
its raw string and parsed components. Captured title and summary did not change.
The source-item identity intentionally hashes source metadata as well as text;
it is a different concept from the headline input seen by a text classifier.
The headline digest here is an inspection aid, not a changed production field.

This demonstrates three stored source-item versions and one distinct captured
headline text. It does not establish three new stories, independent examples,
or a full-article correction: full publisher article bodies were not captured.
Raw timestamps are preserved with parser provenance; their publication/update
meaning and original timezone are not independently verified by this inspection.

All three are pending financial-eligibility review, outside the currently included
18-version sentiment exercise. No eligibility decision or label was generated or
transferred. They are used to learn versioning, not as validated financial
syndication positives. The article ID establishes shared URL identity, not an
independently adjudicated event label or assessment of page contents.

Preserve every source-item version and its true availability for the audit. Later
metadata must not replace earlier evidence. Define the study's article/headline/
story counting rules separately; do not treat every metadata revision as a new
independent text. Model labels still require exact version/guide binding under
the current workflow. Any reuse policy would need its own defined timing rules.

Learning question: for headline-only FinBERT, are these three different text
inputs, or one text stored in three source-item versions?

Discussion update, October 13: the human answered, "I think when FinBERT receive
the headline, it just realize one because the headline and the timestamp dont
come in same". Assistant clarification: this is one distinct headline text stored
in three versions; timestamps are not text inputs in the planned headline-only
setup. FinBERT does not merge records. Sending all three records can still invoke
the model three times. The pipeline and evaluation protocol determine selection
and counting. This completes the assisted explanation, not independent testing
of understanding or a human same-event judgment.

The original inspection JSON remains unchanged, including its blank human-review
field. This discussion adds no eligibility, sentiment or event-identity label.
Do not infer the paper's final counting or label-reuse rule from this question.

Evidence: `source-version-learning-case-20261013.json`; full Docker artifact:
`/app/data/research/source-version-learning-case-20261013.json`.
Snapshot, manifest, annotation ledger and eligibility ledger hashes were preserved.
The current included cohort was rechecked: 14 labeled / 4 unresolved, no pending
first reviews. No grouping, identity rules, source collection, model inference,
scores, or labels changed. Collectors/writer remained stopped after Docker startup.
