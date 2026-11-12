# Duplicate-aware evaluation of a frozen financial-headline sentiment model

**Status: research prototype with a pilot result. Not a finished study and not a paper.**

## The question

News wires and aggregators republish the same story many times. If a sentiment model is
scored on every copy, one heavily syndicated story can dominate the score. This project asks
how much the reported accuracy of a **frozen** model (`ProsusAI/finbert`, pinned by revision)
changes when duplicates are counted three different ways:

| Condition | What counts |
| --- | --- |
| A | every headline version counts 1 |
| B | the versions of one story share a total weight of 1 |
| C | only the story's first available headline counts 1 |

Headlines are collected prospectively and each version gets an availability time sampled after
its content is stored, never backdated. The full contract is in
[docs/research-counting-draft-v1.md](docs/research-counting-draft-v1.md).

## How it works

```
 RSS / GDELT feeds -> Kafka (Redpanda) -> DuckDB lakehouse -> frozen, hashed snapshot
                                                                      |
   you review (append-only ledgers):  eligibility -> sentiment labels -> story groups
                                                                      |
   frozen FinBERT, offline, label-blind -> stored predictions -> scoring A / B / C -> table
```

Every human judgment (which headlines are eligible, their sentiment, which are the same story)
is recorded once in an append-only ledger and hashed. Predictions are made without opening the
labels. Scoring refuses any input whose hash does not link to the same snapshot.

## See the idea in 10 seconds (no Docker, no model, invented data)

```powershell
python -m scripts.demo_weighting
```

It scores three invented stories under A, B and C. One story is syndicated four times, so the
three conditions give different accuracies (0.667, 0.583, 0.333). The headlines are made up.

## Pilot result (do not quote as a finding about FinBERT)

On 26 development labels from one annotator, 34 eligible headlines, every story a single headline:

| Labels | Scored | Accuracy | Macro-F1 | 95% bootstrap interval |
| --- | ---: | ---: | ---: | --- |
| All released labels | 26 | 0.769 | 0.752 | 0.562 to 0.909 |
| Unassisted labels only | 11 | 0.636 | 0.600 | 0.278 to 0.895 |

Because no story in this cohort has several headlines, A, B and C are identical by construction.
**The table shows that the pipeline runs end to end; it says nothing yet about duplicates or the
model.** Full tables: [docs/pilot-score-20261111.md](docs/pilot-score-20261111.md).

Regenerate it from the saved inputs (needs the research data volume; the command rebuilds the
table twice and checks both against the recorded hashes):

```powershell
docker compose --profile tools build research-tools
docker compose --profile tools run --rm --no-deps research-tools -m scripts.reproduce_pilot --inputs /app/docs/pilot-inputs-20261111.json
```

## Run the tests

```powershell
python -m unittest tests.test_scoring tests.test_score_pipeline tests.test_stories tests.test_predict_frozen tests.test_freeze_model tests.test_reproduce_pilot tests.test_demo_weighting
```

The full suite is slow (embedding tests) and runs in CI on Linux and Windows. Redis and Kafka tests
skip when those services are not available.

## Limits, stated plainly

- **Tiny sample.** 26 labels, one annotator who also grouped the stories. No agreement measure.
- **Labels were released before predictions.** The prediction tool never opens labels (tested),
  but the order was not enforced by time as it must be in a final study.
- **Collection has gaps.** It ran on one PC with outages. A broker retention gap lost 7,931
  unread messages on 2026-10-26 ([record](docs/retention-gap-20261104.json)), so the data cannot
  support completeness claims. A move to a server is planned
  ([docs/cloud-collection-server.md](docs/cloud-collection-server.md)).
- **No multi-headline stories yet**, so the duplicate question itself is not answered.
- **Metric conventions are development choices**, to be frozen before any final test.
- Live deduplication in the pipeline has an open Redis race; the research path does not use it.

## Where things are

| | |
| --- | --- |
| [ROADMAP.md](ROADMAP.md) | plan, gates and honest progress scores |
| [docs/research-protocol.md](docs/research-protocol.md) | temporal contract and runbook for every step |
| [docs/infrastructure.md](docs/infrastructure.md) | the ingestion pipeline (Redpanda, Redis, workers, DuckDB writer) and its dedup tiers |
| `research/`, `scripts/` | scoring, eligibility, annotation, story grouping, frozen-model code |
| `docs/*.json` | hashed evidence files (kept byte-exact; see `.gitattributes`) |

Model weights are not stored here: `docs/model-freeze-finbert-20261104.json` pins the revision and
file hashes. FinBERT is licensed CC-BY-NC-SA-4.0.
