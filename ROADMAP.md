# Paper 1: frozen financial sentiment under duplicate-aware evaluation

## Research objective

Build a reproducible evaluation of a fixed financial-headline sentiment model
on news captured during documented collection sessions. The study asks how
duplicate handling and story weighting change reported sentiment performance.
It does not update model weights during the evaluation period.

Primary question: **How sensitive is a frozen model's measured performance to
article duplication, story grouping, and delayed label availability?**

The three required reporting conditions are:

1. equal weight for every eligible article version;
2. equal total weight for every reviewed story group; and
3. the first observed eligible headline from every reviewed story group.

These conditions must share frozen predictions and documented cohort rules.
Changing label availability may change reporting membership, but it must never
change an already frozen prediction.

## 1. Establish the research database

- Use DuckDB at `/app/data/lakehouse.duckdb` in Docker volume `ingestion-data`
  as the sole authoritative collection database.
- Keep `data/lakehouse.duckdb` on the host as legacy development material.
- Inspect an existing Docker database read-only before starting a writer.
- Allow schema migration only after the pre-migration state is recorded. The
  migration creates a timestamped backup when an existing database has tables.
- Run audits and snapshot exports through the `research-tools` container so
  they mount the same named volume as the writer.
- Record the database identity, schema version, code revision, tree state,
  cutoff, configuration, counts, and dataset hash in research evidence.

Gate: the collector configuration, writer, readiness audit, and exporter all
identify `docker-volume:ingestion-data:/app/data/lakehouse.duckdb`. Its initial
state is recorded and a repeatable quiet snapshot procedure is documented.

## 2. Complete provenance and session evidence

- Store feed identity or GDELT query configuration, domain/publisher, collector
  version, request outcome, item count, and errors.
- Record collection-session start/end and distinguish a successful empty result
  from a failed request.
- Preserve source, observation, and durable-availability timestamps separately.
- Retain duplicate observations and corrected versions before study filtering.
- Detect interrupted sessions and record planned shutdowns.

Gate: one headline can be traced from source attempt through delivery, immutable
content version, durable availability, and exported research row.

## 3. Verify scheduled collection

- Run RSS, GDELT, and one lakehouse writer for three provisional sessions of
  about two hours each.
- Record actual observation windows, gaps, source coverage, outbox backlog,
  broker lag, rejected records, and unsealed observations.
- Stop collectors first, drain delivery and writing, then stop the writer.
- Demonstrate restart/replay without multiplying logical content versions.

Gate: real RSS and GDELT headlines reach Bronze and sealed research storage,
backlogs recover, and snapshot counts reconcile with the database. This is an
operational reliability check rather than a research sample-size claim.

## 4. Produce the development pilot

- Version eligibility decisions for language, financial relevance, suitability,
  and exclusion reason while keeping excluded items archived.
- Exclude SEC metadata and synthetic GKG titles from headline sentiment.
- Export a hashed development snapshot and deterministically select 100 eligible
  headlines. Keep the pilot outside final testing.
- Write the sentiment guide before labeling and append revisions to a dated
  annotation ledger bound to exact content hashes.
- Review a separate duplicate sample containing rewrites, hard negatives,
  corrections, cross-session matches, and independently searched missed pairs.

Gate: every pilot row has provenance, an eligibility decision, an immutable
version, and a reproducible selection record.

## 5. Freeze and evaluate Paper 1

- Use pilot evidence to choose collection duration, source panel, annotation
  workload, and sample size based on desired uncertainty.
- Freeze eligibility rules, story-grouping rules, dates, model artifact and
  label mapping, baseline-training provenance, cohort definitions, and metrics.
- Create immutable predictions before final label-aware reporting.
- Report macro-F1 and per-class measures under all three weighting conditions,
  with uncertainty based on independent story groups or time blocks.
- Report actual collection and annotation delays separately from simulated
  label-availability scenarios.

Gate: the dataset, annotations, predictions, grouping decisions, and manifest
reproduce every reported table. Missing coverage and exclusions remain visible.

## Later work

Continual adaptation, replay, sequential fine-tuning, trading strategies,
event-study returns, additional neural models, and semantic grouping are later
extensions. Existing planning code for adaptation may remain in the repository,
but it is not part of Paper 1's acceptance criteria.
