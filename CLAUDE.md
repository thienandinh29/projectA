# Paper 1 research pipeline

Financial-news ingestion (RSS, GDELT) -> Kafka/Redpanda -> DuckDB lakehouse, used to
evaluate a frozen headline sentiment model under duplicate-aware weighting.
Plan and gates: ROADMAP.md. Daily checklist: TODAY.md.

## Research integrity rules (do not break)

- Evidence is append-only: never update/delete eligibility, annotation, session or
  delivery records; revisions append and reference what they supersede.
- Never backdate. Availability timestamps are sampled after content commits.
- Frozen, hashed files must keep exact bytes: docs/headline-eligibility-dev-v1.md,
  docs/sentiment-annotation-dev-v1*.md, docs/*.json (see .gitattributes).
- Do not start, stop or restart collectors/writer (docker compose) or touch the
  `ingestion-data` volume without the user's explicit go-ahead: it changes the
  prospective research record.
- Do not create labels, eligibility decisions, predictions or scores on the user's
  behalf. Tooling may be changed; research judgments are the user's.
- Legacy behaviour stays reproducible: saved selections/snapshots must still verify
  after code changes.

## Environment (Windows 11)

- Shells: Git Bash and PowerShell 5.1. Python 3.11 at the system level (no venv).
- `gh` CLI is not installed; use git + the public GitHub API (repo is public:
  thienandinh29/projectA). CI logs require sign-in, so ask the user to paste them.
- Docker Desktop is often stopped. WSL Ubuntu exists but has no python3-venv.
- Local test runs are slow (~35 min for the full suite; embedding tests dominate).
  Prefer focused runs, e.g.
  `python -m unittest tests.test_eligibility tests.test_annotations tests.test_collection_sessions`.
- Redis/Kafka-backed tests skip without those services; live tests are opt-in via
  RUN_RESEARCH_KAFKA_TEST=1 / RUN_LIVE_STORAGE_TESTS=1.
- CI (.github/workflows/tests.yml) runs the full suite on Linux (with Redis) and
  Windows on every push.

## Conventions

- Match surrounding style: compact Python, short docstrings stating guarantees and
  limits, tests named for the behaviour they protect.
- Every behaviour change gets a regression test that fails on the old code.
- `changes.txt` is a generated patch artifact; don't hand-edit it.
