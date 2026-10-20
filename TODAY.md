# Work session — Wednesday, October 14, 2026

Goal: **get collection running continuously and finish reviewing the frozen
41-version snapshot.** Roadmap stage: Phase 0 (Unblock) in [ROADMAP.md](ROADMAP.md).
Previous checklist: [docs/archive/TODAY-20261013.md](docs/archive/TODAY-20261013.md).

**Run on Monday, October 19, 2026** (the Oct 14 plan, unchanged in scope). Collection
started 2026-10-19 09:16:25 UTC / 16:16:25 Bangkok on clean commit `a0b3d88`.
**Outage:** Docker stopped after the last attempt at 2026-10-19 10:07 UTC; collection resumed
2026-10-20 06:33 UTC (~20.4 h gap, unrecoverable). Ledger marks the old sessions interrupted.

Time blocks are elapsed session time (~5 h), not appointments. Check a box only
after its output exists. Today's rules (ROADMAP Part B): one closure note at
the end, no hand-written per-batch JSON, and review batches of up to 25.

| Elapsed | Checkpoint | Done when |
| --- | --- | --- |
| 00:00–00:15 | CP0 Quick start | Stack state known, nothing running that shouldn't be |
| 00:15–01:30 | CP1 Fix sources + pin deps | Tests pass, fixes committed on a clean tree |
| 01:30–02:00 | CP2 Start continuous collection | Writer + RSS + GDELT up; first cycle logged OK |
| 02:00–02:15 | Break | — |
| 02:15–03:15 | CP3 Finish eligibility on the snapshot | 0 pending of 41 |
| 03:15–04:00 | CP4 Sentiment labels for new includes | Every included version labeled or unresolved |
| 04:00–04:15 | Break | — |
| 04:15–04:45 | CP5 Collection health check | First health numbers saved |
| 04:45–05:00 | CP6 Close | Closure note + roadmap metrics updated |

---

## CP0 — Quick start (15 min)

```powershell
git status --short
docker compose ps -a
docker compose --profile tools run --rm --no-deps research-tools -m scripts.inspect_collection_sessions
```

- [x] Broker and Redis healthy; collectors and writer stopped (last ran Oct 6, not Oct 13).
- [x] No unexpected session containers (`paper1-*`) still running (all exited Oct 6).

No new baseline document is needed: yesterday's CP0 is still the reference.

## CP1 — Fix the source panel and pin dependencies (75 min)

Do all code changes **before** CP2, so collection does not need a restart later.

**GDELT DOC (`workers/gdelt_worker.py`, lines ~59–91).** Today the code retries
3× inside one cycle, sleeping 6 s after a 429. That is a burst against a
rate-limited API.
- [x] On HTTP 429: record the failed request with its response body (first
      500 chars), **do not retry in this cycle**, and wait for the next cycle.
- [x] Keep retries only for 5xx/timeouts, with ≥ 10 s spacing.
- [x] Add a regression test: a 429 produces exactly one recorded attempt (`3b9d62d`; it also
      exposed and fixed random tie ordering of ledger requests/sessions).

**RSS panel (`workers/rss_worker.py`, `MACRO_FEEDS`).**
- [x] Yahoo Finance returned 404 (2/18 on Oct 6). Check the URL in a browser;
      replace or remove it. Removed: both Yahoo RSS URLs return 404.
- [x] Investing.com: 11/18 timeouts. Try a 30 s timeout for this feed only. Not applied:
      the server drops TLS at exactly 15 s even with a 30 s timeout. Kept; failures recorded.
- [x] Add 2–3 public financial RSS feeds from **different publishers** (needed
      for cross-publisher duplicates). Check each before adding:

  ```powershell
  python -c "import feedparser,sys,time; f=feedparser.parse(sys.argv[1]); print(f.get('status'), len(f.entries)); [print(time.strftime('%Y-%m-%d %H:%M', e.published_parsed) if e.get('published_parsed') else '-', e.title[:80]) for e in f.entries[:5]]" <FEED_URL>
  ```

  Keep a feed only if it returns HTTP 200, English headlines, and items under 24 h old.
  Added Guardian Business, BBC Business, NYT Business, Seeking Alpha Market Currents (`341ab86`).
- [x] Record the panel change in one line in `docs/research-protocol.md`
      ("panel v2 from 2026-10-14: …"). This counts as a new configuration.

**Investigate quickly (15 min max, findings in the closure note).** In yesterday's
CP4 set, 3 of 9 Investing.com items have a URL slug that doesn't match the title:

| # | Title | URL slug |
| --- | --- | --- |
| 1 | Anthropic tells Australia it's open to laws requiring reporting of AI agent hacks | australias-abc-rejects-ai-copyright-carveout… |
| 6 | Nasdaq notches record high close as investors focus on earnings | wall-st-futures-dip-as-tech-stocks-take-a-breather |
| 7 | Nasdaq hits record as dollar and Treasury yields climb, oil prices ease | stocks-upbeat-dollar-wobbles-as-fed-hike-bets-recede |

- [x] Open 2 of these URLs. Does Investing.com rewrite headlines under the same
      URL (live-updated stories), or is the feed mixing up items? If it's rewrites,
      **these are real same-article headline changes**, the positives the dedup
      work hasn't had yet. Note it for grouping; don't change identity rules today.
      Finding (Oct 20): pages unreachable from this network (TLS dropped at 15 s, same as the
      feed). From saved feed entries: #6/#7 look like live-updated Reuters market reports: oldest
      article numbers, published after the US close, slug = the earlier pre-market headline.
      #1 (Anthropic/ABC) is ambiguous: unrelated slug, re-published 3x with new times. Summaries
      are empty and no pre-update version was captured: indirect evidence, not a recorded change.

**Pin dependencies.**
- [x] Build the image once, then freeze it (`requirements.lock`, 42 packages, `a0b3d88`):
  ```powershell
  docker compose --profile tools build research-tools
  docker compose --profile tools run --rm --no-deps --entrypoint pip research-tools freeze > requirements.lock
  ```
- [x] Change the Dockerfile to `pip install -r requirements.lock`. 124 tests pass in the pinned image.

**Verify and commit.**
- [x] Run the collection regression set from `docs/research-protocol.md` (the
      `python -m unittest tests.test_collection_sessions … -q` line), plus the new GDELT test.
- [x] `git commit` so the worktree is **clean** before CP2 (main = `a0b3d88`).

## CP2 — Start continuous collection (30 min)

```powershell
$env:COLLECTION_CODE_REVISION = git rev-parse HEAD
$env:COLLECTION_WORKTREE_STATE = if (git status --porcelain) { 'dirty' } else { 'clean' }
docker compose --profile workers --profile tools build lakehouse-writer worker-rss worker-gdelt research-tools
docker compose --profile workers up -d lakehouse-writer worker-rss worker-gdelt
docker compose --profile workers logs --tail 50 lakehouse-writer worker-rss worker-gdelt
```

- [x] `COLLECTION_WORKTREE_STATE` is `clean`.
- [x] Start the regular services, **not** bounded `paper1-*` session containers.
      They use `restart: unless-stopped`, so they survive a Docker restart. Leave them running.
- [x] After ~15 min: every RSS feed shows HTTP 200 in the logs, GDELT shows either
      success or a single 429 per cycle, and the writer shows `batch_committed`.
      15-min check: 6 feeds 200 every cycle, Investing.com 1/5, GDELT one failed attempt
      per cycle (a 429, then a non-JSON 200), 18 writer batches, no errors.
- [x] Write the start time (Bangkok + UTC) down for the closure note: 16:16:25 Bangkok /
      09:16:25 UTC, 2026-10-19.

Don't stop the writer for inspection today. CP3–CP4 only read the frozen snapshot
directory and the SQLite ledgers, not DuckDB.

## CP3 — Finish eligibility on the frozen snapshot (60 min)

Status: 19 included / 2 excluded / 2 uncertain / **18 pending**. Clear all 18 in
one batch (6 left from yesterday's CP4 plus 12 never prepared).

Snapshot: `/app/data/research/session01-20261006-snapshot` (dataset hash `2913b351…`;
the earlier `checkpoint2-corrected` name was wrong). Policy: `/app/docs/headline-eligibility-dev-v1.md`
(frozen v1, `438f7b10…`).

Prepared: `/app/data/research/eligibility-20261019/` holds `template.jsonl` (the 18 pending
rows only, blank) and `worksheet.txt` (line N = headline N). Skip step 1; fill the template
and save it as `reviews.jsonl` in the same folder.

```powershell
# 1. Blank template for the snapshot (the tool writes every row; fill only pending ones)
docker compose --profile tools run --rm --no-deps research-tools -m scripts.review_eligibility prepare --snapshot-dir <SNAPSHOT> --policy-file <POLICY_V1> --ledger /app/data/research/eligibility.sqlite3 --output /app/data/research/eligibility-20261014/template.jsonl
# 2. Fill english / financially_relevant / genuine_headline / reason for the 18 pending rows.
#    Remove already-decided rows. Unsure -> leave it uncertain with a reason; don't force it.
# 3. Record
docker compose --profile tools run --rm --no-deps research-tools -m scripts.review_eligibility record --snapshot-dir <SNAPSHOT> --policy-file <POLICY_V1> --ledger /app/data/research/eligibility.sqlite3 --reviews /app/data/research/eligibility-20261014/reviews.jsonl --reviewer <YOU>
# 4. New selection (cutoff = a moment after step 3 returns, in UTC)
docker compose --profile tools run --rm --no-deps research-tools -m scripts.review_eligibility select --snapshot-dir <SNAPSHOT> --policy-file <POLICY_V1> --ledger /app/data/research/eligibility.sqlite3 --cutoff <UTC_NOW> --output /app/data/research/eligibility-20261014/selection
```

Use the exact snapshot and policy paths from
`docs/checkpoint2-release-rollout-20261013.md`.

- [x] Pending = 0. Save the record command's printed JSON output as the only evidence file.
      Done Oct 20: 18 decisions by `project-owner`, released 07:23:56 UTC (`initial`);
      evidence `eligibility-20261019/record-output.json`. Selection at cutoff 07:24:02 UTC:
      **34 included / 5 excluded / 2 uncertain / 0 pending** (`eligibility-20261019/selection`).
- [x] Target pace: ~1 min per headline. Use the headline only and the policy as written.

## CP4 — Sentiment labels for the new includes (45 min)

```powershell
docker compose --profile tools run --rm --no-deps research-tools -m scripts.review_sentiment prepare --snapshot-dir <SNAPSHOT> --eligibility-selection /app/data/research/eligibility-20261014/selection --guide-file <SENTIMENT_GUIDE_V1> --ledger /app/data/research/annotations.sqlite3 --output /app/data/research/sentiment-20261014/template.json
# fill labels -> record (--reviews, --annotator <YOU>) -> select (--cutoff <UTC_NOW>, --output ...)
```

- [ ] Label every newly included version in **one** batch: positive / negative /
      neutral, or unresolved with a reason. Unresolved is not neutral.
- [ ] **Label without assistance.** Write your label before asking anyone,
      including an AI, so these labels can count as independent development data.
      Mark any label you changed after discussion as assisted.
- [ ] Revisit the 4 earlier unresolved items only if a guide rule now decides them.

## CP5 — First collection health check (30 min)

By now collection has run ~3 h.

```powershell
docker compose --profile tools run --rm --no-deps research-tools -m scripts.inspect_collection_sessions
docker compose --profile workers logs --since 3h worker-gdelt | Select-String "429|failed"
docker compose --profile workers logs --since 3h lakehouse-writer | Select-String "batch_committed" | Measure-Object
```

- [ ] Per source: attempts, successes, failures, max gap between attempts.
- [ ] Note if any gap is > 30 min. If GDELT is still nearly all 429 after the
      fix, record that; the decision to drop it comes on Friday, not today.
- [x] Optional if time is left: turn these three commands into
      `scripts/collection_health.py` (one JSON, < 30 s). It becomes the daily check.
      Done (`d6834a6`, ~9 s): `docker compose --profile tools run --rm --no-deps research-tools
      -m scripts.collection_health --hours 3`. Writer health is still the log count above.

## CP6 — Close (15 min)

- [ ] Collectors and writer **still running** (`docker compose ps`). Do not stop them.
- [ ] Write the closure note at `docs/session-20261014.md`, about 15 lines:
      commit hash, collection start time, panel v2, Investing.com finding,
      eligibility counts, sentiment counts, health numbers, open problems.
- [ ] Update the metrics table in ROADMAP.md Part B: collection hours, reviewed
      headlines, labels.
- [ ] Commit docs and code (not data).

---

## Completion boundary

The day is a success if **collection is running on a clean commit** and the
**41-version snapshot has 0 pending eligibility**. If CP1 overruns, cut the new
feeds and the Investing.com check before you cut CP2. Starting collection matters
more than a perfect panel.

## Not today (parked)

FinBERT/predictions (start Oct 20), story-grouping script (starts at ~200
eligible headlines from ≥ 4 sources), Redis race, Tier 3, SEC, README rewrite.

## Tomorrow (preview)

Let collection run. Do the first daily quiet snapshot (stop collectors → drain →
stop writer → export → restart everything), then review eligibility and sentiment
for the new headlines in batches of 25.
