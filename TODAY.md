# Work plan — CV version, then pause (from November 5, 2026)

Goal: **one reproducible pilot result and a project an outsider can understand in five
minutes, then a clean stop until after December 20.** Decided by the user on 2026-11-05.
Previous checklist: [docs/archive/TODAY-20261104.md](docs/archive/TODAY-20261104.md).
Roadmap: this finishes Phase 1 and the portfolio track in [ROADMAP.md](ROADMAP.md).

About 2–3 sessions. Check a box only after its output exists. The order matters: your
judgments come before anyone sees a prediction.

## Today — Tuesday, November 10 (about 4 h)

Start state: stack restarted 09:41 UTC; collection ran in windows since Nov 4 (RSS 393 attempts,
longest gap about 47 h). CI passed on `1ad172b`. Three grouping commits are local, not pushed.

| Time | Checkpoint | Whose | Done when |
| --- | --- | --- | --- |
| 0:00–0:10 | T0 Push the grouping tool | Your go-ahead | CI passes on the pushed commit |
| 0:10–0:40 | T1 Group the 34 headlines (CP2) | You | Answers written in the guide's format |
| 0:40–0:55 | T2 Record and select groups | Tool | Review released; `story-groups.json` saved |
| 0:55–1:20 | T3 Run predictions (CP3) | Your go-ahead | 41 versions stored; re-run inserts 0 |
| 1:20–3:30 | T4 Build `scripts/score.py` (CP4) | Tool | Toy-example tests pass |
| 3:30–4:00 | T5 Pilot table + close | Your go-ahead | Table saved, labeled pilot; commit and push |

- [ ] **T0:** push `cc5797f`..`91a6aa9`; rebuild `research-tools` from the main folder at that
      commit (it needs the new code and guide).
- [x] **T1:** read `docs/story-grouping-dev-v1.md` first; change it now if you disagree, because
      its hash is recorded with your answers. Worksheet: `/app/data/research/stories-20261105/`.
      Done 2026-11-11: all 34 headlines judged `alone` (a proposed 2/3/6 story was split after
      the guide's same-event rule was checked).
- [x] **T2:** `review_stories record` (reviewer `project-owner`), then `select` with a cutoff after
      the release. Uncertain headlines stay out of B and C and are counted.
      Done 2026-11-11: review `b0ac7cc2`, released 16:29:46 UTC, tool commit `7d68d03`, guide hash
      `b4642d16`; selection at `/app/data/research/stories-20261105/selection/story-groups.json`.
      0 multi-headline stories, 34 alone, 0 uncertain, so conditions A, B and C will coincide.
- [x] **T3:** only after T2. `--check` first, then the run, then the run again.
      Done 2026-11-11 (tool commit `b98467e`): 41 versions (38 distinct inputs) stored and
      released; re-run inserted 0, 41 identical, 0 mismatched. Summaries:
      `docs/predictions-run1-session01-20261111.json`, `docs/predictions-run2-session01-20261111.json`.
      Ledger `/app/data/research/predictions.sqlite3`. Predicted classes: 14 positive, 11 negative,
      16 neutral (no labels were opened; this is not a score).
- [x] **T4:** conditions A, B, C; weighted confusion matrices; story-group bootstrap; counts for
      unresolved, assisted, uncertain and ungrouped. Tests before any real run.
      Built 2026-11-11: `research/scoring.py` (arithmetic), `scripts/score.py` (verifies links,
      writes the report), 28 tests (hand-computed examples, refusals, byte-identical output).
      Not run on your labels yet: that is T5 and needs your go-ahead.
- [ ] **T5:** run on the 26 development labels; note the numbers describe the method, not FinBERT.

- [ ] **T6 (after T3; decided 2026-11-11): move Docker's disk to D:** C: has 17.5 GB free and
      `C:\Users\Hp\AppData\Local\Docker\wsl\disk\docker_data.vhdx` is 24 GB (D: has 173 GB free).
      The project folder is only 0.02 GB, so it stays where it is. Order, on your go-ahead:
      collectors stop, health check shows every partition `"unread": 0`, writer stops; back up
      the volumes and note their SHA-256; you set Docker Desktop, Settings, Resources, Advanced,
      Disk image location to a D: folder; after Docker restarts, check that all 6 containers and
      the `ingestion-data` volume are back and the writer shows `batch_committed`; then clear
      the build cache (about 5 GB). Delete the old `.vhdx` only if Docker has not removed it, and
      only after you've confirmed the new location works.

If the session is short: T0–T2 only. Grouping must happen before predictions exist.

| Step | Work | Whose | Done when |
| --- | --- | --- | --- |
| CP1 | Close open labels (optional) | You | Decided or left unresolved, before predictions exist |
| CP2 | Story grouping | Tool, then you | Groups for the included headlines in an append-only ledger |
| CP3 | Run predictions | Your go-ahead | 41 versions stored; printed summary saved |
| CP4 | `scripts/score.py` | Tool, your go-ahead to run | Pilot table for conditions A, B and C |
| CP5 | One-command table | Tool | Same table byte-for-byte twice from saved inputs |
| CP6 | README and demo | Tool, your review | Short landing page; demo runs without live feeds |
| CP7 | Clean stop and tag | Your go-ahead | Stack stopped in protocol order; volumes backed up; tag pushed |

## CP1 — Close open labels (optional, 15 min)

Must happen before CP3: once predictions exist, you could no longer review blind to them.

- [x] Revisit the 4 earlier unresolved labels, or leave them unresolved. Unresolved is not
      neutral and is counted separately. Decided 2026-11-05: left unresolved.

## CP2 — Story grouping (tool 1 session; your review about 30 min)

Conditions B and C need reviewed stories. None exist yet, and reference clusters must not be
presented as verified stories (`docs/research-counting-draft-v1.md`, section 6).

- [x] Tool: worksheet of the included headlines plus an append-only grouping ledger. It shows
      no sentiment labels or predictions.
- [x] You: assign the groups. Mark unclear relations as uncertain; they stay visible and don't
      silently become single-headline stories. Recorded 2026-11-11 (see T2).

## CP3 — Run predictions (10 min)

From the main folder, on a clean pushed commit. The commands are in
`docs/research-protocol.md` under "Predictions". The first build takes about 12 minutes. Docker
has only about 4 GB of memory and went down during a Nov 4 image rebuild (cause unknown), so
check the stack after building.

- [x] Build `research-inference`; `--check` prints `"ready": true`.
- [x] Run on `session01-20261006-snapshot` (all 41 versions; scoring filters later).
- [x] Save the printed summary as the evidence. Run once more: `inserted` 0, nothing mismatched.
      Note for Git Bash: set `MSYS_NO_PATHCONV=1` or `/app/...` is rewritten to a Windows path.

## CP4 — `scripts/score.py` (1 session)

- [ ] Conditions A, B and C from the counting draft, sharing one set of frozen predictions.
- [ ] Macro-F1 and per-class precision/recall/F1 from weighted confusion matrices, with a
      bootstrap interval over story groups.
- [ ] Report counts for unresolved, excluded, assisted and ungrouped items.
- [ ] Tests: hand-computed toy example (`v1,v2,v3` weights `1,1,1`, `1/3,1/3,1/3`, `1,0,0`);
      unresolved is never scored as neutral.
- [ ] Run on the development labels. **The table is labeled pilot:** 26 labels show that the
      method works, not a finding about the model.

## CP5 — One-command table (30 min)

- [ ] One command regenerates the table from snapshot, selection, grouping, predictions and manifest.
- [ ] Run twice; the files are byte-identical. This is the Phase 1 exit gate.

## CP6 — README and demo (1 session)

- [ ] Landing page: the question, the pipeline diagram, the pilot table, how to run it, and
      limits. Move the infrastructure details to `docs/infrastructure.md`.
- [ ] Move `changes.txt` to `docs/archive/` only after checking what refers to it.
- [ ] Small demo on invented headlines that needs no live feeds.
- [ ] Say plainly what is not done: a research prototype and pilot, not a finished study.
- [ ] Optional: blog post on why no cosine threshold separates rewrites from different events.

## CP7 — Clean stop and tag (30 min)

Stopping changes the research record, so it runs only on your go-ahead.

- [ ] `docker compose --profile workers stop worker-rss worker-gdelt worker-sec`
- [ ] Health check until every partition shows `"unread": 0`, then stop `lakehouse-writer`.
      Rebuild `research-tools` first: the PC image predates the broker check.
- [ ] Back up the three Docker volumes outside the PC and note their SHA-256. This PC holds
      the only copy.
- [ ] Closure note `docs/session-<date>.md`: final numbers, gaps, and how to resume.
- [ ] ROADMAP Part B metrics: new date column. Commit, push and tag the release.
- [ ] Check that CI passed on the tagged commit.

---

## Completion boundary

Success means an outsider can clone the tag, read the limits, and regenerate the pilot table.
Do not describe this as a paper or a finished study, and do not quote the pilot numbers as a
result about FinBERT.

## Not in this plan (after December 20)

Cloud server and migration (`docs/cloud-collection-server.md`), the new baseline, GDELT and
Investing.com decisions, more labels, the second annotator, sample-size decision, final
collection, and the paper. Redis race, Tier 3 and SEC stay parked.

Until CP7 the PC keeps collecting while it's on, but this data can't support completeness
claims: the 2026-09-28 baseline stopped being valid after the Oct 26 retention gap.
