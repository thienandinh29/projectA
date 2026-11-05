# Work plan — CV version, then pause (from November 5, 2026)

Goal: **one reproducible pilot result and a project an outsider can understand in five
minutes, then a clean stop until after December 20.** Decided by the user on 2026-11-05.
Previous checklist: [docs/archive/TODAY-20261104.md](docs/archive/TODAY-20261104.md).
Roadmap: this finishes Phase 1 and the portfolio track in [ROADMAP.md](ROADMAP.md).

About 2–3 sessions. Check a box only after its output exists. The order matters: your
judgments come before anyone sees a prediction.

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

- [ ] Revisit the 4 earlier unresolved labels, or leave them unresolved. Unresolved is not
      neutral and is counted separately.

## CP2 — Story grouping (tool 1 session; your review about 30 min)

Conditions B and C need reviewed stories. None exist yet, and reference clusters must not be
presented as verified stories (`docs/research-counting-draft-v1.md`, section 6).

- [ ] Tool: worksheet of the included headlines plus an append-only grouping ledger. It shows
      no sentiment labels or predictions.
- [ ] You: assign the groups. Mark unclear relations as uncertain; they stay visible and don't
      silently become single-headline stories.

## CP3 — Run predictions (10 min)

From the main folder, on a clean pushed commit. The commands are in
`docs/research-protocol.md` under "Predictions". The first build takes about 12 minutes. Docker
has only about 4 GB of memory and went down during a Nov 4 image rebuild (cause unknown), so
check the stack after building.

- [ ] Build `research-inference`; `--check` prints `"ready": true`.
- [ ] Run on `session01-20261006-snapshot` (all 41 versions; scoring filters later).
- [ ] Save the printed summary as the evidence. Run once more: `inserted` 0, nothing mismatched.

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
