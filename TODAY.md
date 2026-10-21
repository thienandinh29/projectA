# Work session — next session (from Thursday, October 22, 2026)

Goal: **collection runs 24/7 on a server, and Phase 1 starts with a frozen model
and label-blind predictions.** Roadmap: finish Phase 0 (its 72 h exit gate needs the
server), begin Phase 1 in [ROADMAP.md](ROADMAP.md).
Previous checklist: [docs/archive/TODAY-20261021.md](docs/archive/TODAY-20261021.md);
its closure: [docs/session-20261014.md](docs/session-20261014.md).

Time blocks are elapsed session time (~5 h), not appointments. Check a box only
after its output exists. Part B rules apply: one doc per session (the closure note),
generated evidence instead of hand-written JSON, review batches of up to 25.

| Elapsed | Checkpoint | Done when |
| --- | --- | --- |
| 00:00–00:15 | CP0 Quick start | Stack state and last health numbers known |
| 00:15–01:00 | CP1 Create server + test sources | Server ready; Investing.com/GDELT results from it saved |
| 01:00–02:00 | CP2 Migrate collection | Collectors running on the server; PC stack stopped |
| 02:00–02:15 | Break | — |
| 02:15–03:00 | CP3 Freeze the model artifact | Model, revision SHA, file hashes, label map recorded |
| 03:00–04:15 | CP4 `scripts/predict_frozen.py` | Tested; predictions for the 41-version snapshot stored |
| 04:15–04:30 | Break | — |
| 04:30–04:45 | CP5 Source decisions | GDELT and Investing.com kept or dropped, with evidence |
| 04:45–05:00 | CP6 Close | Closure note, metrics, commit |

If the session is short, do CP0–CP2 only. **The server matters more than the model**:
every day on the PC loses most of the day's data.

---

## CP0 — Quick start (15 min)

```powershell
git pull
docker compose --profile workers ps
docker compose --profile tools run --rm --no-deps research-tools -m scripts.collection_health --hours 24
```

- [ ] Note whether the PC stack is running and the last attempt time (start of today's gap).

## CP1 — Create the server and test the sources (45 min)

Follow [docs/cloud-collection-server.md](docs/cloud-collection-server.md) steps 1–4.

- [ ] Server created: Ubuntu 24.04, 2 vCPU / 4 GB / 40 GB, SSH-key login. Note provider + region.
- [ ] Step 3 source test run **before anything else**. Paste the output into the closure note.
      If Investing.com still fails, try one other region before continuing.
- [ ] Step 4 done: `paper1` user, ufw (SSH only), UTC + NTP synchronized, swap, Docker.

## CP2 — Migrate collection (60 min)

Guide steps 5–8. Follow the protocol stop order exactly: collectors → saved drain
(exit 0) → writer.

- [ ] Server: repo in `~/financial-ingestion-pipeline`, `dc` alias, volumes created.
- [ ] PC: drain report `migration-drain-*.json` saved; the three volume archives' SHA-256 noted.
- [ ] Server: hashes match; restored; `COLLECTION_WORKTREE_STATE=clean`; stack up with
      `docker-compose.server.yml`; old sessions show as interrupted; writer shows `batch_committed`.
- [ ] One dated line in `docs/research-protocol.md`: gap start/end (UTC), provider/region,
      commit, drain report path.
- [ ] **PC collectors stay stopped from now on.** Turn off Docker autostart on the PC if it
      is on, so the old stack can't come back by accident.

## CP3 — Freeze the model artifact (45 min)

Roadmap Phase 1, first item. Record in `docs/research-protocol.md` (not a new doc).

- [ ] Model: `ProsusAI/finbert` unless there's a reason to choose another; write the reason.
- [ ] Pin the exact Hugging Face **revision SHA** (not `main`); store the downloaded files in
      the data volume (`/app/data/models/<model>@<sha>/`) and record each file's SHA-256.
- [ ] Record the tokenizer, the label mapping (index → positive/negative/neutral, read from
      the model config, not assumed) and the training-data provenance (Financial PhraseBank).
- [ ] Note for the paper: the 2026 headlines post-date the model's training data.

## CP4 — Label-blind predictions (75 min)

`scripts/predict_frozen.py`: CPU inference over a frozen snapshot.

- [ ] Append-only predictions table keyed by content hash + model revision, with logits,
      probabilities, predicted class and prediction time. Re-running never rewrites a row.
- [ ] **Label-blind:** the script must not open the annotation ledger or selections.
      A test enforces it.
- [ ] Tests: immutability, label-blind isolation, identical output on a re-run, and a
      wrong model hash being rejected.
- [ ] Run on `session01-20261006-snapshot` (all 41 versions; scoring filters later).
      Save the run's printed summary as the evidence.

## CP5 — Source decisions (15 min; Friday's decision)

Use the server's step 3 test plus its first hours of the health check.

- [ ] GDELT DOC: keep if the server gets any successes; otherwise drop it from the panel
      and record that GKG stays audit-only. Record either way in the protocol.
- [ ] Investing.com: roadmap rule: drop if < 80% success **on the server**.
- [ ] A panel change counts as a new configuration: protocol line + collector restart on the server.

## CP6 — Close (15 min)

- [ ] Server health: `dc --profile tools run --rm --no-deps research-tools -m scripts.collection_health --hours 24`.
- [ ] Closure note `docs/session-<date>.md` (about 15 lines): server provider/region, migration
      gap, source test results, model + revision SHA, prediction counts, open problems.
- [ ] ROADMAP Part B metrics: new date column.
- [ ] Commit and push docs and code (not data).

---

## Completion boundary

The session is a success if **collection runs on the server from a clean commit** and the PC
stack is stopped. CP3–CP4 can move to the next session; CP1–CP2 cannot.

## Not today (parked)

`scripts/score.py` and the `paper-dev` one-command table (next session, after predictions);
story-grouping script (at ~200 eligible headlines from ≥ 4 sources); revisiting the 4 earlier
unresolved labels (optional); second annotator (Phase 2); Redis race, Tier 3, SEC, README.

## Next session (preview)

Let the server collect. Build `scripts/score.py` for the three weighting conditions on the
development labels (26 labeled), then `paper-dev` to reproduce the table twice. After 72 h of
gap-free collection: Phase 0 exit gate, then measure yield (new unique financial headlines/day).
