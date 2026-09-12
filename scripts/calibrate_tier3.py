"""
Tier 3 Cosine Threshold Calibration (v2)
=========================================
Empirical calibration of SEMANTIC_COSINE_THRESHOLD, mirroring the Tier 2 LSH
methodology (theory + measured spectrum). The threshold is chosen from this
output, never hand-picked.

v2 changes (review round 2):
  - The gate measurement uses the PRODUCTION entity extractor
    (utils.entity_extract.extract_financial_entities), not oracle tags;
    oracle tags are reported as the ideal ceiling.
  - Reports the gate FALSE-REJECT rate: true-duplicate pairs blocked by the
    entity gate or the polarity guard (the cost of precision).
  - Reports alias false-positive fires on the unrelated band.
  - The recommendation reports precision AND recall AND F1 with explicit
    missed-pair counts — never precision alone.
  - Measures coverage on live lakehouse titles when data/_titles_sample.json
    exists.

Method:
  1. Score a labeled pair corpus in bands:
       B1  unrelated topics                     (must NOT merge)
       B2  paraphrases at graded lexical overlap (must merge)
       B3  same-event rewrites                  (must merge)
       B4  hard negatives: same ticker + same direction, DIFFERENT event (must NOT merge)
       B5  cross-entity same-template           (must NOT merge)
       B6  polarity inversions (incl. inflected, negated) (must NOT merge)
       B7  guard-cost pairs: true dups with mixed polarity vocab
  2. Report per-band cosine distributions, gated (production extractor +
     antonym polarity guard) vs ungated.
  3. Sweep thresholds; report P/R/F1 for both configurations.
  4. Recommend (precision-first): threshold above the highest surviving
     negative; >= 0.05 margin when the bands allow it; recall cost stated.

Usage:
    python scripts/calibrate_tier3.py
Output:
    stdout report + scripts/calibration_results.json
"""

import json
import sys
from pathlib import Path

import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from models.embedder import get_embedder
from utils.polarity import polarity_profile, has_conflict
from utils.entity_extract import extract_financial_entities

# ─────────────────────────────────────────────────────────────────────────────
#  PAIR CORPUS — (title_a, oracle_tickers_a, title_b, oracle_tickers_b)
#  Oracle tags are the ideal entity extraction, kept to measure the ceiling.
# ─────────────────────────────────────────────────────────────────────────────

B1_UNRELATED = [
    ("Federal Reserve raises interest rates by 25 basis points", [], "Manchester United signs new striker in record transfer deal", []),
    ("Oil prices slip amid demand concerns in Asia", [], "New breakthrough in quantum computing announced", []),
    ("US inflation cools to 2.9% in July", [], "Wimbledon final draws record global audience", []),
    ("Goldman Sachs lifts S&P 500 year-end target", [], "Severe drought affects wheat harvest in Australia", []),
    ("Treasury yields edge higher ahead of jobs report", [], "New study links sleep quality to memory retention", []),
    ("Euro slides against the dollar after PMI data", [], "Electric vehicle charging network expands to rural areas", []),
    ("Housing starts fall to three-month low", [], "Archaeologists uncover ancient Roman mosaic", []),
    ("Consumer confidence rebounds in September", [], "Streaming service rolls out ad-supported tier", []),
    ("Jobless claims tick up slightly last week", [], "Chef wins award for sustainable seafood menu", []),
    ("Dollar strengthens as traders pare Fed cut bets", [], "Local library launches digital lending pilot", []),
    ("Copper prices retreat on China growth worries", [], "Marathon record broken in Berlin", []),
    ("Retail sales miss forecasts in August", [], "Astronomers discover exoplanet with water vapor", []),
]

B2_PARAPHRASE = [
    ("Apple beats third-quarter earnings estimates", ["AAPL"], "Apple tops third-quarter earnings estimates", ["AAPL"]),
    ("Oil prices surge as OPEC announces production cuts", [], "Oil surges after OPEC announces output cut", []),
    ("Tesla misses delivery estimates for the first quarter", ["TSLA"], "Tesla first-quarter deliveries fall short of estimates", ["TSLA"]),
    ("Federal Reserve holds benchmark rate steady", [], "Fed leaves key rate unchanged", []),
    ("Amazon forecasts holiday-quarter sales below estimates", ["AMZN"], "Amazon gives weak holiday outlook", ["AMZN"]),
    ("US job growth slows sharply in August", [], "Hiring cools considerably last month", []),
    ("Bitcoin drops below $60,000 amid ETF outflows", [], "Crypto selloff deepens as outflows accelerate", []),
    ("US inflation cools more than expected in July", [], "Consumer price growth slows unexpectedly", []),
    ("Nvidia tops revenue views on data center boom", ["NVDA"], "Chipmaker's data center sales power beat-and-raise quarter", ["NVDA"]),
    ("Bank of England cuts rates to 4.75%", [], "Policymakers trim borrowing costs by a quarter point", []),
    ("GM recalls 400,000 vehicles over brake software", [], "Automaker recalls trucks and SUVs for software fix", []),
    ("Eurozone factory activity contracts for a sixth month", [], "Manufacturing downturn extends across the currency bloc", []),
]

B3_REWRITE = [
    ("Bitcoin falls below $60,000 amid ETF outflows", [], "Crypto markets slide as BTC drops under $60K", []),
    ("Microsoft unveils $60 billion buyback program", ["MSFT"], "Software giant expands shareholder returns with buyback", ["MSFT"]),
    ("Airline cancels 1,000 flights ahead of hurricane", [], "Carrier grounds weekend schedule as storm approaches", []),
    ("Starbucks workers strike over wage talks", [], "Coffee chain baristas walk out in pay dispute", []),
    ("Disney streaming turns first profit", [], "Media giant's streaming unit posts inaugural profit", []),
    ("Chip export curbs tighten for China", [], "Washington expands semiconductor sales restrictions", []),
    ("Housing market cools as mortgage rates hover near 7%", [], "Home sales slow with borrowing costs at decade highs", []),
    ("Ports brace for dockworker strike", [], "Shipping hubs prepare for labor stoppage", []),
]

B4_HARD_NEGATIVE = [
    ("Apple stock rises on strong earnings beat", ["AAPL"], "Apple stock rises after analyst upgrade", ["AAPL"]),
    ("Tesla shares climb after record quarterly deliveries", ["TSLA"], "Tesla shares climb on robotaxi unveiling", ["TSLA"]),
    ("Nvidia surges on data center demand", ["NVDA"], "Nvidia surges as it joins the Dow Jones", ["NVDA"]),
    ("Amazon rises on record Prime Day sales", ["AMZN"], "Amazon rises after announcing a stock split", ["AMZN"]),
    ("Microsoft gains on Azure cloud growth", ["MSFT"], "Microsoft gains after raising its dividend", ["MSFT"]),
    ("Goldman upgrades Ford on margin recovery", ["F"], "Morgan Stanley upgrades Ford citing EV demand", ["F"]),
    ("Apple discloses CFO transition in 8-K filing", ["AAPL"], "Apple announces $110 billion buyback in 8-K filing", ["AAPL"]),
    ("Meta jumps on strong ad revenue", ["META"], "Meta jumps after unveiling AI assistant", ["META"]),
    ("Boeing climbs as deliveries resume", ["BA"], "Boeing climbs on new defense contract", ["BA"]),
    ("Salesforce rises on activist stake report", ["CRM"], "Salesforce rises after earnings guidance raise", ["CRM"]),
]

B5_CROSS_ENTITY = [
    ("Federal Reserve raises interest rates by 25 basis points", [], "European Central Bank raises interest rates by 25 basis points", []),
    ("Apple beats quarterly earnings expectations", ["AAPL"], "Microsoft beats quarterly earnings expectations", ["MSFT"]),
    ("Tesla recalls 100,000 vehicles over autopilot", ["TSLA"], "Ford recalls 100,000 vehicles over brake defect", ["F"]),
    ("Bank of America cut to neutral by analyst", ["BAC"], "Wells Fargo cut to neutral by analyst", ["WFC"]),
    ("Google announces sweeping layoffs", [], "Amazon announces sweeping layoffs", []),
    ("Netflix subscriber growth beats forecasts", ["NFLX"], "Disney subscriber growth beats forecasts", ["DIS"]),
    ("Exxon posts record quarterly profit", ["XOM"], "Chevron posts record quarterly profit", ["CVX"]),
    ("Novartis drug wins FDA approval", [], "Pfizer drug wins FDA approval", []),
]

B6_POLARITY = [
    ("Tesla beats Q3 earnings estimates sending shares higher", ["TSLA"], "Tesla misses Q3 earnings estimates sending shares lower", ["TSLA"]),
    ("Federal Reserve announces surprise rate hike of 50 basis points", [], "Federal Reserve announces surprise rate cut of 50 basis points", []),
    ("Nvidia shares surged after the bell", ["NVDA"], "Nvidia shares slumped after the bell", ["NVDA"]),
    ("Amazon revenue missed analyst estimates by a wide margin", ["AMZN"], "Amazon revenue topped analyst estimates by a wide margin", ["AMZN"]),
    ("Analysts expect the company to beat expectations", [], "Analysts said the company did not beat expectations", []),
    ("Morgan Stanley upgrades Nike to overweight", ["NKE"], "Morgan Stanley downgrades Nike to underweight", ["NKE"]),
    ("Stocks rally on stimulus hopes", [], "Stocks sink as stimulus hopes fade", []),
    ("Oil climbs on supply worries", [], "Oil tumbles on supply worries", []),
    ("Profit rose 12% year over year", [], "Profit fell 12% year over year", []),
    ("Homebuilder sentiment jumps to five-month high", [], "Homebuilder sentiment sinks to five-month low", []),
]

B7_GUARD_COST = [
    ("Oil prices surge as OPEC cuts production", [], "Crude oil rallies on OPEC supply reduction", []),
    ("Tesla surges on record deliveries despite price cuts", ["TSLA"], "Tesla rallies as record volumes offset price reductions", ["TSLA"]),
    ("Gold gains as dollar slides on rate-cut bets", [], "Gold climbs with the dollar lower on easing bets", []),
]

BANDS = [
    ("B1_unrelated", B1_UNRELATED, "distinct"),
    ("B2_paraphrase", B2_PARAPHRASE, "dup"),
    ("B3_rewrite", B3_REWRITE, "dup"),
    ("B4_hard_negative", B4_HARD_NEGATIVE, "distinct"),
    ("B5_cross_entity", B5_CROSS_ENTITY, "distinct"),
    ("B6_polarity", B6_POLARITY, "distinct"),
    ("B7_guard_cost", B7_GUARD_COST, "dup-guard-blocked"),
]

NEGATIVE_BANDS = {"B1_unrelated", "B4_hard_negative", "B5_cross_entity", "B6_polarity"}
POSITIVE_BANDS = {"B2_paraphrase", "B3_rewrite"}


def main():
    embedder = get_embedder()

    # Build unique title list and embed once; extract entities once (production).
    titles = []
    for _, pairs, _ in BANDS:
        for a, _, b, _ in pairs:
            titles.extend([a, b])
    uniq = sorted(set(titles))
    matrix = embedder.embed_batch(uniq)
    vec = {t: matrix[i] for i, t in enumerate(uniq)}
    ent = {t: set(extract_financial_entities(t)) for t in uniq}

    # Score all pairs with production + oracle gate metadata
    scored = []
    for band, pairs, label in BANDS:
        for a, ta, b, tb in pairs:
            cos = float(np.dot(vec[a], vec[b]))
            ent_blocked = bool(ent[a] and ent[b] and ent[a].isdisjoint(ent[b]))
            oa, ob = set(ta), set(tb)
            oracle_blocked = bool(oa and ob and oa.isdisjoint(ob))
            pol_blocked = has_conflict(polarity_profile(a), polarity_profile(b))
            scored.append({
                "band": band, "label": label, "cos": cos, "a": a, "b": b,
                "entity_blocked": ent_blocked, "oracle_blocked": oracle_blocked,
                "polarity_blocked": pol_blocked,
                "gate_blocked": ent_blocked or pol_blocked,
            })

    # ── 1. Band distributions ──
    print("\n=== 1. BAND DISTRIBUTIONS (cosine; gate = production extractor + antonym polarity) ===")
    print(f"{'band':<18} {'n':>3} {'label':<18} {'min':>6} {'median':>7} {'max':>6} "
          f"{'gate-blk':>8} {'min-surv':>9} {'max-surv':>9}")
    band_stats = {}
    for band, _, label in BANDS:
        rows = [r for r in scored if r["band"] == band]
        coss = [r["cos"] for r in rows]
        survivors = [r["cos"] for r in rows if not r["gate_blocked"]]
        blocked = len(rows) - len(survivors)
        band_stats[band] = {
            "n": len(rows), "label": label,
            "min": min(coss), "median": float(np.median(coss)), "max": max(coss),
            "gate_blocked": blocked,
            "min_surviving": min(survivors) if survivors else None,
            "max_surviving": max(survivors) if survivors else None,
        }
        print(f"{band:<18} {len(rows):>3} {label:<18} {min(coss):>6.3f} {np.median(coss):>7.3f} {max(coss):>6.3f} "
              f"{blocked:>8} "
              f"{(f'{min(survivors):9.3f}' if survivors else '     none'):>9} "
              f"{(f'{max(survivors):9.3f}' if survivors else '     none'):>9}")

    # ── 2. Entity extractor coverage & false positives ──
    print("\n=== 2. ENTITY EXTRACTOR COVERAGE & FALSE POSITIVES ===")
    try:
        from workers.rss_worker import extract_tickers as old_extractor
        old_hit = sum(1 for t in uniq if old_extractor(t))
    except Exception:
        old_hit = None
    new_hit = sum(1 for t in uniq if ent[t])
    oracle_hit = sum(
        1 for band, pairs, _ in BANDS for a, ta, b, tb in pairs
        if ta or tb
    ) // 2 * 2  # rough ceiling: pairs with any oracle tag
    print(f"  Corpus titles: {len(uniq)}. Old regex: {old_hit} titles. "
          f"Production extractor: {new_hit} titles ({new_hit/len(uniq)*100:.0f}%).")

    live_path = PROJECT_ROOT / "data" / "_titles_sample.json"
    if live_path.exists():
        live = json.loads(live_path.read_text())
        l_new = sum(1 for t in live if extract_financial_entities(t))
        try:
            from workers.rss_worker import extract_tickers as old_extractor
            l_old = sum(1 for t in live if old_extractor(t))
            print(f"  LIVE lakehouse titles (n={len(live)}): old regex {l_old} ({l_old/len(live)*100:.0f}%) "
                  f"-> production {l_new} ({l_new/len(live)*100:.0f}%). Note: many unmatched titles are "
                  f"fund names / market commentary with no single primary entity (gate no-op by design).")
        except Exception:
            print(f"  LIVE lakehouse titles (n={len(live)}): production extractor hits {l_new}.")

    b1_fires = sorted({c for r in scored if r["band"] == "B1_unrelated"
                       for c in (ent[r["a"]] | ent[r["b"]])})
    print(f"  Alias fires on UNRELATED band (false positives): {b1_fires if b1_fires else 'none'}")

    # ── 3. Gate false-reject analysis (cost of precision) ──
    print("\n=== 3. GATE FALSE-REJECT RATE (true-duplicate pairs blocked by gates) ===")
    positives = [r for r in scored if r["band"] in POSITIVE_BANDS]
    for name, key in [("entity gate", "entity_blocked"), ("polarity guard", "polarity_blocked"), ("either gate", "gate_blocked")]:
        blocked = [r for r in positives if r[key]]
        print(f"  {name:<15}: {len(blocked)}/{len(positives)} positive pairs blocked "
              f"({len(blocked)/len(positives)*100:.0f}%)")
        for r in blocked:
            print(f"      - [{r['band']}] {r['a'][:48]} || {r['b'][:48]} (cos={r['cos']:.3f})")

    # ── 4. Threshold sweep ──
    print("\n=== 4. THRESHOLD SWEEP (precision-first: wrong merges are fatal) ===")
    print(f"{'t':>5} | {'cos-only':>23} | {'gated (production)':>30}")
    print(f"{'':>5} | {'P':>6} {'R':>6} {'F1':>6} {'FM':>4} | {'P':>6} {'R':>6} {'F1':>6} {'FM':>4}   (FM = false merges)")
    sweep = []
    pos = positives
    neg = [r for r in scored if r["band"] in NEGATIVE_BANDS]
    for t100 in range(40, 96):
        t = t100 / 100.0
        for gated in (False, True):
            def merges(r):
                if r["cos"] < t:
                    return False
                if gated and r["gate_blocked"]:
                    return False
                return True
            tp = sum(1 for r in pos if merges(r))
            fp = sum(1 for r in neg if merges(r))
            prec = tp / (tp + fp) if tp + fp else 0.0
            rec = tp / len(pos) if pos else 0.0
            f1 = 2 * prec * rec / (prec + rec) if prec + rec else 0.0
            sweep.append({"t": t, "gated": gated, "precision": prec, "recall": rec, "f1": f1, "false_merges": fp})
    for i in range(0, len(sweep), 2):
        c, g = sweep[i], sweep[i + 1]
        print(f"{c['t']:>5.2f} | {c['precision']:>6.3f} {c['recall']:>6.3f} {c['f1']:>6.3f} {c['false_merges']:>4} | "
              f"{g['precision']:>6.3f} {g['recall']:>6.3f} {g['f1']:>6.3f} {g['false_merges']:>4}")

    # ── 5. Recommendation ──
    print("\n=== 5. RECOMMENDATION (precision-first) ===")
    neg_surv = [r["cos"] for r in scored if r["band"] in NEGATIVE_BANDS and not r["gate_blocked"]]
    pos_surv = [r["cos"] for r in scored if r["band"] in POSITIVE_BANDS and not r["gate_blocked"]]
    t_neg = max(neg_surv) if neg_surv else 0.0
    t_pos = min(pos_surv) if pos_surv else 1.0
    margin = t_pos - t_neg
    print(f"  Highest surviving negative (any threshold below this merges a distinct pair): {t_neg:.4f}")
    print(f"  Lowest surviving positive  (any threshold above this loses this duplicate):    {t_pos:.4f}")
    print(f"  Separation margin: {margin:+.4f}")

    if margin >= 0.10:
        recommended = round((t_neg + t_pos) / 2, 2)
        rationale = "clean separation: midpoint of the gap"
    elif margin >= 0.05:
        recommended = round(t_neg + 0.05, 2)
        rationale = "valid gap: 0.05 above worst negative"
    else:
        recommended = round(t_neg + 0.02, 2)
        rationale = ("OVERLAP: bands are not separable at any threshold. Precision-first keeps the "
                     "threshold just above the worst negative and accepts the recall cost below.")
    rec = next(s for s in sweep if s["t"] >= recommended and s["gated"])
    merged = [r for r in pos if r["cos"] >= recommended and not r["gate_blocked"]]
    missed = [r for r in pos if not (r["cos"] >= recommended and not r["gate_blocked"])]
    print(f"\n  RECOMMENDED SEMANTIC_COSINE_THRESHOLD = {recommended:.2f}  ({rationale})")
    print(f"  HEADLINE @ τ={recommended:.2f} (gated): precision={rec['precision']:.3f}  "
          f"recall={rec['recall']:.3f}  F1={rec['f1']:.3f}  false_merges={rec['false_merges']}")
    print(f"  RECALL COST: {len(merged)}/{len(pos)} true-duplicate pairs merged; "
          f"{len(missed)} MISSED (cosine below τ or gate-blocked):")
    for r in missed:
        why = "gate-blocked" if r["gate_blocked"] else "below τ"
        print(f"      - [{r['band']}|{why}] {r['a'][:44]} || {r['b'][:44]} (cos={r['cos']:.3f})")

    # Oracle ceiling: what an ideal entity extractor would additionally buy.
    neg_oracle = [r["cos"] for r in scored
                  if r["band"] in NEGATIVE_BANDS and not r["oracle_blocked"] and not r["polarity_blocked"]]
    if neg_oracle:
        print(f"\n  ORACLE CEILING: with perfect entity tags, highest surviving negative would be "
              f"{max(neg_oracle):.4f} (vs {t_neg:.4f} production) — the extractor gap costs "
              f"{t_neg - max(neg_oracle):.4f} of threshold headroom.")

    out = {
        "recommended_threshold": recommended, "rationale": rationale,
        "t_neg_max": t_neg, "t_pos_min": t_pos, "margin": margin,
        "at_threshold": rec,
        "band_stats": band_stats, "sweep": sweep, "pairs": scored,
    }
    out_path = PROJECT_ROOT / "scripts" / "calibration_results.json"
    out_path.write_text(json.dumps(out, indent=2))
    print(f"\nResults written to {out_path}")


if __name__ == "__main__":
    main()
