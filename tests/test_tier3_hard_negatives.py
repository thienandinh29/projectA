"""
Tier 3 Hard-Negative & Gate Test Suite
=======================================
The senior review flagged that the old suite only proved Tier 3 CATCHES true
duplicates — nothing proved it EXCLUDES different stories. This suite adds the
adversarial half. All threshold/gate behavior is traceable to
scripts/calibrate_tier3.py (63-pair labeled corpus, 7 bands, gated sweep,
precision-first policy):

  - SEMANTIC_COSINE_THRESHOLD was hand-picked 0.65, now calibrated 0.88.
  - Measured on all-MiniLM-L6-v2 (fastembed ONNX): hard negatives (same ticker,
    different event) score 0.44-0.74 and OVERLAP the same-event rewrite band
    (0.25-0.86) — the bands are not separable. At 0.65 the gated merge had
    precision 0.43 (8 distinct-event false merges); at 0.88 precision is 1.000.
  - Accepted cost: recall on low-overlap paraphrases drops to ~5% on the
    corpus; Tier 2 LSH remains the main dedup workhorse. The hole is pinned
    by test_recall_hole_documented below.

Also verifies the ticker/entity gate and the polarity-class guard
(utils/polarity.py) as merge gates, including the regression for the old
set-inequality over-blocking bug.

Redis db=15 (isolated from production db=0), flushed per test.
"""

import os
import sys
import time
import unittest
import threading
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

os.environ["REDIS_DB"] = "15"


class TestTier3HardNegatives(unittest.TestCase):
    """Adversarial pairs that must NOT cluster through the full 3-tier path."""

    def setUp(self):
        from utils.redis_cache import RedisDeduplicator
        self.dedup = RedisDeduplicator(db=15)
        if not self.dedup.ping():
            self.skipTest("Redis container is not reachable")
        self.dedup.client.flushdb()

    def tearDown(self):
        try:
            self.dedup.client.flushdb()
        except Exception:
            pass

    def _check(self, event_id, title, tickers=None):
        return self.dedup.check_dedup(event_id, title, source="RSS", tickers=tickers)

    def test_same_ticker_different_event_not_clustered(self):
        """
        The senior's exact failure mode: same company, same direction, two
        genuine events an hour apart. Measured cosine ~0.70 — below the
        calibrated threshold; the ticker gate overlaps here (same AAPL) so the
        threshold alone must hold the line.
        """
        ts = int(time.time() * 1000)
        _, _, _ = self._check(f"hn_a_{ts}", "Apple stock rises on strong earnings beat", ["AAPL"])
        _, is_near, _ = self._check(f"hn_b_{ts}", "Apple stock rises after analyst upgrade", ["AAPL"])
        self.assertFalse(is_near,
            "Different events about the same ticker were merged — threshold regression")

    def test_cross_entity_same_template_not_clustered(self):
        """
        Same sentence template, only the company swapped (measured cosine
        0.776). Threshold rejects; the ticker gate is armed as defense-in-depth.
        """
        ts = int(time.time() * 1000)
        _, _, _ = self._check(f"ce_a_{ts}", "Apple beats quarterly earnings expectations", ["AAPL"])
        _, is_near, _ = self._check(f"ce_b_{ts}", "Microsoft beats quarterly earnings expectations", ["MSFT"])
        self.assertFalse(is_near,
            "Company-swap template pair was merged — must never cluster")

    def test_ticker_gate_blocks_identical_embedding_disjoint_tickers(self):
        """
        Deterministic gate test, model noise removed: two events share a
        literally identical embedding (cosine = 1.0 > any threshold) but have
        disjoint ticker sets. The gate must reject the merge.
        """
        from utils.semantic_dedup import SemanticDeduplicator
        from models.embedder import get_embedder

        sem = SemanticDeduplicator(db=15)
        vec = get_embedder().embed_text("Company stock rises on strong earnings beat")

        is_dup, _, score = sem.check_semantic_duplicate(
            "gate_a", vec, title="Apple stock rises on strong earnings beat", tickers=["AAPL"])
        self.assertFalse(is_dup)

        is_dup, canon, score = sem.check_semantic_duplicate(
            "gate_b", vec, title="Microsoft stock rises on strong earnings beat", tickers=["MSFT"])
        self.assertFalse(is_dup,
            f"Ticker gate failed: identical embedding with disjoint tickers merged (score={score:.3f})")
        self.assertIsNone(canon)

    def test_ticker_gate_passes_when_tickers_overlap(self):
        """Same identical-embedding setup, but overlapping tickers -> must merge."""
        from utils.semantic_dedup import SemanticDeduplicator
        from models.embedder import get_embedder

        sem = SemanticDeduplicator(db=15)
        vec = get_embedder().embed_text("Company stock rises on strong earnings beat")

        sem.check_semantic_duplicate(
            "gate_pass_a", vec, title="Apple stock rises on strong earnings beat", tickers=["AAPL"])
        is_dup, canon, _ = sem.check_semantic_duplicate(
            "gate_pass_b", vec, title="Apple shares rally on strong earnings beat", tickers=["AAPL", "NASDAQ"])
        self.assertTrue(is_dup, "Overlapping tickers + identical embedding must merge")
        self.assertEqual(canon, "gate_pass_a")

    def test_empty_ticker_sets_bypass_gate(self):
        """
        Macro headlines extract no tickers — the gate must be a no-op when
        either side is empty, or every macro story would be unmergeable.
        Identical embeddings, no tickers -> merge.
        """
        from utils.semantic_dedup import SemanticDeduplicator
        from models.embedder import get_embedder

        sem = SemanticDeduplicator(db=15)
        vec = get_embedder().embed_text("Central bank holds benchmark rate steady")

        sem.check_semantic_duplicate("macro_a", vec, title="Central bank holds benchmark rate steady", tickers=[])
        is_dup, canon, _ = sem.check_semantic_duplicate(
            "macro_b", vec, title="Central bank holds benchmark rate steady", tickers=None)
        self.assertTrue(is_dup, "Empty ticker sets must bypass the gate")
        self.assertEqual(canon, "macro_a")

    def test_polarity_guard_blocks_inversion_above_threshold(self):
        """
        beats/misses pair measures cosine 0.916 — ABOVE the calibrated
        threshold. Same ticker (gate passes). Only the polarity-class guard
        stops this merge. Without it, 0.88 would ship opposite-event merges.
        """
        from utils.polarity import polarity_profile, has_conflict

        ts = int(time.time() * 1000)
        a = "Tesla beats Q3 earnings estimates sending shares higher"
        b = "Tesla misses Q3 earnings estimates sending shares lower"
        self.assertTrue(has_conflict(polarity_profile(a), polarity_profile(b)),
            "Guard must classify beat/miss profiles as conflicting")

        _, _, _ = self._check(f"pg_a_{ts}", a, ["TSLA"])
        _, is_near, _ = self._check(f"pg_b_{ts}", b, ["TSLA"])
        self.assertFalse(is_near,
            "Polarity guard failed: beats/misses merged despite cosine > threshold")

    def test_negation_inversion_not_clustered(self):
        """'did not beat' flips polarity via the negation window."""
        from utils.polarity import polarity_profile, has_conflict

        a = "Analysts expect the company to beat expectations"
        b = "Analysts said the company did not beat expectations"
        self.assertTrue(has_conflict(polarity_profile(a), polarity_profile(b)),
            "Negation window must flip 'did not beat' to the negative class")

        ts = int(time.time() * 1000)
        _, _, _ = self._check(f"neg_a_{ts}", a)
        _, is_near, _ = self._check(f"neg_b_{ts}", b)
        self.assertFalse(is_near, "Negated polarity pair must not merge")

    def test_inflected_polarity_recognized(self):
        """
        'missed' / 'topped' are not in the raw lexicon — inflection
        normalization must resolve 'missed' to 'miss'. The pair scores 0.863
        (below threshold), so the assertion is belt-and-braces; the mechanism
        check is the point.
        """
        from utils.polarity import base_form
        self.assertEqual(base_form("missed"), "miss")
        self.assertEqual(base_form("tumbled"), "tumble")
        self.assertEqual(base_form("surged"), "surge")

        ts = int(time.time() * 1000)
        _, _, _ = self._check(
            f"inf_a_{ts}", "Amazon revenue missed analyst estimates by a wide margin", ["AMZN"])
        _, is_near, _ = self._check(
            f"inf_b_{ts}", "Amazon revenue topped analyst estimates by a wide margin", ["AMZN"])
        self.assertFalse(is_near, "Opposite-result estimate pairs must not merge")

    def test_recall_hole_documented(self):
        """
        Documents the ACCEPTED recall hole, pinned in CI: this same-event
        paraphrase pair measures cosine ~0.86 — below the calibrated 0.88, so
        it is deliberately NOT merged (the old 0.65 caught it, but also merged
        8 distinct-event pairs). Tier 2 LSH is expected to catch this class in
        production via shared vocabulary. If the cosine drifts above the
        threshold, this test flips into asserting a merge instead.
        """
        import numpy as np
        from models.embedder import get_embedder
        from config import SEMANTIC_COSINE_THRESHOLD

        ts = int(time.time() * 1000)
        a = "Oil prices surge as OPEC announces production cuts"
        b = "Oil surges after OPEC announces output cut"
        _, _, _ = self._check(f"hole_a_{ts}", a)
        _, is_near, _ = self._check(f"hole_b_{ts}", b)

        embedder = get_embedder()
        cos = float(np.dot(embedder.embed_text(a), embedder.embed_text(b)))
        if cos >= SEMANTIC_COSINE_THRESHOLD:
            self.assertTrue(is_near, "Pair is now above threshold but was not merged")
        else:
            self.assertFalse(is_near,
                "Behavior changed: pair below threshold got merged — precision regression")
            self.assertGreaterEqual(cos, 0.80, "hole band drifted; re-run scripts/calibrate_tier3.py")


class TestPolarityOverBlockRegression(unittest.TestCase):
    """
    Regression for the set-inequality over-blocking bug: the old guard treated
    ANY difference in keyword sets as a conflict, blocking true duplicates
    like "Tesla beats Q3 estimates" vs "Tesla beats Q3 estimates, shares rise".
    The class-based rule must not.
    """

    def setUp(self):
        from utils.redis_cache import RedisDeduplicator
        self.dedup = RedisDeduplicator(db=15)
        if not self.dedup.ping():
            self.skipTest("Redis container is not reachable")
        self.dedup.client.flushdb()

    def tearDown(self):
        try:
            self.dedup.client.flushdb()
        except Exception:
            pass

    def test_unit_no_conflict_when_classes_match(self):
        from utils.polarity import polarity_profile, has_conflict
        a = polarity_profile("Tesla beats Q3 estimates")
        b = polarity_profile("Tesla beats Q3 estimates, shares rise")
        self.assertFalse(has_conflict(a, b),
            "pos{beat} vs pos{beat,rise} must not be a conflict")

    def test_true_duplicate_merges_despite_extra_polarity_word(self):
        """
        Tier-3 isolated: explicit threshold 0.65 so the merge decision exercises
        the GUARD, not the calibrated threshold (cosine ~0.86 misses 0.88).
        Under the old set-inequality guard this pair was blocked at any
        threshold — that was the bug.
        """
        from utils.semantic_dedup import SemanticDeduplicator
        from models.embedder import get_embedder

        sem = SemanticDeduplicator(db=15, cosine_threshold=0.65)
        embedder = get_embedder()
        vec_a = embedder.embed_text("Tesla beats Q3 estimates")
        vec_b = embedder.embed_text("Tesla beats Q3 estimates, shares rise")

        sem.check_semantic_duplicate("ob_a", vec_a, title="Tesla beats Q3 estimates")
        is_dup, canon, score = sem.check_semantic_duplicate(
            "ob_b", vec_b, title="Tesla beats Q3 estimates, shares rise")
        self.assertTrue(is_dup, f"True duplicate over-blocked by the guard (score={score:.3f})")
        self.assertEqual(canon, "ob_a")


class TestEmbedderThreadSafety(unittest.TestCase):
    """
    Smoke test for the locked singleton: concurrent embed_text from multiple
    worker threads must return identical vectors to single-threaded calls.
    (main.py runs RSS/GDELT/SEC in parallel threads; onnxruntime sessions are
    documented thread-safe, but the tokenizer wrapper is verified here.)
    """

    TEXTS = [
        "Federal Reserve raises interest rates by 25 basis points",
        "Oil prices surge as OPEC cuts production quotas",
        "Apple beats quarterly earnings expectations",
        "US inflation cools to 2.9% in July",
    ]

    def test_concurrent_embeds_match_single_threaded(self):
        from models.embedder import get_embedder
        embedder = get_embedder()

        expected = {t: embedder.embed_text(t) for t in self.TEXTS}
        results = {}
        errors = []

        def worker():
            try:
                for t in self.TEXTS:
                    for _ in range(25):
                        results.setdefault(t, []).append(embedder.embed_text(t))
            except Exception as e:  # pragma: no cover
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [])
        for t, vecs in results.items():
            self.assertEqual(len(vecs), 100, f"missing concurrent results for: {t}")
            for v in vecs:
                self.assertTrue(np.allclose(v, expected[t], atol=1e-5),
                    f"concurrent embed diverged from single-threaded for: {t}")


class TestMixedPolarityAdversarials(unittest.TestCase):
    """
    Review round 2: the antonym-based rule must handle BOTH mixed-profile
    directions — block net-opposite mixed pairs, and merge same-direction
    mixed pairs (the old set-difference rule failed the second; a naive
    shared-class rule fails the first).
    """

    def setUp(self):
        from utils.redis_cache import RedisDeduplicator
        self.dedup = RedisDeduplicator(db=15)
        if not self.dedup.ping():
            self.skipTest("Redis container is not reachable")
        self.dedup.client.flushdb()

    def tearDown(self):
        try:
            self.dedup.client.flushdb()
        except Exception:
            pass

    def test_mixed_vs_mixed_net_opposite_not_clustered(self):
        """slide/beat vs rally/miss: both mixed, net outcome opposite -> conflict."""
        from utils.polarity import polarity_profile, has_conflict
        a = "Stocks slide even as companies beat profit forecasts"
        b = "Stocks rally even as companies miss profit forecasts"
        self.assertTrue(has_conflict(polarity_profile(a), polarity_profile(b)),
            "Net-opposite mixed profiles must conflict (slide/rally + beat/miss antonyms)")

        ts = int(time.time() * 1000)
        _, _, _ = self.dedup.check_dedup(f"mx_a_{ts}", a, source="RSS")
        _, is_near, _ = self.dedup.check_dedup(f"mx_b_{ts}", b, source="RSS")
        self.assertFalse(is_near, "Net-opposite mixed headlines were merged")

    def test_mixed_vs_mixed_shared_negative_true_duplicate_merges(self):
        """
        surges/cuts vs rallies/cuts: both mixed, same direction (the shared
        'cuts' is the CAUSE in both). Must NOT conflict; with an explicit low
        threshold (isolating the guard from the calibrated 0.76) it must merge.
        """
        from utils.polarity import polarity_profile, has_conflict
        from utils.semantic_dedup import SemanticDeduplicator
        from models.embedder import get_embedder

        a = "Oil surges as OPEC extends output cuts"
        b = "Crude rallies as OPEC prolongs supply cuts"
        self.assertFalse(has_conflict(polarity_profile(a), polarity_profile(b)),
            "Same-direction mixed profiles must not conflict")

        sem = SemanticDeduplicator(db=15, cosine_threshold=0.65)
        embedder = get_embedder()
        sem.check_semantic_duplicate("mx2_a", embedder.embed_text(a), title=a)
        is_dup, canon, score = sem.check_semantic_duplicate(
            "mx2_b", embedder.embed_text(b), title=b)
        self.assertTrue(is_dup, f"True same-direction duplicate over-blocked (score={score:.3f})")
        self.assertEqual(canon, "mx2_a")


class TestMultiTickerGateSemantics(unittest.TestCase):
    """Defined behavior for multi-ticker headlines: overlap merges, disjoint blocks."""

    def setUp(self):
        from utils.semantic_dedup import SemanticDeduplicator
        from models.embedder import get_embedder
        if not redis_ping():
            self.skipTest("Redis container is not reachable")
        self.sem = SemanticDeduplicator(db=15)
        self.vec = get_embedder().embed_text("Company stock rises on strong earnings beat")

    def tearDown(self):
        try:
            import redis
            redis.Redis(host="localhost", port=6379, db=15).flushdb()
        except Exception:
            pass

    def test_multi_ticker_disjoint_blocks(self):
        """'Goldman upgrades Ford' vs an Apple story: {F,GS} vs {AAPL} disjoint -> block."""
        self.sem.check_semantic_duplicate(
            "mt_a", self.vec, title="Goldman upgrades Ford on margin recovery",
            tickers=["F", "GS"])
        is_dup, canon, score = self.sem.check_semantic_duplicate(
            "mt_b", self.vec, title="Apple stock rises on strong earnings beat",
            tickers=["AAPL"])
        self.assertFalse(is_dup, f"Disjoint multi-ticker sets merged (score={score:.3f})")

    def test_multi_ticker_overlap_merges(self):
        """{F, GS} vs {GS, MS}: shared GS -> merge allowed."""
        self.sem.check_semantic_duplicate(
            "mt2_a", self.vec, title="Goldman upgrades Ford on margin recovery",
            tickers=["F", "GS"])
        is_dup, canon, _ = self.sem.check_semantic_duplicate(
            "mt2_b", self.vec, title="Microsoft gains on Azure cloud growth",
            tickers=["GS", "MS"])
        self.assertTrue(is_dup, "Overlapping multi-ticker sets must merge")
        self.assertEqual(canon, "mt2_a")


class TestScanCapParityAndLimitation(unittest.TestCase):
    """
    The capped newest-first scan must produce IDENTICAL dedup decisions to the
    brute-force scan whenever the window fits under the cap (parity), and its
    one divergence — matches to canonicals older than the newest N — must be
    exactly the documented limitation, nothing else.
    """

    FILLER_WORDS = [
        "quartz", "onyx", "cobalt", "zephyr", "meadow", "falcon", "saffron",
        "tundra", "pebble", "willow", "crater", "garnet", "hazel", "juniper",
        "kelp", "lichen", "marble", "nutmeg",
    ]

    def setUp(self):
        if not redis_ping():
            self.skipTest("Redis container is not reachable")
        import redis
        self.r_brute = redis.Redis(host="localhost", port=6379, db=15)
        self.r_cap = redis.Redis(host="localhost", port=6379, db=14)
        self.r_brute.flushdb()
        self.r_cap.flushdb()

        from utils.semantic_dedup import SemanticDeduplicator, WINDOW_ZSET_KEY
        from models.embedder import get_embedder
        self.brute = SemanticDeduplicator(db=15, scan_limit=0)
        self.capped = SemanticDeduplicator(db=14, scan_limit=10)
        self.embedder = get_embedder()

        # Prefill identical windows via direct writes (O(W), not O(W^2)).
        # Filler titles are word-pair headlines with disjoint vocabulary so
        # cross-filler cosine stays far below the threshold — each probe's
        # only plausible match is its own filler. zadd scores ascending:
        # entry 0 is the OLDEST, entry 17 the NEWEST.
        self.fillers = [
            f"{w} {self.FILLER_WORDS[(i * 7 + 3) % len(self.FILLER_WORDS)]} news"
            for i, w in enumerate(self.FILLER_WORDS)
        ]
        vecs = self.embedder.embed_batch(self.fillers)
        now = time.time()
        for r in (self.r_brute, self.r_cap):
            pipe = r.pipeline()
            for i, (t, v) in enumerate(zip(self.fillers, vecs)):
                eid = f"fill_{i}".encode()
                pipe.set(b"emb:vec:" + eid, v.astype(np.float32).tobytes(), ex=7200)
                pipe.set(b"emb:pol:" + eid, b"", ex=7200)
                pipe.zadd(WINDOW_ZSET_KEY, {eid: now - (len(self.FILLER_WORDS) - i)})
            pipe.execute()

    def tearDown(self):
        for r in (getattr(self, "r_brute", None), getattr(self, "r_cap", None)):
            try:
                if r:
                    r.flushdb()
                    r.close()
            except Exception:
                pass

    def test_parity_when_window_fits_under_cap(self):
        """Cap (20) > window (18): the capped code path must produce identical
        decisions to brute force, including real matches and scores."""
        from utils.semantic_dedup import SemanticDeduplicator
        capped_wide = SemanticDeduplicator(db=14, scan_limit=20)  # non-binding cap

        decisions = []
        for i in (3, 7, 12):
            title = self.fillers[i]  # identical text -> cos 1.0 with filler_i
            v = self.embedder.embed_text(title)
            decisions.append(self.brute.check_semantic_duplicate(f"probe_exact_{i}_b", v, title=title))
            decisions.append(capped_wide.check_semantic_duplicate(f"probe_exact_{i}_w", v, title=title))

        for i in range(0, len(decisions), 2):
            brute, wide = decisions[i], decisions[i + 1]
            self.assertTrue(brute[0], f"filler {i//2} should merge in brute force")
            self.assertEqual(brute[0], wide[0], f"dup decision diverged for filler {i//2}")
            self.assertEqual(brute[1], wide[1], f"canonical diverged for filler {i//2}")
            self.assertAlmostEqual(brute[2], wide[2], places=5, msg="score diverged")

    def test_cap_limitation_is_exact_and_documented(self):
        """
        cap=10, window=18: a duplicate of entry 3 (older than the newest 10)
        is MISSED by the capped scan but caught by brute force. Probes for
        entries inside the newest-10 window still match identically.
        """
        v_old = self.embedder.embed_text(self.fillers[3])   # oldest third
        v_new = self.embedder.embed_text(self.fillers[15])  # inside newest 10

        # Old canonical: brute catches, capped misses — the documented trade-off.
        is_dup_b, canon_b, score_b = self.brute.check_semantic_duplicate("lim_old_b", v_old, title=self.fillers[3])
        is_dup_c, canon_c, _ = self.capped.check_semantic_duplicate("lim_old_c", v_old, title=self.fillers[3])
        self.assertTrue(is_dup_b, "brute force must catch the old duplicate")
        self.assertEqual(canon_b, "fill_3")
        self.assertFalse(is_dup_c,
            "capped scan missed-by-design failed: it caught an entry outside its newest-N window")

        # New canonical: capped must still catch (entry 15 is inside newest 10 of 18+1).
        is_dup_c2, canon_c2, _ = self.capped.check_semantic_duplicate("lim_new_c", v_new, title=self.fillers[15])
        self.assertTrue(is_dup_c2, "capped scan must catch duplicates inside its window")
        self.assertEqual(canon_c2, "fill_15")


class TestEmbeddingModelVersioning(unittest.TestCase):
    """Backfill migration + explicit opt-in model filter (review round 2)."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "ver.duckdb")
        from lakehouse.db import LakehouseManager
        self.lh = LakehouseManager(db_path=self.db_path)

    def tearDown(self):
        self.lh.close()
        self.tmp.cleanup()

    def _event(self, eid, title):
        from models.event import CommonEvent
        e = CommonEvent(id=eid, source="RSS", title=title,
                        content_snippet="", published_at=datetime.now(timezone.utc))
        e._embedding = get_embedder_cached().embed_text(title)
        return e

    def test_backfill_stamps_null_model_rows(self):
        """A pre-existing row with no model stamp must be backfilled at init
        (only one model has ever produced embeddings in this pipeline)."""
        from lakehouse.db import LakehouseManager
        from models.embedder import MODEL_NAME
        from models.event import CommonEvent

        e = CommonEvent(id="backfill_1", source="RSS", title="Backfill stamp test headline",
                        content_snippet="", published_at=datetime.now(timezone.utc))
        e._embedding = get_embedder_cached().embed_text(e.title)
        self.lh.insert_news_batch([e])
        self.lh.conn.execute("UPDATE silver_financial_news SET embedding_model = NULL WHERE id = 'backfill_1'")

        self.lh.close()
        self.lh = LakehouseManager(db_path=self.db_path)  # re-init runs backfill

        stamped = self.lh.conn.execute(
            "SELECT embedding_model FROM silver_financial_news WHERE id = 'backfill_1'").fetchone()[0]
        self.assertEqual(stamped, MODEL_NAME, "pre-existing row was not backfilled")

    def test_model_filter_is_explicit_opt_in(self):
        """No filter -> all embedded rows; explicit filter -> only that model.
        NULL-stamped rows must never silently vanish from unfiltered search."""
        from models.event import CommonEvent

        e1 = CommonEvent(id="mv_fed", source="RSS", title="Fed holds benchmark rate steady",
                         content_snippet="", published_at=datetime.now(timezone.utc))
        e1._embedding = get_embedder_cached().embed_text(e1.title)
        e2 = CommonEvent(id="mv_other", source="RSS", title="Fed holds benchmark rate steady",
                         content_snippet="", published_at=datetime.now(timezone.utc))
        e2._embedding = get_embedder_cached().embed_text(e2.title)
        self.lh.insert_news_batch([e1, e2])
        self.lh.conn.execute("UPDATE silver_financial_news SET embedding_model = 'future-model' WHERE id = 'mv_other'")

        q = get_embedder_cached().embed_text("Fed holds benchmark rate steady").tolist()
        unfiltered = self.lh.query_similar_news(query_embedding=q)
        self.assertEqual({r["id"] for r in unfiltered}, {"mv_fed", "mv_other"},
            "unfiltered search must return all embedded rows regardless of model stamp")

        filtered = self.lh.query_similar_news(query_embedding=q, embedding_model="future-model")
        self.assertEqual({r["id"] for r in filtered}, {"mv_other"},
            "explicit model filter must restrict to the requested model")


class TestEntityExtractor(unittest.TestCase):
    """Alias-dictionary extractor: coverage, exclusions, multi-entity output."""

    def test_institutions_extract(self):
        from utils.entity_extract import extract_financial_entities as ex
        self.assertEqual(ex("Federal Reserve raises interest rates by 25 basis points"), ["FED"])
        self.assertEqual(ex("European Central Bank raises rates"), ["ECB"])
        self.assertEqual(ex("OPEC announces production cuts"), ["OPEC"])

    def test_ambiguous_aliases_do_not_fire(self):
        from utils.entity_extract import extract_financial_entities as ex
        self.assertNotIn("BP", ex("Fed raises rates by 50 bp in March"))
        self.assertNotIn("JPM", ex("Police chase suspect in downtown"))
        self.assertNotIn("FDA", ex("Novartis drug wins FDA approval"))

    def test_all_caps_noise_excluded(self):
        from utils.entity_extract import extract_financial_entities as ex
        self.assertEqual(ex("US inflation cools to 2.9% in July"), [])
        self.assertEqual(ex("Tech CEO pay and ETF flows dominate the IPO market"), [])

    def test_multi_entity_output(self):
        from utils.entity_extract import extract_financial_entities as ex
        self.assertEqual(ex("Goldman upgrades Ford on margin recovery"), ["F", "GS"])

    def test_ticker_fallback_still_works(self):
        from utils.entity_extract import extract_financial_entities as ex
        self.assertIn("PLTR", ex("PLTR spikes after earnings"))
        self.assertIn("TSLA", ex("Bought $TSLA calls before the print"))


def redis_ping() -> bool:
    import redis
    try:
        return bool(redis.Redis(host="localhost", port=6379, db=15, socket_timeout=3).ping())
    except Exception:
        return False


_EMBEDDER_CACHE = {}

def get_embedder_cached():
    from models.embedder import get_embedder
    if "e" not in _EMBEDDER_CACHE:
        _EMBEDDER_CACHE["e"] = get_embedder()
    return _EMBEDDER_CACHE["e"]


if __name__ == "__main__":
    unittest.main()
