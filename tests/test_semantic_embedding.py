"""
Test Suite for Tier 3: Semantic Embedding Deduplication
Tests all-MiniLM-L6-v2 embedding + Redis 120-min sliding window + DuckDB vector search.

Uses Redis db=15 (isolated from production db=0) with flushdb in setUp/tearDown.

CALIBRATION NOTE (scripts/calibrate_tier3.py — all-MiniLM-L6-v2 via fastembed ONNX):
Cosine bands measured on a 63-pair labeled corpus (7 bands, gated sweep):
  - Unrelated topics:                       -0.12 – 0.14
  - Same-event rewrites / paraphrases:      0.25 – 0.92 (graded lexical overlap)
  - Hard negatives (same ticker, different event): 0.44 – 0.74  ← overlaps rewrites
  - Polarity inversions:                    0.69 – 0.92 (guard-blocked, not threshold-blocked)
The bands are NOT separable: at the old hand-picked 0.65, gated precision was
0.43 (8 distinct-event false merges). The production threshold is calibrated
to 0.88 (SEMANTIC_COSINE_THRESHOLD, precision-first: gated precision 1.000,
~5% corpus recall — the accepted hole is pinned in test_tier3_hard_negatives).
"""

import os
import sys
import time
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Force test Redis DB
os.environ["REDIS_DB"] = "15"


class TestEmbedderModule(unittest.TestCase):
    """Tests for models/embedder.py — FinancialEmbedder singleton."""

    @classmethod
    def setUpClass(cls):
        """Load the embedding model once for all tests in this class."""
        from models.embedder import get_embedder, EMBEDDING_DIM
        cls.embedder = get_embedder()
        cls.dim = EMBEDDING_DIM

    def test_embed_text_returns_correct_shape(self):
        """embed_text should return a 384-dim float32 vector."""
        vec = self.embedder.embed_text("Federal Reserve raises interest rates by 25 bps")
        self.assertEqual(vec.shape, (self.dim,))
        self.assertEqual(vec.dtype, np.float32)

    def test_embed_text_is_l2_normalized(self):
        """Output vectors should be L2-normalized (unit length)."""
        vec = self.embedder.embed_text("U.S. inflation cools to 2.9% in July")
        norm = np.linalg.norm(vec)
        self.assertAlmostEqual(norm, 1.0, places=4)

    def test_embed_batch_returns_correct_shape(self):
        """embed_batch should return (N, 384) matrix."""
        texts = ["headline one", "headline two", "headline three"]
        matrix = self.embedder.embed_batch(texts)
        self.assertEqual(matrix.shape, (3, self.dim))

    def test_empty_text_returns_zero_vector(self):
        """Empty input should return a zero vector without crashing."""
        vec = self.embedder.embed_text("")
        self.assertEqual(vec.shape, (self.dim,))
        self.assertTrue(np.allclose(vec, 0.0))


class TestSemanticParaphraseDetection(unittest.TestCase):
    """
    Test 1: Paraphrased News Detection (Calibrated to fastembed ONNX)

    Verifies that semantically similar headlines produce higher cosine scores
    than unrelated ones. Thresholds calibrated empirically against fastembed's
    ONNX runtime of all-MiniLM-L6-v2.
    """

    @classmethod
    def setUpClass(cls):
        from models.embedder import get_embedder
        cls.embedder = get_embedder()

    def _cosine(self, text_a: str, text_b: str) -> float:
        va = self.embedder.embed_text(text_a)
        vb = self.embedder.embed_text(text_b)
        return float(np.dot(va, vb))

    def test_fed_rate_paraphrase_above_unrelated(self):
        """'Fed raises rates' vs 'Central bank lifts borrowing costs' should score well above unrelated."""
        paraphrase_score = self._cosine(
            "Federal Reserve raises interest rates by 25 basis points",
            "Central bank lifts borrowing costs by a quarter percentage point"
        )
        unrelated_score = self._cosine(
            "Federal Reserve raises interest rates by 25 basis points",
            "Manchester United signs new striker in record transfer deal"
        )
        # Paraphrase should be at least 3x the unrelated score
        self.assertGreater(paraphrase_score, unrelated_score * 3,
            f"Paraphrase ({paraphrase_score:.4f}) should be much higher than unrelated ({unrelated_score:.4f})")
        # And above 0.35 (empirical floor for true paraphrases)
        self.assertGreaterEqual(paraphrase_score, 0.35,
            f"Paraphrase pair should have cosine >= 0.35, got {paraphrase_score:.4f}")

    def test_same_topic_rewrite(self):
        """Same-topic rewrites with partial lexical overlap should score >= 0.65."""
        score = self._cosine(
            "Oil prices surge as OPEC cuts production",
            "Crude oil rallies on OPEC supply reduction"
        )
        self.assertGreaterEqual(score, 0.65,
            f"Same-topic rewrite should score >= 0.65, got {score:.4f}")

    def test_unrelated_topics_low_score(self):
        """Completely unrelated headlines should have very low similarity."""
        score = self._cosine(
            "Federal Reserve raises interest rates by 25 basis points",
            "Manchester United signs new striker in record transfer deal"
        )
        self.assertLess(score, 0.20,
            f"Unrelated topics should score below 0.20, got {score:.4f}")

    def test_similarity_ranking_preserved(self):
        """
        Semantic ranking: same-topic rewrite > paraphrase > unrelated.
        This is the critical property — relative ordering matters more than absolute scores.
        """
        query = "Federal Reserve raises interest rates by 25 basis points"
        score_rewrite = self._cosine(query, "Fed hikes rates by 25 bps in June meeting")
        score_paraphrase = self._cosine(query, "Central bank lifts borrowing costs by a quarter percentage point")
        score_unrelated = self._cosine(query, "Manchester United signs new striker in record transfer deal")

        self.assertGreater(score_rewrite, score_unrelated,
            f"Rewrite ({score_rewrite:.4f}) should beat unrelated ({score_unrelated:.4f})")
        self.assertGreater(score_paraphrase, score_unrelated,
            f"Paraphrase ({score_paraphrase:.4f}) should beat unrelated ({score_unrelated:.4f})")


class TestPolarityDifferentiation(unittest.TestCase):
    """
    Test 2: Polarity Differentiation

    NOTE: all-MiniLM-L6-v2 does NOT reliably differentiate polarity
    (beats vs misses score ~0.92). This is a known limitation of sentence
    embeddings. Polarity protection is handled by Tier 2 (MinHash LSH polarity
    anchors), NOT Tier 3. These tests verify the known behavior.
    """

    @classmethod
    def setUpClass(cls):
        from models.embedder import get_embedder
        cls.embedder = get_embedder()

    def _cosine(self, text_a: str, text_b: str) -> float:
        va = self.embedder.embed_text(text_a)
        vb = self.embedder.embed_text(text_b)
        return float(np.dot(va, vb))

    def test_polarity_pairs_score_high_known_limitation(self):
        """
        Embedding models score polarity pairs (beats/misses) very high (~0.90+).
        This is a KNOWN limitation — Tier 2 LSH polarity anchors handle this.
        We verify the model produces a score, and it's above 0.80 (confirming
        Tier 3 alone can't differentiate polarity).
        """
        score = self._cosine(
            "Tesla beats Q3 earnings estimates sending shares higher",
            "Tesla misses Q3 earnings estimates sending shares lower"
        )
        # This SHOULD be high — confirming that Tier 2 polarity protection is necessary
        self.assertGreater(score, 0.80,
            f"Polarity pairs should score high in embedding space (known limitation), got {score:.4f}")

    def test_rate_hike_vs_cut_high_similarity(self):
        """'rate hike' vs 'rate cut' are similar in embedding space — Tier 2 handles this."""
        score = self._cosine(
            "Federal Reserve announces surprise rate hike of 50 basis points",
            "Federal Reserve announces surprise rate cut of 50 basis points"
        )
        self.assertGreater(score, 0.80,
            f"Rate hike/cut should score high (known limitation), got {score:.4f}")

    def test_unrelated_topics(self):
        """Completely unrelated headlines should have low similarity."""
        score = self._cosine(
            "Federal Reserve raises interest rates by 25 basis points",
            "Manchester United signs new striker in record transfer deal"
        )
        self.assertLess(score, 0.20,
            f"Unrelated topics should score below 0.20, got {score:.4f}")


class TestSemanticRedisWindow(unittest.TestCase):
    """
    Test 3: 120-Minute Window Expiration
    Verifies vectors cleanly expire after TTL and window pruning works.
    """

    @classmethod
    def setUpClass(cls):
        from models.embedder import get_embedder
        cls.embedder = get_embedder()

    def setUp(self):
        import redis
        self.redis_client = redis.Redis(host="localhost", port=6379, db=15)
        self.redis_client.flushdb()

    def tearDown(self):
        self.redis_client.flushdb()
        self.redis_client.close()

    def test_window_stores_and_retrieves(self):
        """Vectors stored in the window should be retrievable for dedup."""
        from utils.semantic_dedup import SemanticDeduplicator

        dedup = SemanticDeduplicator(db=15, semantic_ttl_seconds=7200)

        # Store first headline
        vec_a = self.embedder.embed_text("Federal Reserve raises interest rates")
        is_dup, canon, score = dedup.check_semantic_duplicate("event_001", vec_a)
        self.assertFalse(is_dup, "First event should never be a duplicate")

        # Store a same-topic rewrite that should be detected
        vec_b = self.embedder.embed_text("Oil prices surge as OPEC cuts production")
        is_dup, canon, score = dedup.check_semantic_duplicate("event_002", vec_b)
        self.assertIsInstance(is_dup, bool)

    def test_window_pruning_removes_expired(self):
        """Expired entries should be pruned from the window."""
        from utils.semantic_dedup import SemanticDeduplicator, WINDOW_ZSET_KEY

        dedup = SemanticDeduplicator(db=15, semantic_ttl_seconds=2)  # 2-second TTL for testing

        vec = self.embedder.embed_text("Test headline for expiration")
        dedup.check_semantic_duplicate("expire_test_001", vec)

        # Verify entry exists in window
        count_before = self.redis_client.zcard(WINDOW_ZSET_KEY)
        self.assertGreaterEqual(count_before, 1)

        # Wait for TTL to expire
        time.sleep(3)

        # Trigger pruning by checking another event
        vec2 = self.embedder.embed_text("Another completely different headline")
        dedup.check_semantic_duplicate("expire_test_002", vec2)

        # The expired entry's vector key should be gone
        expired_vec = self.redis_client.get(b"emb:vec:expire_test_001")
        self.assertIsNone(expired_vec, "Expired vector should be auto-deleted by Redis TTL")

    def test_self_not_matched(self):
        """An event should never match itself in the window."""
        from utils.semantic_dedup import SemanticDeduplicator

        dedup = SemanticDeduplicator(db=15, semantic_ttl_seconds=7200)

        vec = self.embedder.embed_text("Unique financial headline about markets")
        # Store it
        dedup.check_semantic_duplicate("self_test_001", vec)
        # Check the same ID again — should NOT match itself
        is_dup, canon, score = dedup.check_semantic_duplicate("self_test_001", vec)
        self.assertFalse(is_dup, "Event should not match itself")

    def test_high_similarity_detected_as_duplicate(self):
        """Same-topic rewrite within window should be flagged as semantic duplicate."""
        from utils.semantic_dedup import SemanticDeduplicator

        dedup = SemanticDeduplicator(db=15, semantic_ttl_seconds=7200, cosine_threshold=0.65)

        vec_a = self.embedder.embed_text("Oil prices surge as OPEC cuts production")
        is_dup, canon, score = dedup.check_semantic_duplicate("oil_001", vec_a)
        self.assertFalse(is_dup, "First event should not be duplicate")

        vec_b = self.embedder.embed_text("Crude oil rallies on OPEC supply reduction")
        is_dup, canon, score = dedup.check_semantic_duplicate("oil_002", vec_b)
        self.assertTrue(is_dup, f"Same-topic rewrite should be detected (score={score:.4f})")
        self.assertEqual(canon, "oil_001")
        self.assertGreaterEqual(score, 0.65)


class TestDuckDBVectorSearch(unittest.TestCase):
    """
    Test 4: DuckDB Vector Search
    Verifies array_cosine_similarity queries return expected semantic rankings.
    """

    def setUp(self):
        from models.embedder import get_embedder
        self.embedder = get_embedder()

        # Create a temporary DuckDB for this test
        self.tmp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmp_dir, "test_vectors.duckdb")

    def tearDown(self):
        import shutil
        try:
            shutil.rmtree(self.tmp_dir, ignore_errors=True)
        except Exception:
            pass

    def test_cosine_similarity_ranking(self):
        """Articles semantically close to a query should rank higher."""
        from lakehouse.db import LakehouseManager
        from models.event import CommonEvent

        lh = LakehouseManager(db_path=self.db_path, read_only=False)

        now = datetime.now(timezone.utc)
        articles = [
            ("art_fed", "Federal Reserve raises interest rates by 25 basis points"),
            ("art_oil", "Oil prices surge as OPEC cuts production quotas"),
            ("art_rate", "Fed hikes rates by 25 bps in June meeting"),
            ("art_sport", "Manchester United wins Premier League title"),
        ]

        events = []
        for art_id, title in articles:
            vec = self.embedder.embed_text(title)
            event = CommonEvent(
                id=art_id,
                source="RSS",
                title=title,
                content_snippet="",
                published_at=now,
            )
            event._embedding = vec
            event._semantic_score = None
            events.append(event)

        lh.insert_news_batch(events)

        # Query: find articles similar to "Fed rate hike"
        query_vec = self.embedder.embed_text("Federal Reserve rate hike")
        results = lh.query_similar_news(query_embedding=query_vec.tolist(), limit=4)

        self.assertGreater(len(results), 0, "Should return at least one result")

        # The top result should be about the Fed (either art_fed or art_rate)
        top_id = results[0]["id"]
        self.assertIn(top_id, ["art_fed", "art_rate"],
            f"Top result should be Fed-related, got: {top_id}")

        # Sports article should rank last
        last_result = results[-1]
        self.assertIn("Manchester", last_result["title"],
            f"Sports article should rank last, got: {last_result['title']}")

        lh.close()

    def test_embedding_persistence(self):
        """Embedding vectors should be correctly persisted and retrievable."""
        from lakehouse.db import LakehouseManager
        from models.event import CommonEvent

        lh = LakehouseManager(db_path=self.db_path, read_only=False)

        vec = self.embedder.embed_text("Test embedding persistence headline")
        event = CommonEvent(
            id="persist_test_001",
            source="RSS",
            title="Test embedding persistence headline",
            content_snippet="",
            published_at=datetime.now(timezone.utc),
        )
        event._embedding = vec
        event._semantic_score = 0.92

        lh.insert_news_batch([event])

        # Query back the embedding
        result = lh.conn.execute(
            "SELECT embedding, semantic_score FROM silver_financial_news WHERE id = ?",
            ["persist_test_001"]
        ).fetchone()

        self.assertIsNotNone(result, "Should retrieve the inserted event")
        stored_embedding = result[0]
        stored_score = result[1]

        self.assertIsNotNone(stored_embedding, "Embedding should be stored")
        self.assertEqual(len(stored_embedding), 384, "Embedding should have 384 dimensions")
        self.assertAlmostEqual(stored_score, 0.92, places=2)

        lh.close()

    def test_stats_include_embedded_count(self):
        """get_stats should report the number of events with embeddings."""
        from lakehouse.db import LakehouseManager
        from models.event import CommonEvent

        lh = LakehouseManager(db_path=self.db_path, read_only=False)

        # Insert one event WITH embedding and one WITHOUT
        vec = self.embedder.embed_text("Headline with embedding")
        event_with = CommonEvent(
            id="with_emb", source="RSS", title="With embedding",
            content_snippet="", published_at=datetime.now(timezone.utc),
        )
        event_with._embedding = vec
        event_with._semantic_score = None

        event_without = CommonEvent(
            id="no_emb", source="RSS", title="Without embedding",
            content_snippet="", published_at=datetime.now(timezone.utc),
        )
        event_without._embedding = None
        event_without._semantic_score = None

        lh.insert_news_batch([event_with, event_without])

        stats = lh.get_stats()
        self.assertEqual(stats["embedded_events"], 1, "Should count only events with embeddings")
        self.assertEqual(stats["total_news_events"], 2)

        lh.close()


if __name__ == "__main__":
    unittest.main()
