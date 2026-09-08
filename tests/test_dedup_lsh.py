import unittest
import time
from utils.redis_cache import RedisDeduplicator


class TestTwoTierDeduplication(unittest.TestCase):
    def setUp(self):
        # Use isolated test database (db=15) to avoid polluting or conflicting with production db=0
        self.dedup = RedisDeduplicator(db=15)
        if not self.dedup.ping():
            self.skipTest("Redis container is not reachable")
        self.dedup.client.flushdb()

    def tearDown(self):
        try:
            self.dedup.client.flushdb()
        except Exception:
            pass

    def test_tier1_exact_match(self):
        event_id = f"test_exact_{int(time.time() * 1000)}"
        title = "U.S. Inflation Rises to 3.2% in August"

        # First pass: Should NOT be duplicate
        is_exact, is_near, canon_id = self.dedup.check_dedup(event_id, title, source="RSS")
        self.assertFalse(is_exact)
        self.assertFalse(is_near)
        self.assertIsNone(canon_id)

        # Immediate second pass: Should be EXACT duplicate
        is_exact2, is_near2, _ = self.dedup.check_dedup(event_id, title, source="RSS")
        self.assertTrue(is_exact2)

    def test_tier2_near_duplicate_syndication(self):
        base_id = f"test_wire1_{int(time.time() * 1000)}"
        wire_id = f"test_wire2_{int(time.time() * 1000) + 1}"

        title1 = "Federal Reserve raises interest rates by 25 basis points to battle inflation"
        title2 = "Federal Reserve raises interest rates by 25 basis points to curb inflation"

        # Story 1 (Canonical)
        is_exact1, is_near1, canon1 = self.dedup.check_dedup(base_id, title1, source="RSS")
        self.assertFalse(is_exact1)
        self.assertFalse(is_near1)

        # Story 2 (Syndicated rewrite from different source / wire)
        is_exact2, is_near2, canon2 = self.dedup.check_dedup(wire_id, title2, source="RSS")
        self.assertFalse(is_exact2)  # Different URL/ID, so exact match is false
        self.assertTrue(is_near2)    # Semantic / MinHash LSH catches near-duplicate!
        self.assertEqual(canon2, base_id)  # Correctly points to original canonical story

    def test_polarity_protection_beats_vs_misses(self):
        base_id = f"test_polar1_{int(time.time() * 1000)}"
        polar_id = f"test_polar2_{int(time.time() * 1000) + 1}"

        title_beat = "Tesla quarterly revenue beats Wall Street estimates"
        title_miss = "Tesla quarterly revenue misses Wall Street estimates"

        # Headline A (Beat)
        is_exact1, is_near1, _ = self.dedup.check_dedup(base_id, title_beat, source="RSS")
        self.assertFalse(is_exact1)
        self.assertFalse(is_near1)

        # Headline B (Miss) -> MUST NOT be flagged as duplicate despite 80%+ identical words
        is_exact2, is_near2, _ = self.dedup.check_dedup(polar_id, title_miss, source="RSS")
        self.assertFalse(is_exact2)
        self.assertFalse(is_near2, "Polarity guard failed: 'misses' was wrongly merged with 'beats'!")

    def test_sec_filing_bypass(self):
        # SEC filings share standard boilerplate titles, so fuzzy dedup must be bypassed
        sec_id1 = f"0000320193-26-{int(time.time()) % 10000:04d}"
        sec_id2 = f"0000789019-26-{int(time.time()) % 10000:04d}"

        title1 = "[8-K] Apple Inc. Form 8-K Current Report"
        title2 = "[8-K] Microsoft Corp. Form 8-K Current Report"

        self.dedup.check_dedup(sec_id1, title1, source="SEC")
        is_exact, is_near, _ = self.dedup.check_dedup(sec_id2, title2, source="SEC")
        self.assertFalse(is_exact)
        self.assertFalse(is_near, "SEC filings must never be flagged as near-duplicates via LSH")


if __name__ == "__main__":
    unittest.main()
