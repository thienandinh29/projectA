import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from lakehouse.db import LakehouseManager
from lakehouse.sync import parse_event_payload
from models.event import CommonEvent
from tests import test_tier2_exact_jaccard as exact_tests
from utils.redis_cache import RedisDeduplicator


class TestTier2Polarity(unittest.TestCase):
    setUp = exact_tests.TestTier2ExactJaccard.setUp
    run_candidates = exact_tests.TestTier2ExactJaccard.run_candidates
    payload = exact_tests.TestTier2ExactJaccard.payload
    suffix = (' as investors review the latest financial results and management '
              'commentary on global demand production margins and next year outlook')

    def test_high_similarity_opposites_rejected(self):
        pairs = [('Tesla revenue beats estimates', 'Tesla revenue misses estimates'),
                 ('Federal Reserve raises interest rates', 'Federal Reserve cuts interest rates'),
                 ('Tesla revenue beat estimates', 'Tesla revenue did not beat estimates'),
                 ('Tesla shares rose after earnings', 'Tesla shares fell after earnings')]
        for left, right in pairs:
            with self.subTest(left=left):
                left += self.suffix
                right += self.suffix
                self.assertGreaterEqual(self.dedup._exact_jaccard(
                    self.dedup._create_shingles(left), self.dedup._create_shingles(right)), .75)
                result, _ = self.run_candidates(left, {b'original': self.payload(right)})
                self.assertEqual(result, (False, None))

    def test_shared_negative_context_still_matches(self):
        left = 'Oil surges as stocks slide' + self.suffix
        right = 'Oil rallies as stocks slide' + self.suffix
        result, _ = self.run_candidates(left, {b'original': self.payload(right)})
        self.assertEqual(result, (True, 'original'))

    def test_duplicate_candidate_points_to_original(self):
        result, writer = self.run_candidates('example headline', {
            b'duplicate': self.payload('example headline')})
        self.assertEqual(result, (True, 'original'))
        import json
        self.assertEqual(json.loads(writer.set.call_args.args[1])['canonical_id'], 'original')


class TestEventHandoff(unittest.TestCase):
    def test_json_storage_roundtrip_and_pit(self):
        published = datetime(2026, 9, 1, 14, tzinfo=timezone.utc)
        ingested = published + timedelta(minutes=5)
        event = CommonEvent(id='rewrite', source='GDELT', title='Tesla beats estimates',
                            published_at=published, ingested_time=ingested,
                            event_time=published - timedelta(minutes=1),
                            content_full='Full article body', content_snippet='Summary',
                            embedding=[1.0] + [0.0] * 383, semantic_score=.91,
                            is_near_duplicate=True, canonical_cluster_id='original',
                            tickers_mentioned=['TSLA'], metadata={'wire': 'test'})
        parsed = parse_event_payload(event.to_json_bytes())
        self.assertEqual(parsed.model_dump(), event.model_dump())
        with tempfile.TemporaryDirectory() as tmp:
            lake = LakehouseManager(str(Path(tmp) / 'lake.duckdb'))
            try:
                lake.insert_news_batch([parsed])
                self.assertEqual(lake.query_news_as_of(published + timedelta(minutes=3),
                                                      exclude_duplicates=False), [])
                row = lake.conn.execute('SELECT event_time, content_full, embedding, '
                                        'semantic_score, canonical_cluster_id FROM silver_financial_news').fetchone()
                self.assertEqual(row[0], event.event_time)
                self.assertEqual(row[1], event.content_full)
                self.assertEqual(list(row[2]), event.embedding)
                self.assertAlmostEqual(row[3], event.semantic_score, places=6)
                self.assertEqual(row[4], 'original')
            finally:
                lake.close()

    def test_event_results_remain_independent_between_threads(self):
        dedup = RedisDeduplicator()
        def semantic(event_id, title, tickers):
            return [float(event_id)] * 384, False, None, 0.0
        with patch.object(dedup, 'is_exact_duplicate', return_value=False), \
             patch.object(dedup, 'check_near_duplicate', return_value=(False, None)), \
             patch.object(dedup, '_check_semantic', side_effect=semantic):
            with ThreadPoolExecutor(max_workers=4) as pool:
                results = list(pool.map(lambda i: dedup.check_dedup_result(str(i), 'headline'), range(20)))
        for i, result in enumerate(results):
            self.assertEqual(result.embedding, [float(i)] * 384)


if __name__ == '__main__':
    unittest.main()
