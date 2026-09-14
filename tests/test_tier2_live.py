"""Live Redis tests use unique prefixed keys and never flush a database."""
import json
import unittest
import uuid

import redis
from utils.redis_cache import RedisDeduplicator
from config import DEDUP_RESERVATION_SECONDS


class NamespacedRedis:
    def __init__(self, client, prefix, keys=None):
        self.client = client
        self.prefix = prefix
        self.keys = keys if keys is not None else set()

    def pipeline(self):
        return NamespacedRedis(self.client.pipeline(), self.prefix, self.keys)

    def execute(self):
        return self.client.execute()

    def __getattr__(self, name):
        def command(key, *args, **kwargs):
            scoped = self.prefix + key
            self.keys.add(scoped)
            if name == 'delete':
                args = tuple(self.prefix + k for k in args)
                self.keys.update(args)
            return getattr(self.client, name)(scoped, *args, **kwargs)
        return command


class TestTier2Live(unittest.TestCase):
    def setUp(self):
        self.dedup = RedisDeduplicator(db=15)
        self.raw = self.dedup.client
        try:
            self.raw.ping()
        except redis.RedisError:
            self.skipTest('Redis unavailable')
        self.scoped = NamespacedRedis(self.raw, f'test:{uuid.uuid4()}:'.encode())
        self.dedup.client = self.scoped

    def tearDown(self):
        if hasattr(self, 'scoped') and self.scoped.keys:
            self.raw.delete(*self.scoped.keys)

    def test_cross_source_rewrites_share_original_cluster(self):
        original = 'Federal Reserve raises interest rates by 25 basis points to battle inflation'
        rewrite = original.replace('battle', 'curb')
        self.assertEqual(self.dedup.check_near_duplicate('original', original, 'RSS'), (False, None))
        self.assertEqual(self.dedup.check_near_duplicate('rewrite', rewrite, 'GDELT'), (True, 'original'))
        # Exact match to the rewrite wins, but must return the original root.
        self.assertEqual(self.dedup.check_near_duplicate('third', rewrite, 'RSS'), (True, 'original'))
        record = json.loads(self.scoped.get(b'lsh:news:v2:event:third'))
        self.assertEqual(record['canonical_id'], 'original')
        self.assertGreater(self.raw.ttl(self.scoped.prefix + b'lsh:news:v2:event:third'), 0)

    def test_long_opposite_and_negated_headlines_remain_distinct(self):
        title = ('Tesla quarterly revenue beats Wall Street estimates as investors review '
                 'the latest financial results and management commentary on global demand '
                 'production margins and next year outlook')
        self.dedup.check_near_duplicate('beat', title, 'RSS')
        self.assertEqual(self.dedup.check_near_duplicate('miss', title.replace('beats', 'misses'), 'GDELT'), (False, None))
        # "Did not beat" agrees with the negative "misses" story, while
        # remaining separate from the original positive "beats" cluster.
        self.assertEqual(self.dedup.check_near_duplicate('negated', title.replace('beats', 'did not beat'), 'RSS'), (True, 'miss'))

    def test_expired_record_cannot_authorize_merge(self):
        self.dedup.check_near_duplicate('original', 'example headline')
        self.raw.delete(self.scoped.prefix + b'lsh:news:v2:event:original')
        self.assertEqual(self.dedup.check_near_duplicate('new', 'example headline'), (False, None))

    def test_reservation_promoted_only_after_durable_staging(self):
        self.assertFalse(self.dedup.is_exact_duplicate('id', 'RSS', reserve_for_delivery=True))
        scoped_key = self.scoped.prefix + b'dedup:exact:rss:id'
        self.assertEqual(self.raw.get(scoped_key), b'pending')
        self.assertLessEqual(self.raw.ttl(scoped_key), DEDUP_RESERVATION_SECONDS)
        self.dedup.confirm_staged('id', 'RSS')
        self.assertEqual(self.raw.get(scoped_key), b'1')
        self.assertGreater(self.raw.ttl(scoped_key), DEDUP_RESERVATION_SECONDS)

    def test_failed_staging_releases_exact_and_fuzzy_records(self):
        self.dedup.is_exact_duplicate('id', 'RSS', reserve_for_delivery=True)
        self.dedup.check_near_duplicate('id', 'example headline')
        self.dedup.release_unstaged('id', 'RSS')
        self.assertFalse(self.dedup.is_exact_duplicate('id', 'RSS', reserve_for_delivery=True))
        self.assertEqual(self.dedup.check_near_duplicate('second', 'example headline'), (False, None))


if __name__ == '__main__':
    unittest.main()
