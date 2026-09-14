"""Tier 2 precision-gate tests, independent of Redis and embedding downloads."""
import json
import unittest
from unittest.mock import MagicMock, patch

from datasketch import MinHash
from utils.redis_cache import RedisDeduplicator


class TestTier2ExactJaccard(unittest.TestCase):
    def setUp(self):
        self.dedup = RedisDeduplicator()
        self.dedup.client = MagicMock()

    def run_candidates(self, title, payloads):
        """Force LSH retrieval so these tests isolate the acceptance gate."""
        lookup = MagicMock()
        lookup.execute.return_value = [set(payloads)] * self.dedup.num_bands
        readers = []
        ordered = sorted(payloads)
        for start in range(0, len(ordered), 256):
            reader = MagicMock()
            reader.execute.return_value = [payloads[k] for k in ordered[start:start + 256]]
            readers.append(reader)
        writer = MagicMock()
        self.dedup.client.pipeline.side_effect = [lookup, *readers, writer]
        # An accidental regression to estimated Jaccard would fail the result
        # assertion (the production fail-open handler catches this exception).
        with patch.object(MinHash, 'jaccard', side_effect=AssertionError('Must use exact sets')):
            result = self.dedup.check_near_duplicate('incoming', title)
        return result, writer

    def payload(self, title):
        return json.dumps({'title': title, 'shingles': sorted(self.dedup._create_shingles(title)), 'canonical_id': 'original'}).encode()

    def test_exact_threshold_below_at_and_above(self):
        for length, accepted in [(10, False), (11, True), (12, True)]:
            with self.subTest(words=length):
                tokens = [f'word{i}' for i in range(length)]
                title = ' '.join(tokens)
                tokens[4] = 'replacement'
                candidate = ' '.join(tokens)
                # One interior replacement changes three of 2*n-1 shingles.
                expected_score = (2 * length - 4) / (2 * length + 2)
                self.assertEqual(self.dedup._exact_jaccard(
                    self.dedup._create_shingles(title),
                    self.dedup._create_shingles(candidate)), expected_score)
                result, _ = self.run_candidates(title, {b'original': self.payload(candidate)})
                self.assertEqual(result, (True, 'original') if accepted else (False, None))

    def test_all_candidates_checked_and_ties_deterministic(self):
        payloads = {f'a{i:03}'.encode(): self.payload('unrelated headline') for i in range(270)}
        payloads[b'z-first'] = self.payload('example headline').replace(b'original', b'z-first')
        payloads[b'z-second'] = self.payload('example headline').replace(b'original', b'z-second')
        result, _ = self.run_candidates('example headline', payloads)
        self.assertEqual(result, (True, 'z-first'))

    def test_missing_and_invalid_candidates_do_not_hide_valid_match(self):
        result, _ = self.run_candidates('example headline', {
            b'a-legacy-or-expired': None,
            b'b-corrupt': b'not json',
            b'c-wrong-shape': b'{"title": "example headline"}',
            b'd-valid': self.payload('example headline'),
        })
        self.assertEqual(result, (True, 'original'))

    def test_signature_only_candidate_cannot_merge(self):
        result, writer = self.run_candidates('example headline', {b'legacy': None})
        self.assertEqual(result, (False, None))
        self.assertTrue(writer.execute.called)

    def test_new_and_duplicate_payloads_have_window_ttl(self):
        for candidates in ({}, {b'original': self.payload('Example headline')}):
            with self.subTest(duplicate=bool(candidates)):
                _, writer = self.run_candidates('Example headline', candidates)
                key, saved = writer.set.call_args.args
                self.assertEqual(key, b'lsh:news:v2:event:incoming')
                self.assertEqual(set(json.loads(saved)['shingles']), {'example', 'headline', 'example headline'})
                self.assertEqual(writer.set.call_args.kwargs, {'ex': self.dedup.lsh_ttl})

    def test_empty_normalized_headlines_and_sec_bypass_redis(self):
        for title, source in [('!!!', 'RSS'), ('  ', 'RSS'), ('Filing title', 'SEC')]:
            self.assertEqual(self.dedup.check_near_duplicate('id', title, source), (False, None))
        self.dedup.client.pipeline.assert_not_called()

    def test_normalization_and_shared_shingle_representation(self):
        shingles = self.dedup._create_shingles('Tesla BEATS, estimates!')
        self.assertTrue({'tesla', 'beats', 'estimates', 'tesla beats', 'beats estimates'} <= shingles)
        self.assertIn('__POLAR_ANCHOR_1__beat', shingles)
        self.assertEqual(self.dedup._exact_jaccard(set(), set()), 0.0)
        self.assertTrue((self.dedup._create_minhash('Tesla BEATS, estimates!').hashvalues ==
                         self.dedup._minhash_shingles(shingles).hashvalues).all())


if __name__ == '__main__':
    unittest.main()
