"""Collection health: per-feed attempts, gaps and failures from session evidence."""
import unittest

from scripts.collection_health import build_health


def request(n, topic, minute, outcome='success', status=200, feed=None, items=3):
    return {'request_id': f'q{n}', 'run_id': 'run', 'topic': topic,
            'started_at': f'2026-10-19T10:{minute:02d}:00+00:00', 'endpoint': f'https://{topic}.example',
            'configuration': {'feed_name': feed} if feed else {'attempt': 1},
            'outcome': outcome, 'http_status': status, 'item_count': items if outcome == 'success' else None}


class TestCollectionHealth(unittest.TestCase):
    def evidence(self, requests):
        return {'sessions': [{'run_id': 'run', 'topic': 'rss', 'status': 'unfinished',
                              'started_at': '2026-10-19T10:00:00+00:00'}], 'requests': requests}

    def test_feeds_are_separate_and_gap_over_threshold_is_flagged(self):
        report = build_health(self.evidence([
            request(1, 'rss', 0, feed='A'), request(2, 'rss', 3, feed='A'), request(3, 'rss', 6, feed='A'),
            request(4, 'rss', 0, feed='B'), request(5, 'rss', 45, feed='B'),
        ]), '2026-10-19T09:00:00+00:00', '2026-10-19T10:50:00+00:00')
        a, b = report['sources']['rss | A'], report['sources']['rss | B']
        self.assertEqual((a['attempts'], a['max_gap_minutes'], a['gap_over_threshold']), (3, 3.0, True))
        self.assertEqual(a['minutes_since_last_attempt'], 44.0)
        self.assertEqual((b['max_gap_minutes'], b['gap_over_threshold']), (45.0, True))
        self.assertEqual(report['problems'], ['rss | A', 'rss | B'])

    def test_all_failed_source_is_a_problem_and_statuses_are_counted(self):
        report = build_health(self.evidence([
            request(1, 'gdelt', 0, 'failed', 429), request(2, 'gdelt', 15, 'failed', 200),
            request(3, 'gdelt', 30, 'failed', None),
        ]), '2026-10-19T09:00:00+00:00', '2026-10-19T10:35:00+00:00')
        source = report['sources']['gdelt | https://gdelt.example']
        self.assertEqual(source['outcomes'], {'failed': 3})
        self.assertEqual(source['http_statuses'], {'429': 1, '200': 1, 'none': 1})
        self.assertFalse(source['gap_over_threshold'])
        self.assertEqual(report['problems'], ['gdelt | https://gdelt.example'])

    def test_timestamped_gkg_batches_are_one_source(self):
        batches = [dict(request(n, 'gdelt', minute), endpoint=f'http://data.gdeltproject.org/gdeltv2/20261019{minute:04d}00.gkg.csv.zip')
                   for n, minute in ((1, 0), (2, 15))]
        report = build_health(self.evidence(batches), '2026-10-19T09:00:00+00:00', '2026-10-19T10:20:00+00:00')
        self.assertEqual(list(report['sources']), ['gdelt | GDELT GKG batch downloads'])
        self.assertEqual(report['sources']['gdelt | GDELT GKG batch downloads']['max_gap_minutes'], 15.0)

    def test_window_excludes_older_attempts_and_reports_unfinished_sessions(self):
        report = build_health(self.evidence([request(1, 'rss', 0, feed='A'), request(2, 'rss', 20, feed='A')]),
                              '2026-10-19T10:10:00+00:00', '2026-10-19T10:25:00+00:00')
        self.assertEqual(report['sources']['rss | A']['attempts'], 1)
        self.assertEqual(report['problems'], [])
        self.assertEqual([s['run_id'] for s in report['unfinished_sessions']], ['run'])


if __name__ == '__main__':
    unittest.main()
