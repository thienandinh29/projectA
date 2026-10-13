"""Source quality separates repeated returns, unfinished attempts and clock bases."""
import copy
import json
import unittest

from scripts.review_source_quality import build_review


def attempt(request_id, run_id, at, outcome, count, endpoint='rss', status=200):
    return {'request_id': request_id, 'run_id': run_id, 'endpoint': endpoint,
            'started_at': at, 'outcome': outcome, 'item_count': count,
            'http_status': status, 'error': None}


class TestSourceQualityReview(unittest.TestCase):
    def test_attempt_gaps_are_per_run_and_returned_counts_are_not_unique_news(self):
        evidence = {'sessions': [{'run_id':'old','status':'closed','invalidated':True},
                                 {'run_id':'new','status':'unfinished','invalidated':False}],
                    'session_events': [], 'requests': [
                        attempt('old','old','2026-10-05T00:00:00Z','success',49),
                        attempt('a','new','2026-10-06T00:00:00Z','success',49),
                        attempt('b','new','2026-10-06T00:03:00Z','success',49),
                        attempt('c','new','2026-10-06T00:04:00Z','failed',None,'doc',429),
                        attempt('d','new','2026-10-06T00:04:06Z','unfinished',None,'doc',None)]}
        original = copy.deepcopy(evidence)
        report = build_review([], evidence)
        rss = next(r for r in report['source_requests_by_run'] if r['run_id']=='new' and r['endpoint']=='rss')
        self.assertEqual(rss['reported_items_across_attempts'], 98)
        self.assertEqual(rss['max_interval_between_attempt_starts_seconds'], 180)
        self.assertEqual(report['selected_article_ids'], 0)
        doc = next(r for r in report['source_requests_by_run'] if r['endpoint']=='doc')
        self.assertEqual(doc['outcomes'], {'failed':1,'unfinished':1})
        self.assertEqual(doc['attempts_without_item_count'], 2)
        self.assertEqual(report['request_links']['unfinished_sessions'], 1)
        self.assertEqual(evidence, original)

    def test_orphan_attempt_fails_instead_of_inventing_a_source_session(self):
        with self.assertRaisesRegex(ValueError, 'no session'):
            build_review([], {'sessions': [], 'session_events': [], 'requests':[
                attempt('a','missing','2026-10-06T00:00:00Z','success',1)]})

    def test_gdelt_discovery_is_not_publication_and_negative_clock_lag_is_visible(self):
        from config import TOPIC_RSS, TOPIC_GDELT
        evidence = {'sessions': [], 'session_events': [], 'requests': []}
        rows = []
        for source, topic, kind in [('RSS', TOPIC_RSS, 'feed:published_parsed'),
                                    ('GDELT', TOPIC_GDELT, 'gdelt_seendate')]:
            run = {'run_id': source}
            evidence['sessions'].append({'run_id':source,'topic':topic,'provenance':run,
                                         'status':'closed','invalidated':False})
            req = attempt(source,source,'2026-10-06T00:00:00Z','success',1)
            req['topic'] = topic
            evidence['requests'].append(req)
            snapshot = {'source':source,'observed_at':'2026-10-06T00:01:00Z',
                        'source_time_kind':kind,'collection_run':run,
                        'request_context':{'run_id':source,'request_id':source}}
            rows.append({'version_id':source,'article_id':source,'snapshot':json.dumps(snapshot),
                         'observed_at':snapshot['observed_at'], 'source_time':'2026-10-06T00:02:00Z'})
        result = build_review(rows, evidence)
        self.assertEqual({d['source_time_basis'] for d in result['source_to_observation_lags']},
                         {'publication','gdelt_seendate'})
        self.assertTrue(all(d['negative_lag_versions']==1 and d['median_hours']<0
                            for d in result['source_to_observation_lags']))


if __name__ == '__main__':
    unittest.main()
