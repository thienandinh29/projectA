"""Research timing, immutable observations and causally isolated experiments."""
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from lakehouse.db import LakehouseManager
from lakehouse.records import KafkaEnvelope
from research.observations import make_observation, SourceObservation
from research.protocol import assign_clusters, build_windows
from scripts.export_nlp_label_sample import export_sample
from scripts.audit_data_readiness import build_report
from scripts.export_research_dataset import export_dataset
from tests.test_storage import event, envelope, SOURCES


T = datetime(2026, 9, 1, 14, tzinfo=timezone.utc)
TOPICS = {'observations': 'RESEARCH'}


def observation(title='Tesla beats earnings estimates', source='RSS', **kwargs):
    values = dict(source=source, article_id='article', title=title, url='https://example.com/story',
                  source_item={'title': title}, title_provenance='source_feed' if source == 'RSS' else 'source_api',
                  observed_at=T+timedelta(minutes=5), source_time=T, source_time_kind='feed:published')
    values.update(kwargs)
    return make_observation(**values)


def message(obs, offset=0):
    return KafkaEnvelope('observations', 0, offset, obs.to_json_bytes())


def example(vid, hour, title=None, family=None):
    return dict(version_id=vid, article_id=family or vid, content_hash=vid,
                observed_at=(T+timedelta(hours=hour)).isoformat(),
                available_at=(T+timedelta(hours=hour)).isoformat(), title=title or vid,
                source='RSS', snapshot='{}')


def label(vid, hour, sentiment='neutral'):
    return dict(version_id=vid, content_hash=vid, label_available_at=(T+timedelta(hours=hour)).isoformat(),
                sentiment=sentiment, annotator='fixture', annotation_policy='test-v1')


class TestResearchStorage(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name)/'lake.duckdb')
        self.lake = LakehouseManager(self.path)

    def tearDown(self):
        self.lake.close()
        self.tmp.cleanup()

    def write_at(self, messages, time):
        with patch('lakehouse.db.datetime') as clock:
            clock.now.return_value = time
            return self.lake.write_message_batch(messages, TOPICS)

    def test_delivery_delay_cannot_backdate_availability(self):
        obs = observation()
        self.write_at([message(obs)], T+timedelta(minutes=20))
        self.assertEqual(self.lake.query_research_as_of(T+timedelta(minutes=6)), [])
        result = self.lake.query_research_as_of(T+timedelta(minutes=21))
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['observed_at'], obs.observed_at)
        self.assertEqual(result[0]['available_at'], T+timedelta(minutes=20))
        with self.assertRaises(ValueError):
            self.lake.query_research_as_of(T.replace(tzinfo=None))

    def test_crash_before_sealing_replays_without_early_visibility(self):
        record = message(observation())
        with patch.object(self.lake, 'seal_research_observations', side_effect=RuntimeError('crash after commit')):
            with self.assertRaises(RuntimeError):
                self.lake.write_message_batch([record], TOPICS)
        self.assertEqual(self.lake.query_research_as_of(T+timedelta(days=1)), [])
        result = self.write_at([record], T+timedelta(hours=1))
        self.assertEqual(result['replayed'], 1)
        self.assertEqual(len(self.lake.query_research_as_of(T+timedelta(hours=2))), 1)
        self.assertEqual(self.lake.query_research_as_of(T+timedelta(minutes=59)), [])

    def test_corrections_append_versions_and_redelivery_preserves_first_observation(self):
        first, correction = observation(), observation('Tesla misses earnings estimates')
        self.write_at([message(first)], T+timedelta(minutes=10))
        reissue = observation(observed_at=T+timedelta(minutes=15))
        self.assertEqual(first.id, reissue.id)
        self.write_at([message(reissue, 1), message(correction, 2)], T+timedelta(minutes=20))
        before = self.lake.query_research_as_of(T+timedelta(minutes=11))
        after = self.lake.query_research_as_of(T+timedelta(minutes=21))
        self.assertEqual(len(before), 1)
        self.assertEqual(len(after), 2)
        self.assertEqual(before[0]['version_id'], first.id)
        self.assertEqual(before[0]['available_at'], T+timedelta(minutes=10))
        self.assertEqual(self.lake.conn.execute('SELECT COUNT(*) FROM bronze_events_raw').fetchone()[0], 3)

    def test_synthetic_gdelt_and_sec_are_audit_only(self):
        slug = observation(source='GDELT', title_provenance='url_slug')
        sec = observation(source='SEC', title_provenance='constructed_from_filing_identity')
        self.write_at([message(slug), message(sec, 1)], T+timedelta(minutes=10))
        self.assertEqual(self.lake.query_research_as_of(T+timedelta(hours=1)), [])
        self.assertEqual(len(self.lake.query_research_as_of(T+timedelta(hours=1), eligible_only=False)), 2)

    def test_forged_observation_is_bronze_rejection(self):
        payload = observation().model_dump(mode='json')
        payload['title'] = 'forged title'
        result = self.write_at([KafkaEnvelope('observations', 0, 0, json.dumps(payload).encode())], T)
        self.assertEqual(result['rejected'], 1)
        self.assertEqual(result['research_inserted'], 0)

    def test_legacy_rows_are_never_backfilled_as_research_evidence(self):
        self.lake.write_message_batch([envelope(event())], SOURCES)
        self.assertEqual(self.lake.query_research_as_of(T+timedelta(days=30)), [])

    def test_frozen_export_is_deterministic_and_refuses_overwrite(self):
        self.write_at([message(observation())], T+timedelta(minutes=10))
        # Close the writer before an independent read-only connection.
        self.lake.close()
        a, b = Path(self.tmp.name)/'a', Path(self.tmp.name)/'b'
        first = export_dataset(self.path, a, T+timedelta(hours=1))
        second = export_dataset(self.path, b, T+timedelta(hours=1))
        self.assertEqual(first['dataset_sha256'], second['dataset_sha256'])
        self.assertEqual(first['rows'], 1)
        self.assertEqual(first['database']['schema_version'], 5)
        self.assertEqual(first['database']['total_research_versions'], 1)
        self.assertEqual(first['database']['sealed_research_versions'], 1)
        self.assertEqual(first['database']['unsealed_research_versions'], 0)
        self.assertTrue(first['database']['id'].startswith('file:'))
        self.assertEqual(first['collection_configuration']['TOPIC_OBSERVATIONS'],
                         'financial.research.observations')
        with self.assertRaises(FileExistsError):
            export_dataset(self.path, a, T+timedelta(hours=1))

    def test_pilot_export_and_audit_share_eligibility(self):
        self.lake.write_message_batch([
            envelope(event('RSS', 'rss'), 0),
            envelope(event('GDELT', 'slug', content_snippet='Domain:x Themes:y'), 0),
            envelope(event('GDELT', 'real', metadata={'title_provenance':'source_api'}), 1),
            envelope(event('GDELT', 'excluded', metadata={'title_provenance':'source_api', 'nlp_eligible':False}), 2),
        ], SOURCES)
        self.lake.close()
        count = export_sample(self.path, Path(self.tmp.name)/'sample.csv', total=100)
        report = build_report(self.path)
        self.assertEqual(count, 2)
        self.assertEqual(count, report['quality']['nlp_eligible_headline_rows'])
        self.assertFalse(report['gates']['continuous_operation_verified'])


class TestChronologicalProtocol(unittest.TestCase):
    def test_timezone_roundtrip_and_invalid_observation(self):
        obs = observation()
        self.assertEqual(SourceObservation.model_validate_json(obs.to_json_bytes()), obs)
        with self.assertRaises(ValueError):
            observation(observed_at=T.replace(tzinfo=None))

    def test_future_observations_never_rewrite_earlier_clusters(self):
        rows = [example('a', 0, 'Federal reserve raises rates today'),
                example('b', 1, 'Federal reserve raises rates today'),
                example('c', 2, 'Federal reserve cuts rates today')]
        short, long = assign_clusters(rows[:2]), assign_clusters(rows)
        self.assertEqual(short, long[:2])
        self.assertEqual(long[1]['cluster_id'], 'a')
        self.assertEqual(long[2]['cluster_id'], 'c')

    def test_labels_are_released_strictly_before_update_boundary(self):
        rows = assign_clusters([example('a', 0), example('b', 1), example('c', 3)])
        plan = build_windows(rows, [label('a', 1), label('b', 4)], [T+timedelta(hours=h) for h in (2,4,6)])
        self.assertEqual([l['version_id'] for l in plan['windows'][0]['training_labels']], ['a'])
        self.assertNotIn('b', [l['version_id'] for l in plan['windows'][1]['training_labels']])

    def test_future_label_revision_preserves_earlier_plans(self):
        rows = assign_clusters([example('a', 0), example('b', 3)])
        cuts = [T+timedelta(hours=h) for h in (2,4)]
        base = build_windows(rows, [label('a', 1)], cuts)
        future = build_windows(rows, [label('a', 1), label('a', 8, 'negative')], cuts)
        self.assertEqual(base, future)

    def test_eval_cohort_identical_across_policies_and_excludes_prior_clusters(self):
        rows = assign_clusters([example('a', 0), example('b', 2), example('c', 3, family='a'), example('d', 5)])
        cuts = [T+timedelta(hours=h) for h in (1,4,6)]
        plans = [build_windows(rows, [label('a', .5), label('b', 3)], cuts, policy)
                 for policy in ('frozen', 'retrain', 'sequential', 'replay')]
        cohorts = [[w['primary_evaluation_ids'] for w in p['windows']] for p in plans]
        self.assertTrue(all(c == cohorts[0] for c in cohorts))
        self.assertEqual(cohorts[0], [['b'], ['d']])
        self.assertEqual(plans[0]['windows'][1]['training_labels'], [])
        self.assertEqual([l['version_id'] for l in plans[2]['windows'][1]['training_labels']], ['b'])

    def test_bad_hash_naive_cutoff_and_overlapping_windows_rejected(self):
        rows = assign_clusters([example('a', 0)])
        wrong = {**label('a', 1), 'content_hash': 'wrong'}
        for labels, cuts in [([wrong], [T, T+timedelta(hours=2)]),
                             ([], [T.replace(tzinfo=None), T+timedelta(hours=2)]),
                             ([], [T, T])]:
            with self.assertRaises(ValueError):
                build_windows(rows, labels, cuts)


if __name__ == '__main__':
    unittest.main()
