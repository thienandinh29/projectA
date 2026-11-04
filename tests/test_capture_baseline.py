"""A new baseline is written only from a quiet, fully stored stack, and readers accept it."""
import json
import os
import tempfile
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from config import LAKEHOUSE_GROUP_ID
from lakehouse.db import LakehouseManager
from lakehouse.migrations import VERSION
from research.collection_boundary import load_baseline
from research.sessions import CollectionSession
from scripts import capture_baseline as capture
from utils.outbox import DeliveryOutbox


def state(members=0, **overrides):
    partitions = {f'{t}:0': {'committed': 100, 'log_start': 10, 'high': 100, 'retention_ms': 1, 'oldest_unread_ms': None}
                  for t in capture.TOPICS}
    for name, values in overrides.items():
        partitions[f'{getattr(capture, name)}:0'].update(values)
    return {'group': LAKEHOUSE_GROUP_ID, 'writer_members': members, 'partitions': partitions}


class TestStackBlockers(unittest.TestCase):
    def test_quiet_stack_has_no_blockers(self):
        self.assertEqual(capture.stack_blockers(state(), state(), [], 0), [])

    def test_running_writer_collector_and_pending_messages_block(self):
        blockers = capture.stack_blockers(state(members=1), state(members=1), ['rss'], 3)
        self.assertIn('writer is running: stop it before capture', blockers)
        self.assertIn('collectors are running: rss', blockers)
        self.assertIn('outbox has 3 pending messages: let collectors deliver them first', blockers)

    def test_writer_lag_blocks(self):
        lagging = state(TOPIC_RSS={'committed': 90})
        self.assertEqual(capture.stack_blockers(lagging, lagging, [], 0),
                         [f'{capture.TOPIC_RSS}:0: writer is 10 messages behind'])

    def test_retention_past_the_commit_blocks(self):
        gap = state(TOPIC_OBSERVATIONS={'committed': 42339, 'log_start': 50270, 'high': 50501})
        self.assertEqual(capture.stack_blockers(gap, gap, [], 0),
                         [f'{capture.TOPIC_OBSERVATIONS}:0: retention passed the writer commit by 7931 messages'])

    def test_expired_commit_is_fine_only_on_an_idle_topic(self):
        idle = state(TOPIC_GDELT={'committed': None, 'log_start': 50, 'high': 50})
        self.assertEqual(capture.stack_blockers(idle, idle, [], 0), [])
        unread = state(TOPIC_GDELT={'committed': None, 'log_start': 50, 'high': 53})
        self.assertEqual(capture.stack_blockers(unread, unread, [], 0),
                         [f'{capture.TOPIC_GDELT}:0: 3 retained messages have no writer commit'])

    def test_offsets_moving_between_reads_block(self):
        moved = state(TOPIC_RSS={'committed': 101, 'high': 101})
        self.assertEqual(capture.stack_blockers(state(), moved, [], 0),
                         [f'{capture.TOPIC_RSS}:0: broker offsets changed between reads'])

    def test_broker_error_blocks(self):
        self.assertEqual(capture.stack_blockers({'error': 'down'}, state(), [], 0), ['broker unavailable: down'])


class TestOtherBlockers(unittest.TestCase):
    def test_code_must_be_a_recorded_clean_revision(self):
        self.assertEqual(capture.code_blockers('abc', 'clean'), [])
        self.assertTrue(capture.code_blockers('unrecorded', 'clean'))
        self.assertTrue(capture.code_blockers(None, 'clean'))
        self.assertTrue(capture.code_blockers('abc', 'dirty'))

    def test_database_blockers(self):
        good = {'schema_version': VERSION, 'unresolved_faults': 0, 'unsealed_observations': 0}
        self.assertEqual(capture.database_blockers(good), [])
        self.assertEqual(len(capture.database_blockers(
            {'schema_version': VERSION - 1, 'unresolved_faults': 1, 'unsealed_observations': 2})), 3)


class TestDocument(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / 'baseline.json'

    def tearDown(self):
        self.tmp.cleanup()

    def write(self, activate):
        document = capture.build_document(
            state(TOPIC_GDELT={'committed': None, 'log_start': 50, 'high': 50}),
            {'schema_version': VERSION, 'counts': {'bronze_events_raw': 5}},
            captured_at=datetime(2026, 11, 4, 7, tzinfo=timezone.utc), revision='abc', database_id='db',
            reason='retention gap', activate=activate)
        self.path.write_text(json.dumps(document), encoding='utf-8')
        return document

    def test_active_baseline_round_trips_through_the_reader(self):
        self.write(True)
        loaded = load_baseline(self.path, capture.TOPICS, LAKEHOUSE_GROUP_ID)
        self.assertEqual(loaded['partitions'][(capture.TOPIC_RSS, 0)], {'start': 100, 'committed_at_capture': 100})
        self.assertEqual(loaded['partitions'][(capture.TOPIC_GDELT, 0)], {'start': 50, 'committed_at_capture': None})
        self.assertEqual(loaded['database']['database_id'], 'db')

    def test_inactive_baseline_is_refused_by_the_reader(self):
        self.write(False)
        with self.assertRaisesRegex(ValueError, 'not active'):
            load_baseline(self.path, capture.TOPICS, LAKEHOUSE_GROUP_ID)

    def test_reason_is_required(self):
        with self.assertRaisesRegex(ValueError, 'reason'):
            capture.build_document(state(), {'schema_version': VERSION, 'counts': {}},
                                   captured_at=datetime(2026, 11, 4, tzinfo=timezone.utc), revision='a',
                                   database_id='d', reason='  ', activate=False)


class TestLocalReads(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.outbox = DeliveryOutbox(self.root / 'outbox.sqlite3')

    def tearDown(self):
        self.tmp.cleanup()

    def test_live_collector_lock_is_detected_and_released(self):
        run = {'run_id': str(uuid.uuid4()), 'started_at': datetime.now(timezone.utc).isoformat()}
        self.assertEqual(capture.collectors_holding_locks(self.outbox.path), [])
        session = CollectionSession(self.outbox, run, capture.TOPIC_RSS, {})
        try:
            self.assertEqual(capture.collectors_holding_locks(self.outbox.path), [capture.TOPIC_RSS])
        finally:
            session.close()
        self.assertEqual(capture.collectors_holding_locks(self.outbox.path), [])

    def test_read_database_reports_schema_and_counts(self):
        path = self.root / 'lake.duckdb'
        LakehouseManager(str(path)).close()
        database = capture.read_database(path)
        self.assertEqual(database['schema_version'], VERSION)
        self.assertEqual((database['unresolved_faults'], database['unsealed_observations']), (0, 0))
        self.assertEqual(database['counts']['bronze_events_raw'], 0)


class TestMain(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        DeliveryOutbox(self.root / 'outbox.sqlite3')
        self.db = self.root / 'lake.duckdb'
        LakehouseManager(str(self.db)).close()
        self.output = self.root / 'new-baseline.json'
        self.env = {'COLLECTION_CODE_REVISION': 'abc', 'COLLECTION_WORKTREE_STATE': 'clean',
                    'RESEARCH_DATABASE_ID': 'db'}

    def tearDown(self):
        self.tmp.cleanup()

    def run_main(self, broker, *extra):
        argv = ['capture', '--settle-seconds', '0', '--outbox-path', str(self.root / 'outbox.sqlite3'),
                '--db-path', str(self.db), *extra]
        with patch.object(capture, 'read_broker_state', return_value=broker), patch('sys.argv', argv), \
             patch.dict(os.environ, self.env), patch('builtins.print'):
            capture.main()

    def test_quiet_stack_writes_an_active_baseline_that_refuses_overwrite(self):
        self.run_main(state(), '--output', str(self.output), '--reason', 'gap', '--activate')
        self.assertTrue(load_baseline(self.output, capture.TOPICS, LAKEHOUSE_GROUP_ID)['partitions'])
        with self.assertRaises(SystemExit):
            self.run_main(state(), '--output', str(self.output), '--reason', 'gap')

    def test_any_blocker_writes_nothing(self):
        with self.assertRaises(SystemExit) as stopped:
            self.run_main(state(members=1), '--output', str(self.output), '--reason', 'gap', '--activate')
        self.assertEqual(stopped.exception.code, 1)
        self.assertFalse(self.output.exists())

    def test_check_never_writes(self):
        with self.assertRaises(SystemExit) as stopped:
            self.run_main(state(), '--check', '--output', str(self.output))
        self.assertEqual(stopped.exception.code, 0)
        self.assertFalse(self.output.exists())

    def test_capture_needs_output_and_reason(self):
        with self.assertRaises(SystemExit), patch('sys.stderr'):
            self.run_main(state(), '--output', str(self.output))
        self.assertFalse(self.output.exists())


if __name__ == '__main__':
    unittest.main()
