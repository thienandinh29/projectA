"""Real temporary DuckDB, deterministic consumer failures and ownership checks."""
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch
from confluent_kafka import TopicPartition
from lakehouse.db import LakehouseManager
from lakehouse.sync import LakehouseWriter, TransportFaultHalt
from tests.test_storage import SOURCES, envelope, event


class Message:
    def __init__(self, record):
        self.record = record
    def error(self): return None
    def topic(self): return self.record.topic
    def partition(self): return self.record.partition
    def offset(self): return self.record.offset
    def value(self): return self.record.payload
    def key(self): return self.record.key
    def headers(self): return self.record.headers
    def timestamp(self): return (1, 123)


class ConsumerDouble:
    def __init__(self):
        self.commits, self.queue = [], []
        self.closed = False
        self.on_poll = None
        self.on_commit = None
    def subscribe(self, topics, **callbacks):
        self.topics, self.callbacks = topics, callbacks
    def poll(self, timeout):
        if self.on_poll:
            self.on_poll()
        return self.queue.pop(0) if self.queue else None
    def commit(self, offsets, asynchronous):
        if self.on_commit:
            self.on_commit()
        assert asynchronous is False
        self.commits.append([(p.topic,p.partition,p.offset) for p in offsets])
        return offsets
    def close(self): self.closed = True


class TestWriter(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name)/'lake.duckdb')
        self.consumer = ConsumerDouble()
        self.configuration = {}
        def factory(config):
            self.configuration.update(config)
            return self.consumer
        self.writer = LakehouseWriter(db_path=self.path, topic_sources=SOURCES,
            consumer_factory=factory, report_lag=False, batch_size=2)
    def tearDown(self):
        self.writer.lakehouse.close()
        self.tmp.cleanup()
    def scalar(self, table):
        return self.writer.lakehouse.conn.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
    def test_database_precedes_explicit_partition_commits(self):
        self.writer.buffer = [envelope(event(),4,partition=0),
            envelope(event(eid='other'),9,partition=1)]
        self.consumer.on_commit = lambda: self.assertEqual(self.scalar('bronze_events_raw'),2)
        self.writer.flush_batch()
        self.assertEqual(self.consumer.commits,[[('rss',0,5),('rss',1,10)]])
        self.assertFalse(self.configuration['enable.auto.commit'])
        self.assertFalse(self.configuration['enable.auto.offset.store'])
    def test_transport_fault_is_durable_and_halts_before_kafka_commit(self):
        self.writer.buffer = [envelope(event(eid='valid'), 7),
            envelope(event(eid='invalid'), -1)]
        with self.assertLogs('lakehouse.sync', level='ERROR') as logs:
            with self.assertRaisesRegex(TransportFaultHalt, 'offsets were not committed'):
                self.writer.flush_batch()
        self.assertEqual(self.consumer.commits, [])
        self.assertEqual(len(self.writer.buffer), 2)
        self.assertEqual(self.scalar('silver_financial_news'), 1)
        self.assertEqual(self.scalar('lakehouse_transport_faults'), 1)
        self.assertIn('transport_fault_halt', ''.join(logs.output))

    def test_unresolved_fault_blocks_restart_before_consumer_construction(self):
        self.writer.buffer = [envelope(event(), -1)]
        with self.assertRaises(TransportFaultHalt):
            self.writer.flush_batch()
        fault_id = self.writer.lakehouse.list_transport_faults()[0]['fault_id']
        self.writer.lakehouse.close()
        created = []
        with self.assertRaisesRegex(TransportFaultHalt, 'block startup'):
            LakehouseWriter(db_path=self.path, topic_sources=SOURCES,
                consumer_factory=lambda config: created.append(config))
        self.assertEqual(created, [])
        manager = LakehouseManager(self.path)
        manager.resolve_transport_fault(fault_id, 'Synthetic invalid coordinate removed')
        self.assertEqual(manager.unresolved_transport_fault_count(), 0)
        manager.close()
        replacement = LakehouseWriter(db_path=self.path, topic_sources=SOURCES,
            consumer_factory=lambda config: self.consumer, report_lag=False)
        replacement.lakehouse.close()

    def test_transport_fault_cli_lists_and_resolves_without_kafka(self):
        self.writer.lakehouse.write_message_batch([envelope(event(), -1)], SOURCES)
        fault_id = self.writer.lakehouse.list_transport_faults()[0]['fault_id']
        self.writer.lakehouse.close()
        listed = subprocess.run([sys.executable, '-m', 'lakehouse.sync', '--db-path', self.path,
            '--list-transport-faults'], capture_output=True, text=True, timeout=30)
        self.assertEqual(listed.returncode, 0, listed.stderr)
        self.assertIn(fault_id, listed.stdout)
        self.assertNotIn('raw_bytes', listed.stdout)
        resolved = subprocess.run([sys.executable, '-m', 'lakehouse.sync', '--db-path', self.path,
            '--resolve-transport-fault', fault_id, '--resolution-note', 'Confirmed test input'],
            capture_output=True, text=True, timeout=30)
        self.assertEqual(resolved.returncode, 0, resolved.stderr)
        manager = LakehouseManager(self.path)
        self.assertEqual(manager.unresolved_transport_fault_count(), 0)
        manager.close()

    def test_unknown_topic_rejection_does_not_break_delay_reporting(self):
        self.writer.buffer = [envelope(event(), topic='unknown')]
        result = self.writer.flush_batch()
        self.assertEqual(result['rejected'], 1)
        self.assertEqual(self.consumer.commits, [[('unknown', 0, 1)]])
    def test_database_failure_does_not_commit_offsets(self):
        self.writer.buffer = [envelope(event())]
        with patch.object(self.writer.lakehouse,'insert_news_batch',side_effect=RuntimeError('DB failure')):
            with self.assertRaisesRegex(RuntimeError,'DB failure'):
                self.writer.flush_batch()
        self.assertEqual(self.consumer.commits,[])
        self.assertEqual(self.scalar('bronze_events_raw'),0)
    def test_commit_failure_replays_durable_batch_without_replacing_silver(self):
        messages=[envelope(event())]
        self.writer.buffer=messages
        def fail(): raise RuntimeError('Commit lost')
        self.consumer.on_commit=fail
        with self.assertRaisesRegex(RuntimeError,'Commit lost'):
            self.writer.flush_batch()
        self.assertEqual(self.scalar('bronze_events_raw'),1)
        self.writer.lakehouse.close()
        self.writer=LakehouseWriter(db_path=self.path,topic_sources=SOURCES,
            consumer_factory=lambda _:self.consumer, report_lag=False)
        self.consumer.on_commit=None
        self.writer.buffer=messages
        result=self.writer.flush_batch()
        self.assertEqual((result['replayed'],result['silver_inserted']),(1,0))
        self.assertEqual(self.scalar('silver_financial_news'),1)
    def test_revoke_and_lost_discard_only_unpersisted_revoked_partitions(self):
        self.writer.buffer=[envelope(event(),0,partition=0),envelope(event(),1,partition=1)]
        self.writer.on_revoke(self.consumer,[TopicPartition('rss',0)])
        self.assertEqual([m.partition for m in self.writer.buffer],[1])
        self.writer.flush_batch()
        self.assertEqual(self.consumer.commits,[[('rss',1,2)]])
    def test_partition_commit_error_stops_after_durable_storage(self):
        from types import SimpleNamespace
        self.writer.buffer=[envelope(event())]
        self.consumer.commit=lambda **kwargs:[SimpleNamespace(error='Injected partition error')]
        with self.assertRaisesRegex(RuntimeError,'Injected partition error'):
            self.writer.flush_batch()
        self.assertEqual(self.scalar('bronze_events_raw'),1)
        self.assertEqual(len(self.writer.buffer),1)
    def test_once_flushes_at_batch_size_and_closes(self):
        self.consumer.queue=[Message(envelope(event())),Message(envelope(event(eid='second'),1))]
        result=self.writer.run(once=True)
        self.assertEqual(result['silver_inserted'],2)
        self.assertTrue(self.consumer.closed)
        self.assertIn('on_lost',self.consumer.callbacks)
    def test_interval_flush(self):
        self.consumer.queue=[Message(envelope(event()))]
        self.writer.batch_seconds=1
        with patch('lakehouse.sync.monotonic',side_effect=[0,0,1.1]):
            result=self.writer.run(once=True)
        self.assertEqual(result['bronze_inserted'],1)
    def test_once_idle_exit(self):
        with patch('lakehouse.sync.monotonic',side_effect=[0,11]):
            self.assertEqual(self.writer.run(once=True),{})
        self.assertEqual(self.consumer.commits,[])
    def test_continuous_idle_waits_and_graceful_stop_flushes(self):
        self.consumer.queue=[Message(envelope(event()))]
        polls=[]
        def poll():
            polls.append(1)
            if len(polls)==3:
                self.writer.stop_event.set()
        self.consumer.on_poll=poll
        with patch('lakehouse.sync.monotonic',side_effect=[0,0,100,101]):
            self.writer.batch_seconds=1000
            result=self.writer.run()
        self.assertEqual(len(polls),3)
        self.assertEqual(result['bronze_inserted'],1)
    def test_polling_failure_discards_buffer_without_offset_commit(self):
        self.writer.buffer=[envelope(event())]
        def fail(): raise RuntimeError('Polling failure')
        self.consumer.on_poll=fail
        with self.assertRaisesRegex(RuntimeError,'Polling failure'):
            self.writer.run()
        self.assertEqual(self.consumer.commits,[])
        self.assertTrue(self.consumer.closed)
        reopened=LakehouseManager(self.path)
        self.assertEqual(reopened.conn.execute('SELECT COUNT(*) FROM bronze_events_raw').fetchone()[0],0)
        reopened.close()
    def test_second_process_fails_before_consumer_construction(self):
        marker=Path(self.tmp.name)/'consumer-created'
        script='''import sys
from pathlib import Path
from lakehouse.sync import LakehouseWriter
def factory(config):
    Path(sys.argv[2]).write_text('created')
    raise RuntimeError('Consumer must not be constructed')
LakehouseWriter(db_path=sys.argv[1],consumer_factory=factory)
'''
        child=subprocess.run([sys.executable,'-c',script,self.path,str(marker)],capture_output=True,text=True,timeout=30)
        self.assertNotEqual(child.returncode,0)
        self.assertFalse(marker.exists(),child.stderr)
        self.assertIn('duckdb',child.stderr.lower())


if __name__=='__main__':
    unittest.main()
