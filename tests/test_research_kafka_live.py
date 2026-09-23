"""Opt-in broker smoke test; uses an isolated topic/group and a temporary DB."""
import os
import tempfile
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from confluent_kafka.admin import AdminClient, NewTopic
from config import KAFKA_BOOTSTRAP_SERVERS
from lakehouse.db import LakehouseManager
from lakehouse.sync import LakehouseWriter
from research.observations import make_observation
from utils.kafka_producer import RedpandaProducer


@unittest.skipUnless(os.getenv('RUN_RESEARCH_KAFKA_TEST') == '1', 'Opt-in isolated Kafka test')
class TestResearchKafka(unittest.TestCase):
    def test_observation_delivery_replay_and_availability(self):
        topic = 'research-validation-' + uuid.uuid4().hex
        admin = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
        admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])[topic].result(timeout=15)
        try:
            with tempfile.TemporaryDirectory() as tmp:
                path = str(Path(tmp)/'lake.duckdb')
                producer = RedpandaProducer(outbox_path=str(Path(tmp)/'outbox.sqlite3'))
                old = datetime.now(timezone.utc)-timedelta(hours=1)
                obs = make_observation(source='RSS', article_id='synthetic-fixture', title='Synthetic validation headline',
                    url='https://example.invalid/fixture', source_item={'fixture':True},
                    title_provenance='source_feed', observed_at=old)
                self.assertTrue(producer.produce_event(topic, obs))
                producer.flush(10)
                self.assertEqual(producer.outbox.count(topic), 0)
                counts = LakehouseWriter(path, group_id=topic+'-first', topic_sources={topic:'RESEARCH'},
                                         batch_size=1, report_lag=False).run(once=True, idle_timeout=15)
                self.assertEqual(counts['research_inserted'], 1)
                lake = LakehouseManager(path, read_only=True)
                try:
                    self.assertEqual(lake.query_research_as_of(old+timedelta(minutes=1)), [])
                    first = lake.query_research_as_of(datetime.now(timezone.utc))
                    self.assertEqual(len(first), 1)
                finally:
                    lake.close()
                replay = LakehouseWriter(path, group_id=topic+'-replay', topic_sources={topic:'RESEARCH'},
                                         batch_size=1, report_lag=False).run(once=True, idle_timeout=15)
                self.assertEqual(replay['replayed'], 1)
                lake = LakehouseManager(path, read_only=True)
                try:
                    self.assertEqual(lake.query_research_as_of(datetime.now(timezone.utc)), first)
                finally:
                    lake.close()
        finally:
            admin.delete_topics([topic])[topic].result(timeout=15)
            # Remove only the two groups created by this test, never other groups.
            for future in admin.delete_consumer_groups([topic+'-first', topic+'-replay']).values():
                future.result(timeout=15)


if __name__ == '__main__':
    unittest.main()
