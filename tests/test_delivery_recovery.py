import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

from models.event import CommonEvent
from utils.kafka_producer import RedpandaProducer
from utils.outbox import DeliveryOutbox


class TestDeliveryRecovery(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / 'outbox.sqlite3')
        self.event = CommonEvent(id='event', source='RSS', title='Headline',
                                 published_at=datetime.now(timezone.utc))

    def tearDown(self):
        self.tmp.cleanup()

    def producer(self):
        with patch.object(RedpandaProducer, '_init_producer'):
            producer = RedpandaProducer(outbox_path=self.path)
        producer._is_confluent = True
        producer._producer = MagicMock()
        return producer

    def test_failed_delivery_survives_restart_and_recovers(self):
        first = self.producer()
        self.assertTrue(first.produce_event('rss', self.event))
        self.assertEqual(first.delivered_count('rss'), 0)
        callback = first._producer.produce.call_args.kwargs['on_delivery']
        callback(RuntimeError('Broker offline'), None)
        self.assertEqual(first.outbox.count('rss'), 1)
        # Recovery does not ask Redis whether this event has already been seen.
        second = self.producer()
        second.retry_pending('rss')
        self.assertEqual(second._producer.produce.call_args.kwargs['value'], self.event.to_json_bytes())
        callback = second._producer.produce.call_args.kwargs['on_delivery']
        callback(None, None)
        self.assertEqual(second.outbox.count('rss'), 0)
        self.assertEqual(second.delivered_count('rss'), 1)

    def test_local_queue_error_remains_durable(self):
        producer = self.producer()
        producer._producer.produce.side_effect = BufferError('Queue full')
        self.assertTrue(producer.produce_event('rss', self.event))
        self.assertEqual(producer.outbox.count('rss'), 1)
        self.assertEqual(producer.delivered_count('rss'), 0)

    def test_inflight_messages_not_queued_twice_and_first_payload_preserved(self):
        producer = self.producer()
        producer.produce_event('rss', self.event)
        later = self.event.model_copy(update={'title': 'Changed during retry'})
        producer.produce_event('rss', later)
        producer.retry_pending('rss')
        self.assertEqual(producer._producer.produce.call_count, 1)
        self.assertEqual(producer.outbox.pending('rss')[0][2], self.event.to_json_bytes())

    def test_disk_failure_does_not_queue_or_report_success(self):
        producer = self.producer()
        with patch.object(producer.outbox, 'enqueue', side_effect=OSError('Disk full')):
            self.assertFalse(producer.produce_event('rss', self.event))
        producer._producer.produce.assert_not_called()

    def test_cycle_counts_acknowledgements_separately_from_queued(self):
        producer = self.producer()
        producer.produce_event('rss', self.event)
        stats = {'queued': 1, 'published': 0, '_delivery_start': 0}
        self.assertEqual(producer.finish_cycle('rss', stats)['published'], 0)
        self.assertEqual(stats['pending_delivery'], 1)

    def test_topic_recovery_respects_producer_profile_boundary(self):
        outbox = DeliveryOutbox(self.path)
        outbox.enqueue('rss', 'event', self.event.to_json_bytes())
        outbox.enqueue('sec', 'filing', b'filing')
        producer = self.producer()
        producer.retry_pending('rss')
        self.assertEqual(producer._producer.produce.call_count, 1)
        self.assertEqual(producer._producer.produce.call_args.kwargs['topic'], 'rss')


if __name__ == '__main__':
    unittest.main()
