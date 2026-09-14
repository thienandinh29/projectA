import logging
import threading
from collections import Counter
from typing import Optional, Literal
from models.event import CommonEvent
from config import KAFKA_BOOTSTRAP_SERVERS, OUTBOX_PATH
from utils.outbox import DeliveryOutbox

logger = logging.getLogger(__name__)


def delivery_report(err, msg):
    """Callback invoked on message delivery or failure."""
    if err is not None:
        logger.error(f"Message delivery failed to {msg.topic()} [{msg.partition()}]: {err}")
    else:
        logger.debug(
            f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
        )


class RedpandaProducer:
    """
    Tiered Redpanda / Kafka Producer.
    - 'fast' profile: acks=1, low latency for RSS/GDELT.
    - 'critical' profile: acks=all, idempotence enabled for SEC regulatory filings.
    """
    def __init__(
        self,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        profile: Literal["fast", "critical"] = "fast",
        outbox_path: Optional[str] = None
    ):
        self.bootstrap_servers = bootstrap_servers
        self.profile = profile
        self._producer = None
        self.outbox = DeliveryOutbox(outbox_path or OUTBOX_PATH)
        self._inflight = set()
        self._delivery_counts = Counter()
        self._state_lock = threading.RLock()

        # Build configuration based on profile
        base_config = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": f"ingestion-producer-{profile}",
        }

        if profile == "critical":
            # Mission-critical settings for SEC Filings
            base_config.update({
                "acks": "all",
                "enable.idempotence": True,
                "retries": 10,
                "max.in.flight.requests.per.connection": 5,
                "compression.type": "snappy",
            })
        else:
            # High-throughput settings for RSS & GDELT news
            base_config.update({
                "acks": 1,
                "retries": 3,
                "linger.ms": 5,
                "compression.type": "lz4",
            })

        self._init_producer(base_config)

    def _init_producer(self, conf: dict):
        try:
            from confluent_kafka import Producer
            self._producer = Producer(conf)
            self._is_confluent = True
            logger.info(f"Initialized Confluent Redpanda Producer (profile='{self.profile}', servers='{self.bootstrap_servers}')")
        except ImportError:
            # Fallback to kafka-python if confluent-kafka is missing
            try:
                from kafka import KafkaProducer
                if self.profile == 'critical':
                    raise RuntimeError('SEC critical profile requires confluent-kafka idempotence support')
                acks_val = -1 if conf.get("acks") == "all" else 1
                self._producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    acks=acks_val,
                    retries=conf.get("retries", 3),
                )
                self._is_confluent = False
                logger.info(f"Initialized KafkaProducer fallback (profile='{self.profile}')")
            except ImportError:
                raise RuntimeError("Neither confluent-kafka nor kafka-python is installed.")

    def produce_event(self, topic: str, event: CommonEvent) -> bool:
        """
        Persist the wire payload before asynchronously queueing it. True means
        durably staged, not acknowledged. Broker failures remain in the outbox.
        """
        try:
            payload = self.outbox.enqueue(topic, event.id, event.to_json_bytes())
        except Exception:
            logger.exception('Unable to persist outgoing event %s', event.id)
            return False
        self._queue_pending(topic, event.id, payload)
        return True

    def _finish_delivery(self, topic, event_id, error=None):
        with self._state_lock:
            if error is None:
                self._delivery_counts[topic] += 1
                try:
                    self.outbox.acknowledge(topic, event_id)
                except Exception:
                    logger.exception('Acknowledged message remains in outbox: %s', event_id)
            else:
                logger.error('Delivery failed; event %s remains in outbox: %s', event_id, error)
            self._inflight.discard((topic, event_id))

    def _queue_pending(self, topic, event_id, payload):
        with self._state_lock:
            if (topic, event_id) in self._inflight:
                return
            self._inflight.add((topic, event_id))
        key = event_id.encode('utf-8')
        try:
            if self._is_confluent:
                self._producer.produce(
                    topic=topic,
                    key=key,
                    value=payload,
                    on_delivery=lambda err, msg: self._finish_delivery(topic, event_id, err)
                )
                # Serve delivery report callbacks periodically
                self._producer.poll(0)
            else:
                future = self._producer.send(topic=topic, key=key, value=payload)
                future.add_callback(lambda _: self._finish_delivery(topic, event_id))
                future.add_errback(lambda err: self._finish_delivery(topic, event_id, err))
        except Exception as e:
            self._finish_delivery(topic, event_id, e)

    def retry_pending(self, topic: str, limit: int = 100):
        """Called each fetch cycle, including after process restart."""
        for pending_topic, event_id, payload in self.outbox.pending(topic, limit):
            self._queue_pending(pending_topic, event_id, payload)

    def delivered_count(self, topic: str) -> int:
        with self._state_lock:
            return self._delivery_counts[topic]

    def finish_cycle(self, topic: str, stats: dict) -> dict:
        self.flush(timeout=5.0)
        stats['published'] = self.delivered_count(topic) - stats.pop('_delivery_start')
        stats['pending_delivery'] = self.outbox.count(topic)
        return stats

    def flush(self, timeout: float = 10.0):
        """Flush remaining buffered messages."""
        if self._producer:
            if self._is_confluent:
                self._producer.flush(timeout=timeout)
            else:
                self._producer.flush(timeout=timeout)
