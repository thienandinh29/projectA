import logging
from typing import Optional, Literal
from models.event import CommonEvent
from config import KAFKA_BOOTSTRAP_SERVERS

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
        profile: Literal["fast", "critical"] = "fast"
    ):
        self.bootstrap_servers = bootstrap_servers
        self.profile = profile
        self._producer = None

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
        Asynchronously produces a CommonEvent to the specified Redpanda topic.
        Key is event.id for deterministic partition hashing.
        """
        payload = event.to_json_bytes()
        key = event.id.encode("utf-8")

        try:
            if self._is_confluent:
                self._producer.produce(
                    topic=topic,
                    key=key,
                    value=payload,
                    on_delivery=delivery_report
                )
                # Serve delivery report callbacks periodically
                self._producer.poll(0)
            else:
                self._producer.send(topic=topic, key=key, value=payload)
            return True
        except Exception as e:
            logger.error(f"Error producing event {event.id} to {topic}: {e}")
            return False

    def flush(self, timeout: float = 10.0):
        """Flush remaining buffered messages."""
        if self._producer:
            if self._is_confluent:
                self._producer.flush(timeout=timeout)
            else:
                self._producer.flush()
