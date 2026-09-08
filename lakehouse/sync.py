import json
import logging
import argparse
from datetime import datetime, timezone
from confluent_kafka import Consumer, TopicPartition, KafkaError
from config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_RSS, TOPIC_GDELT, TOPIC_SEC
from models.event import CommonEvent
from lakehouse.db import LakehouseManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [LakehouseSync] %(message)s"
)
logger = logging.getLogger("LakehouseSync")


def sync_redpanda_to_lakehouse(limit_per_topic: int = 500):
    """
    Consumes events from Redpanda topics and syncs them into DuckDB Lakehouse tables.
    """
    lakehouse = LakehouseManager()
    topics = [TOPIC_RSS, TOPIC_GDELT, TOPIC_SEC]

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": f"lakehouse-sync-{int(datetime.now().timestamp())}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    total_synced = {"news": 0, "sec": 0}

    try:
        for topic in topics:
            logger.info(f"Syncing topic '{topic}' to Lakehouse...")
            consumer.subscribe([topic])

            news_batch = []
            sec_batch = []
            empty_polls = 0

            while empty_polls < 5 and (len(news_batch) + len(sec_batch)) < limit_per_topic:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    empty_polls += 1
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        break
                    logger.error(f"Consumer error: {msg.error()}")
                    continue

                empty_polls = 0
                try:
                    payload = json.loads(msg.value().decode("utf-8"))
                    # Parse timestamp
                    pub_time_str = payload.get("published_at")
                    pub_time = datetime.fromisoformat(pub_time_str) if pub_time_str else datetime.now(timezone.utc)
                    ingested_time_str = payload.get("ingested_time")
                    ingested_time = datetime.fromisoformat(ingested_time_str) if ingested_time_str else datetime.now(timezone.utc)

                    event = CommonEvent(
                        id=payload["id"],
                        source=payload["source"],
                        title=payload["title"],
                        content_snippet=payload.get("content_snippet", ""),
                        url=payload.get("url"),
                        published_at=pub_time,
                        ingested_time=ingested_time,
                        tickers_mentioned=payload.get("tickers_mentioned", []),
                        is_near_duplicate=payload.get("is_near_duplicate", False),
                        canonical_cluster_id=payload.get("canonical_cluster_id"),
                        embedding=payload.get("embedding"),
                        semantic_score=payload.get("semantic_score"),
                        metadata=payload.get("metadata", {}),
                        schema_version=payload.get("schema_version", "1.0")
                    )

                    if payload["source"] == "SEC":
                        sec_batch.append(event)
                    else:
                        news_batch.append(event)

                except Exception as e:
                    logger.warning(f"Error parsing message offset {msg.offset()}: {e}")

            if news_batch:
                # Ensure all news items have dense embeddings
                unembedded = [e for e in news_batch if e.embedding is None and getattr(e, '_embedding', None) is None]
                if unembedded:
                    from models.embedder import get_embedder
                    embedder = get_embedder()
                    logger.info(f"Computing embeddings for {len(unembedded)} news events...")
                    vectors = embedder.embed_batch([e.title for e in unembedded])
                    for e, vec in zip(unembedded, vectors):
                        e._embedding = vec

                count = lakehouse.insert_news_batch(news_batch)
                total_synced["news"] += count

            if sec_batch:
                count = lakehouse.insert_sec_batch(sec_batch)
                total_synced["sec"] += count

            consumer.unsubscribe()

    finally:
        consumer.close()

    stats = lakehouse.get_stats()
    lakehouse.close()

    logger.info("=== Lakehouse Sync Complete ===")
    logger.info(f"Synced New Events: {total_synced}")
    logger.info(f"Lakehouse Totals: {stats}")
    return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync Redpanda events to DuckDB Lakehouse")
    parser.add_argument("--limit", type=int, default=1000, help="Max messages to sync per topic")
    args = parser.parse_args()

    sync_redpanda_to_lakehouse(limit_per_topic=args.limit)
