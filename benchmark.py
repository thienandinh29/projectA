"""
Ingestion Pipeline Performance Audit Tool
==========================================
Comprehensive diagnostic report measuring throughput, latency, deduplication
effectiveness, data coverage, and infrastructure health for the financial
data ingestion pipeline.

Usage:
    python benchmark.py                 # Full audit report
    python benchmark.py --live 300      # Live monitor for 300 seconds (5 min)
    python benchmark.py --stress        # Stress test: run all workers + measure
"""
import sys
import os
import json
import time
import hashlib
import argparse
import logging
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from typing import Dict, List, Any, Optional

import redis

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import (
    KAFKA_BOOTSTRAP_SERVERS, REDIS_HOST, REDIS_PORT, REDIS_DB,
    TOPIC_RSS, TOPIC_GDELT, TOPIC_SEC
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [Benchmark] %(message)s"
)
logger = logging.getLogger("Benchmark")


# ─────────────────────────────────────────────────────────────────────────────
#  1. REDPANDA / KAFKA METRICS
# ─────────────────────────────────────────────────────────────────────────────

def get_topic_metrics(bootstrap_servers: str) -> Dict[str, Any]:
    """
    Collects per-topic message counts, partition offsets, and estimated
    throughput from Redpanda using confluent-kafka AdminClient + Consumer.
    """
    from confluent_kafka import Consumer, TopicPartition, KafkaException

    topics = [TOPIC_RSS, TOPIC_GDELT, TOPIC_SEC]
    results = {}

    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": "benchmark-audit-tool",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    try:
        cluster_meta = consumer.list_topics(timeout=5)
    except KafkaException as e:
        logger.error(f"Cannot connect to Redpanda broker: {e}")
        return {"error": str(e)}

    for topic_name in topics:
        topic_info = cluster_meta.topics.get(topic_name)
        if topic_info is None:
            results[topic_name] = {"status": "NOT_FOUND", "total_messages": 0}
            continue

        total_messages = 0
        partitions_detail = []

        for partition_id in topic_info.partitions:
            tp = TopicPartition(topic_name, partition_id)
            low, high = consumer.get_watermark_offsets(tp, timeout=5)
            count = high - low
            total_messages += count
            partitions_detail.append({
                "partition": partition_id,
                "low_offset": low,
                "high_offset": high,
                "message_count": count
            })

        # Sample first and last message to compute time span
        first_ts = None
        last_ts = None
        first_msg_sample = None
        last_msg_sample = None

        if total_messages > 0:
            # Read first message
            tp_start = TopicPartition(topic_name, 0, partitions_detail[0]["low_offset"])
            consumer.assign([tp_start])
            msg = consumer.poll(timeout=3.0)
            if msg and not msg.error():
                first_ts = msg.timestamp()[1] / 1000.0  # ms to s
                try:
                    first_msg_sample = json.loads(msg.value().decode("utf-8"))
                except Exception:
                    pass

            # Read last message
            tp_end = TopicPartition(topic_name, 0, max(0, partitions_detail[0]["high_offset"] - 1))
            consumer.assign([tp_end])
            msg = consumer.poll(timeout=3.0)
            if msg and not msg.error():
                last_ts = msg.timestamp()[1] / 1000.0
                try:
                    last_msg_sample = json.loads(msg.value().decode("utf-8"))
                except Exception:
                    pass

        # Compute time span and throughput
        time_span_seconds = None
        avg_throughput_per_min = None
        if first_ts and last_ts and last_ts > first_ts:
            time_span_seconds = round(last_ts - first_ts, 2)
            if time_span_seconds > 0:
                avg_throughput_per_min = round(total_messages / (time_span_seconds / 60), 2)

        results[topic_name] = {
            "status": "ACTIVE",
            "total_messages": total_messages,
            "partitions": len(partitions_detail),
            "partitions_detail": partitions_detail,
            "time_span_seconds": time_span_seconds,
            "avg_msgs_per_minute": avg_throughput_per_min,
            "first_message_time": datetime.fromtimestamp(first_ts, tz=timezone.utc).isoformat() if first_ts else None,
            "last_message_time": datetime.fromtimestamp(last_ts, tz=timezone.utc).isoformat() if last_ts else None,
            "sample_first_title": first_msg_sample.get("title", "N/A") if first_msg_sample else None,
            "sample_last_title": last_msg_sample.get("title", "N/A") if last_msg_sample else None,
        }

    consumer.close()
    return results


# ─────────────────────────────────────────────────────────────────────────────
#  2. REDIS DEDUPLICATION METRICS
# ─────────────────────────────────────────────────────────────────────────────

def get_redis_metrics(host: str, port: int, db: int) -> Dict[str, Any]:
    """
    Inspects Redis deduplication cache: key counts per source,
    TTL distribution, memory usage, and overall health.
    """
    r = redis.Redis(host=host, port=port, db=db, decode_responses=True, socket_timeout=5)

    try:
        r.ping()
    except Exception as e:
        return {"error": f"Cannot connect to Redis: {e}"}

    # Server info
    info = r.info()
    memory_used_mb = round(info.get("used_memory", 0) / 1024 / 1024, 2)
    memory_peak_mb = round(info.get("used_memory_peak", 0) / 1024 / 1024, 2)
    total_keys = info.get("db0", {}).get("keys", 0) if isinstance(info.get("db0"), dict) else r.dbsize()
    uptime_seconds = info.get("uptime_in_seconds", 0)

    # Count keys per source category using SCAN (non-blocking)
    source_counts = defaultdict(int)
    ttl_samples = []
    cursor = 0
    sample_limit = 500  # Sample up to 500 keys for TTL analysis

    scanned = 0
    while True:
        cursor, keys = r.scan(cursor=cursor, match="dedup:*", count=100)
        for key in keys:
            scanned += 1
            # Parse source from key pattern dedup:<source>:<id>
            parts = key.split(":", 2)
            if len(parts) >= 2:
                source_counts[parts[1]] += 1

            # Sample TTL values
            if len(ttl_samples) < sample_limit:
                ttl_val = r.ttl(key)
                if ttl_val > 0:
                    ttl_samples.append(ttl_val)

        if cursor == 0:
            break

    # TTL distribution analysis
    ttl_stats = {}
    if ttl_samples:
        ttl_samples.sort()
        avg_ttl_hours = round(sum(ttl_samples) / len(ttl_samples) / 3600, 1)
        min_ttl_hours = round(min(ttl_samples) / 3600, 1)
        max_ttl_hours = round(max(ttl_samples) / 3600, 1)
        median_ttl_hours = round(ttl_samples[len(ttl_samples) // 2] / 3600, 1)

        # Keys expiring soon (< 24h remaining)
        expiring_soon = sum(1 for t in ttl_samples if t < 86400)

        ttl_stats = {
            "avg_remaining_hours": avg_ttl_hours,
            "min_remaining_hours": min_ttl_hours,
            "max_remaining_hours": max_ttl_hours,
            "median_remaining_hours": median_ttl_hours,
            "keys_expiring_within_24h": expiring_soon,
            "sample_size": len(ttl_samples),
        }

    # AOF persistence status
    aof_enabled = info.get("aof_enabled", 0) == 1

    return {
        "status": "CONNECTED",
        "memory_used_mb": memory_used_mb,
        "memory_peak_mb": memory_peak_mb,
        "total_dedup_keys": scanned,
        "keys_by_source": dict(source_counts),
        "aof_enabled": aof_enabled,
        "uptime_seconds": uptime_seconds,
        "ttl_distribution": ttl_stats,
    }


# ─────────────────────────────────────────────────────────────────────────────
#  3. INGESTION LATENCY ANALYSIS
# ─────────────────────────────────────────────────────────────────────────────

def analyze_ingestion_latency(bootstrap_servers: str, topic: str, sample_size: int = 20) -> Dict[str, Any]:
    """
    Measures the gap between published_at (event time) and Kafka timestamp
    (ingested_time) for a sample of messages.
    This approximates ingestion delay = how long after an event was published
    did our pipeline actually capture and stream it.
    """
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": f"latency-audit-{int(time.time())}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    })

    try:
        cluster_meta = consumer.list_topics(timeout=5)
        topic_meta = cluster_meta.topics.get(topic)
        if not topic_meta or not topic_meta.partitions:
            return {"error": f"Topic {topic} not found"}
    except Exception as e:
        return {"error": str(e)}

    # Get latest N messages
    tp = TopicPartition(topic, 0)
    low, high = consumer.get_watermark_offsets(tp, timeout=5)
    start_offset = max(low, high - sample_size)
    consumer.assign([TopicPartition(topic, 0, start_offset)])

    latencies_seconds = []
    messages_sampled = 0

    deadline = time.time() + 5.0
    while messages_sampled < sample_size and time.time() < deadline:
        msg = consumer.poll(timeout=1.0)
        if msg is None or msg.error():
            continue

        kafka_ts_ms = msg.timestamp()[1]  # Kafka produce timestamp
        kafka_ts = kafka_ts_ms / 1000.0

        try:
            payload = json.loads(msg.value().decode("utf-8"))
            published_at_str = payload.get("published_at")
            if published_at_str:
                event_dt = datetime.fromisoformat(published_at_str)
                event_ts = event_dt.timestamp()
                latency = kafka_ts - event_ts
                if latency >= 0:  # Ignore negative (clock skew)
                    latencies_seconds.append(round(latency, 2))
        except Exception:
            pass
        messages_sampled += 1

    consumer.close()

    if not latencies_seconds:
        return {"error": "No valid latency samples collected", "messages_sampled": messages_sampled}

    latencies_seconds.sort()
    return {
        "messages_sampled": messages_sampled,
        "latency_samples": len(latencies_seconds),
        "avg_latency_seconds": round(sum(latencies_seconds) / len(latencies_seconds), 2),
        "min_latency_seconds": min(latencies_seconds),
        "max_latency_seconds": max(latencies_seconds),
        "median_latency_seconds": latencies_seconds[len(latencies_seconds) // 2],
        "p95_latency_seconds": latencies_seconds[int(len(latencies_seconds) * 0.95)],
    }


# ─────────────────────────────────────────────────────────────────────────────
#  4. DATA QUALITY & COVERAGE CHECK
# ─────────────────────────────────────────────────────────────────────────────

def analyze_data_quality(bootstrap_servers: str, topic: str, sample_size: int = 50) -> Dict[str, Any]:
    """
    Checks data quality metrics: schema completeness, field fill rates,
    ticker extraction effectiveness, and content richness.
    """
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": f"quality-audit-{int(time.time())}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    })

    try:
        cluster_meta = consumer.list_topics(timeout=5)
        topic_meta = cluster_meta.topics.get(topic)
        if not topic_meta:
            return {"error": f"Topic {topic} not found"}
    except Exception as e:
        return {"error": str(e)}

    tp = TopicPartition(topic, 0)
    low, high = consumer.get_watermark_offsets(tp, timeout=5)
    start_offset = max(low, high - sample_size)
    consumer.assign([TopicPartition(topic, 0, start_offset)])

    field_present = defaultdict(int)
    field_non_empty = defaultdict(int)
    sources_seen = defaultdict(int)
    unique_domains = set()
    ticker_counts = []
    content_lengths = []
    total_sampled = 0

    required_fields = ["id", "source", "title", "published_at", "schema_version"]
    optional_fields = ["content_snippet", "url", "tickers_mentioned", "metadata"]

    deadline = time.time() + 5.0
    while total_sampled < sample_size and time.time() < deadline:
        msg = consumer.poll(timeout=1.0)
        if msg is None or msg.error():
            continue

        try:
            payload = json.loads(msg.value().decode("utf-8"))
        except Exception:
            continue

        total_sampled += 1
        sources_seen[payload.get("source", "UNKNOWN")] += 1

        for field in required_fields + optional_fields:
            if field in payload:
                field_present[field] += 1
                val = payload[field]
                if val and (not isinstance(val, (list, str)) or len(val) > 0):
                    field_non_empty[field] += 1

        # Ticker analysis
        tickers = payload.get("tickers_mentioned", [])
        ticker_counts.append(len(tickers))

        # Content richness
        snippet = payload.get("content_snippet", "")
        content_lengths.append(len(snippet) if snippet else 0)

        # Domain diversity (from metadata or URL)
        meta = payload.get("metadata", {})
        domain = meta.get("domain") or meta.get("feed_name", "")
        if domain:
            unique_domains.add(domain)

    consumer.close()

    if total_sampled == 0:
        return {"error": "No messages to analyze"}

    # Compute fill rates
    fill_rates = {}
    for field in required_fields + optional_fields:
        present = field_present.get(field, 0)
        non_empty = field_non_empty.get(field, 0)
        fill_rates[field] = {
            "present_pct": round(present / total_sampled * 100, 1),
            "non_empty_pct": round(non_empty / total_sampled * 100, 1),
        }

    return {
        "total_sampled": total_sampled,
        "sources_distribution": dict(sources_seen),
        "unique_domains": len(unique_domains),
        "field_fill_rates": fill_rates,
        "ticker_extraction": {
            "avg_tickers_per_msg": round(sum(ticker_counts) / len(ticker_counts), 2) if ticker_counts else 0,
            "msgs_with_tickers_pct": round(sum(1 for t in ticker_counts if t > 0) / len(ticker_counts) * 100, 1) if ticker_counts else 0,
        },
        "content_richness": {
            "avg_snippet_length": round(sum(content_lengths) / len(content_lengths), 1) if content_lengths else 0,
            "msgs_with_content_pct": round(sum(1 for c in content_lengths if c > 0) / len(content_lengths) * 100, 1) if content_lengths else 0,
        }
    }


# ─────────────────────────────────────────────────────────────────────────────
#  5. LIVE THROUGHPUT MONITOR
# ─────────────────────────────────────────────────────────────────────────────

def live_monitor(bootstrap_servers: str, duration_seconds: int = 60):
    """
    Real-time throughput monitor: polls Redpanda topic offsets every 5 seconds
    and prints live msg/sec and cumulative stats.
    """
    from confluent_kafka import Consumer, TopicPartition

    topics = [TOPIC_RSS, TOPIC_GDELT, TOPIC_SEC]
    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": f"live-monitor-{int(time.time())}",
        "enable.auto.commit": False,
    })

    def get_offsets():
        offsets = {}
        for topic in topics:
            try:
                tp = TopicPartition(topic, 0)
                _, high = consumer.get_watermark_offsets(tp, timeout=3)
                offsets[topic] = high
            except Exception:
                offsets[topic] = 0
        return offsets

    print(f"\n{'='*80}")
    print(f"  LIVE THROUGHPUT MONITOR (Duration: {duration_seconds}s, Interval: 5s)")
    print(f"{'='*80}")
    print(f"{'Time':>10} | {'Topic':^28} | {'Total':>8} | {'Δ New':>6} | {'Rate':>10}")
    print(f"{'-'*10}-+-{'-'*28}-+-{'-'*8}-+-{'-'*6}-+-{'-'*10}")

    prev_offsets = get_offsets()
    start_time = time.time()
    cumulative_new = defaultdict(int)

    try:
        while time.time() - start_time < duration_seconds:
            time.sleep(5)
            elapsed = round(time.time() - start_time)
            curr_offsets = get_offsets()

            for topic in topics:
                delta = curr_offsets[topic] - prev_offsets[topic]
                cumulative_new[topic] += delta
                rate = f"{delta / 5:.1f} msg/s" if delta > 0 else "idle"
                short_topic = topic.split(".")[-1]
                print(f"{elapsed:>8}s | {short_topic:^28} | {curr_offsets[topic]:>8} | {delta:>+6} | {rate:>10}")

            prev_offsets = curr_offsets

    except KeyboardInterrupt:
        print("\n[Monitor interrupted]")

    consumer.close()

    total_elapsed = round(time.time() - start_time)
    print(f"\n{'='*80}")
    print(f"  SUMMARY (Monitored for {total_elapsed}s)")
    print(f"{'='*80}")
    for topic in topics:
        short = topic.split(".")[-1]
        total = cumulative_new[topic]
        rate = round(total / max(total_elapsed, 1) * 60, 2)
        print(f"  {short:.<30} +{total} new messages ({rate} msg/min)")


# ─────────────────────────────────────────────────────────────────────────────
#  6. STRESS TEST (run all workers + measure)
# ─────────────────────────────────────────────────────────────────────────────

def run_stress_test(bootstrap_servers: str):
    """
    Runs all 3 workers in one-shot mode while measuring time and throughput.
    """
    from confluent_kafka import Consumer, TopicPartition

    topics = [TOPIC_RSS, TOPIC_GDELT, TOPIC_SEC]
    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": f"stress-{int(time.time())}",
        "enable.auto.commit": False,
    })

    def get_offsets():
        offsets = {}
        for topic in topics:
            try:
                tp = TopicPartition(topic, 0)
                _, high = consumer.get_watermark_offsets(tp, timeout=3)
                offsets[topic] = high
            except Exception:
                offsets[topic] = 0
        return offsets

    print("\n" + "="*70)
    print("  STRESS TEST: Running all 3 workers (one-shot)")
    print("="*70)

    before_offsets = get_offsets()
    start = time.time()

    # Import and run workers
    from utils.redis_cache import RedisDeduplicator
    from utils.kafka_producer import RedpandaProducer
    from utils.rate_limiter import ResilientHttpClient
    from workers.rss_worker import run_rss_fetch_cycle
    from workers.gdelt_worker import run_gdelt_fetch_cycle
    from workers.sec_worker import run_sec_fetch_cycle
    from config import SEC_USER_AGENT, SEC_MAX_REQUESTS_PER_SECOND

    dedup = RedisDeduplicator()
    prod_fast = RedpandaProducer(profile="fast")
    prod_critical = RedpandaProducer(profile="critical")
    sec_http = ResilientHttpClient(
        user_agent=SEC_USER_AGENT,
        max_requests_per_sec=SEC_MAX_REQUESTS_PER_SECOND
    )

    # Run sequentially to avoid GDELT rate limit issues
    print("\n  [1/3] Running RSS Worker...")
    t1 = time.time()
    rss_stats = run_rss_fetch_cycle(prod_fast, dedup)
    t1_elapsed = round(time.time() - t1, 2)
    print(f"        RSS: {rss_stats} ({t1_elapsed}s)")

    print("\n  [2/3] Running GDELT Worker...")
    t2 = time.time()
    gdelt_stats = run_gdelt_fetch_cycle(prod_fast, dedup)
    t2_elapsed = round(time.time() - t2, 2)
    print(f"        GDELT: {gdelt_stats} ({t2_elapsed}s)")

    print("\n  [3/3] Running SEC Worker...")
    t3 = time.time()
    sec_stats = run_sec_fetch_cycle(sec_http, prod_critical, dedup)
    t3_elapsed = round(time.time() - t3, 2)
    print(f"        SEC: {sec_stats} ({t3_elapsed}s)")

    total_elapsed = round(time.time() - start, 2)

    # Measure after offsets
    prod_fast.flush()
    prod_critical.flush()
    time.sleep(1)
    after_offsets = get_offsets()

    consumer.close()

    print(f"\n{'='*70}")
    print(f"  STRESS TEST RESULTS (Total: {total_elapsed}s)")
    print(f"{'='*70}")
    print(f"  {'Source':<20} {'Fetched':>10} {'Dedup':>10} {'Published':>10} {'Errors':>10} {'Time':>8}")
    print(f"  {'-'*20} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*8}")

    all_stats = {"RSS": rss_stats, "GDELT": gdelt_stats, "SEC": sec_stats}
    times = {"RSS": t1_elapsed, "GDELT": t2_elapsed, "SEC": t3_elapsed}
    for src, st in all_stats.items():
        fetched = st.get("fetched", 0)
        dupes = st.get("duplicates", 0)
        pub = st.get("published", 0)
        errs = st.get("errors", 0)
        t = times[src]
        print(f"  {src:<20} {fetched:>10} {dupes:>10} {pub:>10} {errs:>10} {t:>7}s")

    total_published = sum(st.get("published", 0) for st in all_stats.values())
    print(f"\n  Total messages published: {total_published}")
    print(f"  Overall throughput: {round(total_published / max(total_elapsed, 0.1), 1)} msg/s")

    # Dedup effectiveness
    total_fetched = sum(st.get("fetched", 0) for st in all_stats.values())
    total_dupes = sum(st.get("duplicates", 0) for st in all_stats.values())
    if total_fetched > 0:
        dedup_rate = round(total_dupes / total_fetched * 100, 1)
        print(f"  Deduplication rate: {total_dupes}/{total_fetched} = {dedup_rate}%")


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN REPORT
# ─────────────────────────────────────────────────────────────────────────────

def print_section(title: str):
    print(f"\n{'━'*80}")
    print(f"  {title}")
    print(f"{'━'*80}")


def print_dict(d: dict, indent: int = 4):
    prefix = " " * indent
    for k, v in d.items():
        if isinstance(v, dict):
            print(f"{prefix}{k}:")
            print_dict(v, indent + 4)
        elif isinstance(v, list):
            print(f"{prefix}{k}: [{len(v)} items]")
        else:
            print(f"{prefix}{k}: {v}")


def run_full_audit():
    """Run the complete performance audit report."""

    now = datetime.now(timezone.utc)
    print("\n" + "="*80)
    print("  FINANCIAL INGESTION PIPELINE — PERFORMANCE AUDIT REPORT")
    print(f"  Generated: {now.isoformat()}")
    print("="*80)

    # ── Section 1: Redpanda Topic Metrics ──
    print_section("1. REDPANDA TOPIC METRICS (Throughput & Message Counts)")
    topic_metrics = get_topic_metrics(KAFKA_BOOTSTRAP_SERVERS)
    if "error" in topic_metrics:
        print(f"    ❌ Error: {topic_metrics['error']}")
    else:
        for topic, info in topic_metrics.items():
            print(f"\n    📊 Topic: {topic}")
            print(f"       Status:            {info.get('status', 'UNKNOWN')}")
            print(f"       Total Messages:     {info.get('total_messages', 0)}")
            print(f"       Partitions:         {info.get('partitions', 0)}")

            if info.get("time_span_seconds"):
                span_min = round(info['time_span_seconds'] / 60, 1)
                print(f"       Time Span:          {span_min} minutes")
                print(f"       Avg Throughput:      {info.get('avg_msgs_per_minute', 0)} msg/min")
            print(f"       First Message:      {info.get('first_message_time', 'N/A')}")
            print(f"       Last Message:       {info.get('last_message_time', 'N/A')}")

            if info.get("sample_first_title"):
                print(f"       First Title:        {info['sample_first_title'][:60]}...")
            if info.get("sample_last_title"):
                print(f"       Last Title:         {info['sample_last_title'][:60]}...")

    # ── Section 2: Redis Deduplication Metrics ──
    print_section("2. REDIS DEDUPLICATION CACHE HEALTH")
    redis_metrics = get_redis_metrics(REDIS_HOST, REDIS_PORT, REDIS_DB)
    if "error" in redis_metrics:
        print(f"    ❌ Error: {redis_metrics['error']}")
    else:
        print(f"    Status:                {redis_metrics['status']}")
        print(f"    Memory Used:           {redis_metrics['memory_used_mb']} MB")
        print(f"    Memory Peak:           {redis_metrics['memory_peak_mb']} MB")
        print(f"    Total Dedup Keys:      {redis_metrics['total_dedup_keys']}")
        print(f"    AOF Persistence:       {'✅ Enabled' if redis_metrics['aof_enabled'] else '⚠️ Disabled'}")
        print(f"    Uptime:                {round(redis_metrics['uptime_seconds'] / 3600, 1)} hours")

        print(f"\n    Keys by Source:")
        for src, cnt in redis_metrics.get("keys_by_source", {}).items():
            print(f"       {src:.<20} {cnt} keys")

        ttl = redis_metrics.get("ttl_distribution", {})
        if ttl:
            print(f"\n    TTL Distribution (sampled {ttl.get('sample_size', 0)} keys):")
            print(f"       Avg Remaining:      {ttl.get('avg_remaining_hours', 'N/A')} hours")
            print(f"       Min Remaining:      {ttl.get('min_remaining_hours', 'N/A')} hours")
            print(f"       Max Remaining:      {ttl.get('max_remaining_hours', 'N/A')} hours")
            print(f"       Median Remaining:   {ttl.get('median_remaining_hours', 'N/A')} hours")
            print(f"       Expiring < 24h:     {ttl.get('keys_expiring_within_24h', 0)} keys")

    # ── Section 3: Ingestion Latency ──
    print_section("3. INGESTION LATENCY (Event → Redpanda Delay)")
    for topic in [TOPIC_RSS, TOPIC_SEC]:
        short_name = topic.split(".")[-1].upper()
        print(f"\n    📈 {short_name} Latency Analysis:")
        latency = analyze_ingestion_latency(KAFKA_BOOTSTRAP_SERVERS, topic)
        if "error" in latency:
            print(f"       ⚠️  {latency['error']}")
        else:
            print(f"       Samples:            {latency['latency_samples']}")
            print(f"       Avg Latency:        {latency['avg_latency_seconds']}s")
            print(f"       Min Latency:        {latency['min_latency_seconds']}s")
            print(f"       Max Latency:        {latency['max_latency_seconds']}s")
            print(f"       Median Latency:     {latency['median_latency_seconds']}s")
            print(f"       P95 Latency:        {latency['p95_latency_seconds']}s")

    # ── Section 4: Data Quality ──
    print_section("4. DATA QUALITY & SCHEMA COVERAGE")
    for topic in [TOPIC_RSS, TOPIC_SEC]:
        short_name = topic.split(".")[-1].upper()
        print(f"\n    🔍 {short_name} Data Quality:")
        quality = analyze_data_quality(KAFKA_BOOTSTRAP_SERVERS, topic)
        if "error" in quality:
            print(f"       ⚠️  {quality['error']}")
        else:
            print(f"       Sampled:            {quality['total_sampled']} messages")
            print(f"       Sources:            {quality['sources_distribution']}")
            print(f"       Unique Domains:     {quality['unique_domains']}")

            print(f"\n       Field Fill Rates:")
            for field, rates in quality.get("field_fill_rates", {}).items():
                fill = rates["non_empty_pct"]
                icon = "✅" if fill >= 90 else "⚠️" if fill >= 50 else "❌"
                print(f"         {icon} {field:.<25} {fill}%")

            te = quality.get("ticker_extraction", {})
            print(f"\n       Ticker Extraction:  {te.get('msgs_with_tickers_pct', 0)}% of messages have tickers")
            print(f"                           Avg {te.get('avg_tickers_per_msg', 0)} tickers/message")

            cr = quality.get("content_richness", {})
            print(f"       Content Richness:   {cr.get('msgs_with_content_pct', 0)}% have snippet")
            print(f"                           Avg length: {cr.get('avg_snippet_length', 0)} chars")

    # ── Summary ──
    print_section("5. OVERALL PIPELINE HEALTH SCORE")
    total_msgs = sum(
        topic_metrics.get(t, {}).get("total_messages", 0)
        for t in [TOPIC_RSS, TOPIC_GDELT, TOPIC_SEC]
    ) if "error" not in topic_metrics else 0

    total_keys = redis_metrics.get("total_dedup_keys", 0) if "error" not in redis_metrics else 0

    checks = {
        "Redpanda Connected": "error" not in topic_metrics,
        "Redis Connected": "error" not in redis_metrics,
        "RSS Topic Has Data": topic_metrics.get(TOPIC_RSS, {}).get("total_messages", 0) > 0 if "error" not in topic_metrics else False,
        "SEC Topic Has Data": topic_metrics.get(TOPIC_SEC, {}).get("total_messages", 0) > 0 if "error" not in topic_metrics else False,
        "Redis AOF Enabled": redis_metrics.get("aof_enabled", False) if "error" not in redis_metrics else False,
        "Dedup Keys Present": total_keys > 0,
    }

    passed = sum(1 for v in checks.values() if v)
    total = len(checks)
    score = round(passed / total * 100)

    for check, ok in checks.items():
        icon = "✅" if ok else "❌"
        print(f"    {icon} {check}")

    print(f"\n    Pipeline Health Score: {score}% ({passed}/{total} checks passed)")
    print(f"    Total Messages Ingested: {total_msgs}")
    print(f"    Total Dedup Keys: {total_keys}")
    print(f"\n{'='*80}\n")


# ─────────────────────────────────────────────────────────────────────────────
#  CLI ENTRYPOINT
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Ingestion Pipeline Performance Audit")
    parser.add_argument("--live", type=int, default=0, metavar="SECONDS",
                        help="Run live throughput monitor for N seconds")
    parser.add_argument("--stress", action="store_true",
                        help="Run stress test: execute all workers + measure")
    args = parser.parse_args()

    if args.live > 0:
        live_monitor(KAFKA_BOOTSTRAP_SERVERS, args.live)
    elif args.stress:
        run_stress_test(KAFKA_BOOTSTRAP_SERVERS)
    else:
        run_full_audit()


if __name__ == "__main__":
    main()
