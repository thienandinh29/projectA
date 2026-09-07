import argparse
import logging
import sys
import threading
from workers.rss_worker import run_rss_fetch_cycle
from workers.gdelt_worker import run_gdelt_fetch_cycle
from workers.sec_worker import run_sec_fetch_cycle
from utils.kafka_producer import RedpandaProducer
from utils.redis_cache import RedisDeduplicator
from utils.rate_limiter import ResilientHttpClient
from config import SEC_USER_AGENT, SEC_MAX_REQUESTS_PER_SECOND

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [Orchestrator] %(message)s"
)
logger = logging.getLogger("Orchestrator")


def run_one_shot_all():
    """Runs one fetch cycle for all three sources in parallel."""
    logger.info("Starting one-shot execution across all 3 ingestion pipelines...")

    producer_fast = RedpandaProducer(profile="fast")
    producer_critical = RedpandaProducer(profile="critical")
    dedup = RedisDeduplicator()

    if not dedup.ping():
        logger.warning("Redis ping failed. Ensure Redis container is running.")
    else:
        logger.info("Connected to Redis successfully.")

    sec_http = ResilientHttpClient(
        user_agent=SEC_USER_AGENT,
        max_requests_per_sec=SEC_MAX_REQUESTS_PER_SECOND
    )

    results = {}

    def fetch_rss():
        results["RSS"] = run_rss_fetch_cycle(producer_fast, dedup)

    def fetch_gdelt():
        results["GDELT"] = run_gdelt_fetch_cycle(producer_fast, dedup)

    def fetch_sec():
        results["SEC"] = run_sec_fetch_cycle(sec_http, producer_critical, dedup)

    threads = [
        threading.Thread(target=fetch_rss, name="RSS-Thread"),
        threading.Thread(target=fetch_gdelt, name="GDELT-Thread"),
        threading.Thread(target=fetch_sec, name="SEC-Thread")
    ]

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    logger.info("=== One-Shot Ingestion Summary ===")
    for source, stats in results.items():
        logger.info(f"[{source}] Stats: {stats}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Financial Ingestion Orchestrator")
    parser.add_argument("--once", action="store_true", help="Run one cycle for all sources and exit")
    args = parser.parse_args()

    if args.once:
        run_one_shot_all()
    else:
        print("Usage:")
        print("  - Run single source worker: python -m workers.rss_worker [--once]")
        print("  - Run GDELT worker:         python -m workers.gdelt_worker [--once]")
        print("  - Run SEC worker:           python -m workers.sec_worker [--once]")
        print("  - Run all once:             python main.py --once")
