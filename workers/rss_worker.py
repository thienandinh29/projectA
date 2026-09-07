import sys
import time
import hashlib
import re
import logging
import signal
import argparse
from datetime import datetime, timezone
import feedparser

from config import (
    TOPIC_RSS,
    RSS_POLL_INTERVAL_SECONDS
)
from models.event import CommonEvent
from utils.redis_cache import RedisDeduplicator
from utils.kafka_producer import RedpandaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [RSS-Worker] %(message)s"
)
logger = logging.getLogger("RSSWorker")

# Curated high-yield Macro & Financial RSS Feeds
MACRO_FEEDS = [
    {
        "name": "Yahoo Finance Economy",
        "url": "https://finance.yahoo.com/news/rssindex"
    },
    {
        "name": "CNBC Economy",
        "url": "https://www.cnbc.com/id/20910258/device/rss/rss.html"
    },
    {
        "name": "MarketWatch Top Stories",
        "url": "https://feeds.content.dowjones.io/public/rss/mw_topstories"
    },
    {
        "name": "Investing.com Economy",
        "url": "https://www.investing.com/rss/news_14.rss"
    }
]

# Simple Ticker symbol matcher (e.g. $AAPL, $TSLA, SPY, NVDA)
TICKER_REGEX = re.compile(r"(?:\$([A-Z]{1,5})\b|\b([A-Z]{2,5})\b)")
COMMON_EXCLUDE_WORDS = {
    "THE", "AND", "FOR", "NEW", "GDP", "FED", "CPI", "USA", "USD", "EUR", "RATE", "BANK", "NEWS", "BILL", "POST", "TECH", "AI"
}


def extract_tickers(text: str) -> list:
    """Extracts probable stock ticker symbols mentioned in headline/snippet."""
    if not text:
        return []
    matches = TICKER_REGEX.findall(text)
    tickers = set()
    for m in matches:
        t = m[0] if m[0] else m[1]
        if t and t not in COMMON_EXCLUDE_WORDS and len(t) <= 5:
            tickers.add(t)
    return sorted(list(tickers))[:10]


def parse_published_time(entry) -> datetime:
    """Safely extracts UTC datetime from feed entry."""
    for field in ["published_parsed", "updated_parsed", "created_parsed"]:
        time_struct = getattr(entry, field, None)
        if time_struct:
            try:
                return datetime(*time_struct[:6], tzinfo=timezone.utc)
            except Exception:
                pass
    return datetime.now(timezone.utc)


def run_rss_fetch_cycle(producer: RedpandaProducer, dedup: RedisDeduplicator) -> dict:
    """Fetches all feeds, deduplicates, and publishes new items to Redpanda."""
    stats = {"fetched": 0, "duplicates": 0, "published": 0, "errors": 0}

    for feed_info in MACRO_FEEDS:
        feed_name = feed_info["name"]
        feed_url = feed_info["url"]
        logger.info(f"Polling feed: {feed_name}...")

        try:
            feed = feedparser.parse(feed_url)
            if feed.bozo and not feed.entries:
                logger.warning(f"Failed to parse feed {feed_name}: {feed.bozo_exception}")
                continue

            for entry in feed.entries:
                stats["fetched"] += 1
                link = getattr(entry, "link", "")
                title = getattr(entry, "title", "").strip()

                if not link and not title:
                    continue

                # Generate deterministic deduplication ID based on URL or title
                hash_input = link or title
                event_id = hashlib.sha256(hash_input.encode("utf-8")).hexdigest()

                # Check atomic Redis deduplication
                if dedup.is_duplicate_or_set(event_id, source="RSS"):
                    stats["duplicates"] += 1
                    continue

                # Parse snippet
                summary = getattr(entry, "summary", "") or getattr(entry, "description", "")
                # Clean html tags from summary
                clean_snippet = re.sub(r"<[^>]+>", "", summary).strip()[:500]

                pub_time = parse_published_time(entry)
                tickers = extract_tickers(f"{title} {clean_snippet}")

                event = CommonEvent(
                    id=event_id,
                    source="RSS",
                    title=title,
                    content_snippet=clean_snippet,
                    url=link,
                    published_at=pub_time,
                    tickers_mentioned=tickers,
                    metadata={
                        "feed_name": feed_name,
                        "feed_url": feed_url,
                        "author": getattr(entry, "author", None),
                    }
                )

                if producer.produce_event(TOPIC_RSS, event):
                    stats["published"] += 1
                else:
                    stats["errors"] += 1

        except Exception as e:
            logger.error(f"Error fetching RSS feed {feed_name}: {e}")
            stats["errors"] += 1

    producer.flush(timeout=5.0)
    return stats


def main():
    parser = argparse.ArgumentParser(description="Macro RSS Ingestion Worker")
    parser.add_argument("--once", action="store_true", help="Run a single fetch cycle and exit")
    args = parser.parse_args()

    producer = RedpandaProducer(profile="fast")
    dedup = RedisDeduplicator()

    if not dedup.ping():
        logger.warning("Redis is currently not reachable. Deduplication will operate in memory-only fallback or retry on reconnect.")

    stop_requested = False

    def handle_signal(signum, frame):
        nonlocal stop_requested
        logger.info("Termination signal received. Shutting down RSS Worker...")
        stop_requested = True

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    logger.info(f"RSS Ingestion Worker started (Topic: {TOPIC_RSS}, Interval: {RSS_POLL_INTERVAL_SECONDS}s)")

    while not stop_requested:
        stats = run_rss_fetch_cycle(producer, dedup)
        logger.info(
            f"Cycle finished: Fetched={stats['fetched']}, Duplicates={stats['duplicates']}, "
            f"Published={stats['published']}, Errors={stats['errors']}"
        )

        if args.once:
            break

        # Sleep with periodic checks for termination
        for _ in range(RSS_POLL_INTERVAL_SECONDS):
            if stop_requested:
                break
            time.sleep(1)

    producer.flush(timeout=5.0)
    logger.info("RSS Ingestion Worker exited cleanly.")


if __name__ == "__main__":
    main()
