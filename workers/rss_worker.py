import hashlib
import re
import logging
from datetime import datetime, timezone
import feedparser
from feedparser.exceptions import NonXMLContentType, CharacterEncodingOverride
import requests
from urllib.parse import urlsplit, urljoin

from config import (
    TOPIC_RSS,
    RSS_POLL_INTERVAL_SECONDS
)
from models.event import CommonEvent
from utils.redis_cache import RedisDeduplicator
from utils.kafka_producer import RedpandaProducer
from utils.entity_extract import extract_financial_entities

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
    return parse_published_time_with_provenance(entry)[0]


def parse_published_time_with_provenance(entry):
    """Return a safe timestamp plus whether it came from the source feed."""
    for field in ["published_parsed", "updated_parsed", "created_parsed"]:
        time_struct = getattr(entry, field, None)
        if time_struct:
            try:
                return datetime(*time_struct[:6], tzinfo=timezone.utc), f'feed:{field}'
            except Exception:
                pass
    return datetime.now(timezone.utc), 'ingestion_fallback'


def parse_feed_response(response):
    # Preserve the final response URL/headers that feedparser's URL-fetch path
    # would supply. Otherwise relative links and title_detail.base change, which
    # manufactures content versions even when the publisher changed nothing.
    headers = {key.lower(): value for key, value in getattr(response, 'headers', {}).items()}
    headers['content-location'] = urljoin(response.url, headers.get('content-location') or response.url)
    return feedparser.parse(response.content, response_headers=headers)


def run_rss_fetch_cycle(producer: RedpandaProducer, dedup: RedisDeduplicator) -> dict:
    """Fetches all feeds, deduplicates, and publishes new items to Redpanda."""
    stats = {"fetched": 0, "duplicates": 0, "published": 0, "queued": 0, "errors": 0}
    stats["_delivery_start"] = producer.delivered_count(TOPIC_RSS)
    stats['_cycle_started_at'] = datetime.now(timezone.utc).isoformat()
    session = producer.start_session(TOPIC_RSS, {'feeds': MACRO_FEEDS})
    producer.retry_pending(TOPIC_RSS)

    for feed_info in MACRO_FEEDS:
        feed_name = feed_info["name"]
        feed_url = feed_info["url"]
        logger.info(f"Polling feed: {feed_name}...")
        request_id = session.begin_request(feed_url, {'feed_name': feed_name, 'timeout_seconds': 15})
        request_finished = False
        try:
            response = requests.get(feed_url, timeout=15,
                                    headers={'User-Agent': 'FinancialIngestionWorker/1.0'})
            response.raise_for_status()
            feed = parse_feed_response(response)
            fatal_empty_parse = (feed.bozo and not feed.entries and not isinstance(
                getattr(feed, 'bozo_exception', None), (NonXMLContentType, CharacterEncodingOverride)))
            if fatal_empty_parse or not feed.version:
                logger.warning('Failed to parse feed %s: %s', feed_name,
                               getattr(feed, 'bozo_exception', 'Unrecognized feed'))
                session.finish_request(request_id, 'failed', http_status=response.status_code,
                                       error=str(getattr(feed, 'bozo_exception', 'Unrecognized feed')))
                request_finished = True
                stats['errors'] += 1
                continue

            session.finish_request(request_id, 'success' if feed.entries else 'empty',
                                   http_status=response.status_code, item_count=len(feed.entries),
                                   parse_warning=str(feed.bozo_exception) if feed.bozo else None,
                                   response_url=response.url)
            request_finished = True

            for entry in feed.entries:
                stats["fetched"] += 1
                link = getattr(entry, "link", "")
                title = getattr(entry, "title", "").strip()

                if not link and not title:
                    continue

                # Generate deterministic deduplication ID based on URL or title
                hash_input = link or title
                event_id = hashlib.sha256(hash_input.encode("utf-8")).hexdigest()

                pub_time, pub_time_provenance = parse_published_time_with_provenance(entry)
                raw_item = dict(entry) if isinstance(entry, dict) else vars(entry)
                producer.capture_observation(
                    source='RSS', article_id=event_id, title=title, url=link,
                    source_item=raw_item, title_provenance='source_feed',
                    source_time=None if pub_time_provenance == 'ingestion_fallback' else pub_time,
                    source_time_kind=pub_time_provenance,
                    request_context=session.context(request_id, urlsplit(link).hostname),
                    source_time_raw=str(raw_item.get('published') or raw_item.get('updated') or '') or None)

                # Parse snippet and extract entities BEFORE dedup:
                # Tier 3's entity gate needs them at merge-decision time.
                # Dictionary extractor (~40% live coverage vs 29% regex);
                # no-entity headlines bypass the gate by design.
                summary = getattr(entry, "summary", "") or getattr(entry, "description", "")
                # Clean html tags from summary
                clean_snippet = re.sub(r"<[^>]+>", "", summary).strip()[:500]
                tickers = extract_financial_entities(f"{title} {clean_snippet}")

                # Check Two-Tier Redis deduplication & near-duplicate clustering
                result = dedup.check_dedup_result(
                    event_id, title=title, source="RSS", tickers=tickers, reserve_for_delivery=True
                )
                if result.is_exact:
                    stats["duplicates"] += 1
                    continue

                event = CommonEvent(
                    id=event_id,
                    source="RSS",
                    title=title,
                    content_snippet=clean_snippet,
                    url=link,
                    published_at=pub_time,
                    tickers_mentioned=tickers,
                    is_near_duplicate=result.is_near,
                    canonical_cluster_id=result.canonical_id,
                    embedding=result.embedding,
                    semantic_score=result.semantic_score,
                    metadata={
                        "feed_name": feed_name,
                        "feed_url": feed_url,
                        "author": getattr(entry, "author", None),
                        "title_provenance": "source_feed",
                        "published_time_provenance": pub_time_provenance,
                    }
                )


                if producer.produce_event(TOPIC_RSS, event):
                    dedup.confirm_staged(event_id, event.source)
                    stats["queued"] += 1
                else:
                    dedup.release_unstaged(event_id, event.source)
                    stats["errors"] += 1

        except Exception as e:
            if not request_finished:
                session.finish_request(request_id, 'failed', error=f'{type(e).__name__}: {e}',
                                       http_status=getattr(getattr(e, 'response', None), 'status_code', None))
            else:
                session.event('processing_error', request_id=request_id, error=f'{type(e).__name__}: {e}')
            logger.error(f"Error fetching RSS feed {feed_name}: {e}")
            stats["errors"] += 1

    return producer.finish_cycle(TOPIC_RSS, stats)


def main():
    from utils.collector_lifecycle import run_collector
    run_collector('rss_worker', TOPIC_RSS, RSS_POLL_INTERVAL_SECONDS,
                  {'feeds': MACRO_FEEDS}, run_rss_fetch_cycle)


if __name__ == "__main__":
    main()
