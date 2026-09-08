import sys
import time
import hashlib
import logging
import signal
import argparse
from datetime import datetime, timezone
import requests

from config import (
    TOPIC_GDELT,
    GDELT_POLL_INTERVAL_SECONDS
)
from models.event import CommonEvent
from utils.redis_cache import RedisDeduplicator
from utils.kafka_producer import RedpandaProducer
from workers.rss_worker import extract_tickers

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
)
logger = logging.getLogger("GDELT-Worker")

GDELT_DOC_API = "https://api.gdeltproject.org/api/v2/doc/doc"
MACRO_QUERY = '(inflation OR "interest rate" OR "central bank" OR "Federal Reserve" OR "ECB")'


def parse_gdelt_timestamp(seendate_str: str) -> datetime:
    """Parses GDELT timestamp format YYYYMMDDTHHMMSSZ into UTC datetime."""
    try:
        return datetime.strptime(seendate_str, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)


def run_gdelt_fetch_cycle(producer: RedpandaProducer, dedup: RedisDeduplicator) -> dict:
    """Queries GDELT DOC 2.0 API, deduplicates, and publishes to Redpanda."""
    stats = {"fetched": 0, "duplicates": 0, "published": 0, "errors": 0}

    params = {
        "query": MACRO_QUERY,
        "mode": "ArtList",
        "format": "json",
        "maxrecords": "50",
        "sort": "datedesc"
    }

    logger.info(f"Querying GDELT DOC 2.0 API with query: {MACRO_QUERY}...")
    max_retries = 3
    resp = None
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(
                GDELT_DOC_API,
                params=params,
                headers={"User-Agent": "FinancialIngestionWorker/1.0"},
                timeout=15.0
            )
            if r.status_code == 429:
                logger.warning(f"GDELT rate limit (429) hit. Waiting 6 seconds before retry {attempt}/{max_retries}...")
                time.sleep(6.0)
                continue
            if r.status_code != 200:
                logger.warning(f"GDELT API returned status {r.status_code}: {r.text[:200]}")
                stats["errors"] += 1
                return stats
            resp = r
            break
        except Exception as e:
            if attempt == max_retries:
                logger.error(f"Error querying GDELT API: {e}")
                stats["errors"] += 1
                return stats
            time.sleep(3.0)

    if resp is None or not (resp_text := resp.text.strip()).startswith(("{", "[")):
        logger.warning("GDELT DOC 2.0 API is rate-limited (HTTP 429). Falling back to GDELT 15-minute Realtime GKG Stream...")
        return run_gdelt_raw_stream_cycle(producer, dedup, stats)

    try:
        data = resp.json()
        articles = data.get("articles", [])
        stats["fetched"] = len(articles)
        logger.info(f"Retrieved {len(articles)} articles from GDELT.")

        for art in articles:
            url = art.get("url", "")
            title = art.get("title", "").strip()

            if not url and not title:
                continue

            event_id = hashlib.sha256((url or title).encode("utf-8")).hexdigest()

            # Check Two-Tier Redis deduplication & near-duplicate clustering
            is_exact, is_near, canon_id = dedup.check_dedup(event_id, title=title, source="GDELT")
            if is_exact:
                stats["duplicates"] += 1
                continue

            seendate = art.get("seendate", "")
            pub_time = parse_gdelt_timestamp(seendate)
            domain = art.get("domain", "")
            socialimage = art.get("socialimage", "")
            tone = art.get("tone", None)

            tickers = extract_tickers(title)

            event = CommonEvent(
                id=event_id,
                source="GDELT",
                title=title,
                content_snippet=f"Source: {domain}. Language: {art.get('language', 'English')}.",
                url=url,
                published_at=pub_time,
                tickers_mentioned=tickers,
                is_near_duplicate=is_near,
                canonical_cluster_id=canon_id,
                metadata={
                    "domain": domain,
                    "language": art.get("language"),
                    "sourcecountry": art.get("sourcecountry"),
                    "socialimage": socialimage,
                    "tone": tone
                }
            )

            # Attach embedding vector for DuckDB persistence (set by Tier 3)
            event._embedding = getattr(dedup, '_last_embedding', None)
            event._semantic_score = getattr(dedup, '_last_semantic_score', None)

            if producer.produce_event(TOPIC_GDELT, event):
                stats["published"] += 1
            else:
                stats["errors"] += 1

    except Exception as e:
        logger.error(f"Error processing GDELT response: {e}")
        stats["errors"] += 1

    producer.flush(timeout=5.0)
    return stats


def run_gdelt_raw_stream_cycle(producer: RedpandaProducer, dedup: RedisDeduplicator, stats: dict) -> dict:
    """
    Fallback Ingestion from GDELT 15-minute Realtime GKG Stream.
    This endpoint (http://data.gdeltproject.org/gdeltv2/lastupdate.txt) is updated
    every 15 minutes and is never blocked by IP rate-limits.
    """
    import io
    import zipfile

    GDELT_LASTUPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
    ECON_THEMES = ["ECON_", "INFLATION", "CENTRAL_BANK", "EPU_ECONOMY", "WB_1104_MACROECONOMIC", "UNEMPLOYMENT"]

    try:
        logger.info(f"Fetching GDELT Realtime manifest from {GDELT_LASTUPDATE_URL}...")
        resp = requests.get(GDELT_LASTUPDATE_URL, timeout=10.0)
        if resp.status_code != 200:
            logger.error(f"Failed to fetch GDELT lastupdate.txt: HTTP {resp.status_code}")
            stats["errors"] += 1
            return stats

        # Lines format: <size> <md5> <url>
        lines = resp.text.strip().split("\n")
        gkg_url = None
        for line in lines:
            parts = line.split()
            if len(parts) >= 3 and "gkg.csv.zip" in parts[2]:
                gkg_url = parts[2]
                break

        if not gkg_url:
            logger.error("Could not locate GKG file in GDELT lastupdate.txt")
            stats["errors"] += 1
            return stats

        logger.info(f"Downloading GDELT Realtime GKG batch: {gkg_url}...")
        zip_resp = requests.get(gkg_url, timeout=30.0)
        z = zipfile.ZipFile(io.BytesIO(zip_resp.content))
        fname = z.namelist()[0]

        logger.info(f"Parsing GDELT GKG batch '{fname}' for macroeconomic events...")
        max_batch = 50

        with z.open(fname) as f:
            for line_bytes in f:
                line_str = line_bytes.decode("utf-8", errors="ignore")
                row = line_str.split("\t")
                if len(row) <= 7:
                    continue

                url = row[4].strip()
                themes = row[7]
                domain = row[3].strip() if len(row) > 3 else "gdelt.org"
                date_str = row[1].strip() if len(row) > 1 else ""

                if not url or not any(k in themes for k in ECON_THEMES):
                    continue

                stats["fetched"] += 1
                event_id = hashlib.sha256(url.encode("utf-8")).hexdigest()

                # Extract title/headline approximation from URL slug
                slug = url.rstrip("/").split("/")[-1].replace("-", " ").replace("_", " ")
                slug_clean = " ".join([w for w in slug.split() if not w.endswith((".html", ".htm", ".php"))])
                title = slug_clean.capitalize() if len(slug_clean) > 5 else f"[{domain}] Macroeconomic & Policy News"

                # Check Two-Tier Redis deduplication & near-duplicate clustering
                is_exact, is_near, canon_id = dedup.check_dedup(event_id, title=title, source="GDELT")
                if is_exact:
                    stats["duplicates"] += 1
                    continue

                # Parse timestamp YYYYMMDDHHMMSS
                try:
                    pub_time = datetime.strptime(date_str, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
                except Exception:
                    pub_time = datetime.now(timezone.utc)

                tickers = extract_tickers(f"{title} {themes}")

                event = CommonEvent(
                    id=event_id,
                    source="GDELT",
                    title=title,
                    content_snippet=f"Domain: {domain}. Themes: {themes[:120]}...",
                    url=url,
                    published_at=pub_time,
                    tickers_mentioned=tickers,
                    is_near_duplicate=is_near,
                    canonical_cluster_id=canon_id,
                    metadata={
                        "domain": domain,
                        "themes": themes[:250],
                        "gkg_batch": fname
                    }
                )

                # Attach embedding vector for DuckDB persistence (set by Tier 3)
                event._embedding = getattr(dedup, '_last_embedding', None)
                event._semantic_score = getattr(dedup, '_last_semantic_score', None)

                if producer.produce_event(TOPIC_GDELT, event):
                    stats["published"] += 1
                else:
                    stats["errors"] += 1

                if stats["published"] >= max_batch:
                    break

        logger.info(f"GDELT GKG stream batch published: {stats['published']} events to {TOPIC_GDELT}.")

    except Exception as e:
        logger.error(f"Error in GDELT raw stream cycle: {e}")
        stats["errors"] += 1

    producer.flush(timeout=5.0)
    return stats


def main():
    parser = argparse.ArgumentParser(description="GDELT DOC 2.0 Ingestion Worker")
    parser.add_argument("--once", action="store_true", help="Run a single fetch cycle and exit")
    args = parser.parse_args()

    producer = RedpandaProducer(profile="fast")
    dedup = RedisDeduplicator()

    if not dedup.ping():
        logger.warning("Redis is currently not reachable.")

    stop_requested = False

    def handle_signal(signum, frame):
        nonlocal stop_requested
        logger.info("Termination signal received. Shutting down GDELT Worker...")
        stop_requested = True

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    logger.info(f"GDELT Worker started (Topic: {TOPIC_GDELT}, Interval: {GDELT_POLL_INTERVAL_SECONDS}s)")

    while not stop_requested:
        stats = run_gdelt_fetch_cycle(producer, dedup)
        logger.info(
            f"GDELT Cycle: Fetched={stats['fetched']}, Duplicates={stats['duplicates']}, "
            f"Published={stats['published']}, Errors={stats['errors']}"
        )

        if args.once:
            break

        for _ in range(GDELT_POLL_INTERVAL_SECONDS):
            if stop_requested:
                break
            time.sleep(1)

    producer.flush(timeout=5.0)
    logger.info("GDELT Worker exited cleanly.")


if __name__ == "__main__":
    main()
