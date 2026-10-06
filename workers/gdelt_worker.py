import time
import hashlib
import logging
from datetime import datetime, timezone
import requests
from urllib.parse import urlsplit

from config import (
    TOPIC_GDELT,
    GDELT_POLL_INTERVAL_SECONDS
)
from models.event import CommonEvent
from utils.redis_cache import RedisDeduplicator
from utils.kafka_producer import RedpandaProducer
from utils.entity_extract import extract_financial_entities

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
)
logger = logging.getLogger("GDELT-Worker")

GDELT_DOC_API = "https://api.gdeltproject.org/api/v2/doc/doc"
MACRO_QUERY = '(inflation OR "interest rate" OR "central bank" OR "Federal Reserve" OR "ECB")'


def parse_gdelt_timestamp(seendate_str: str) -> datetime:
    """Parses GDELT timestamp format YYYYMMDDTHHMMSSZ into UTC datetime."""
    return parse_gdelt_timestamp_with_provenance(seendate_str)[0]


def parse_gdelt_timestamp_with_provenance(seendate_str: str):
    """Return a safe timestamp plus whether GDELT supplied a valid value."""
    try:
        return (datetime.strptime(seendate_str, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc),
                'source_api')
    except Exception:
        return datetime.now(timezone.utc), 'ingestion_fallback'


def run_gdelt_fetch_cycle(producer: RedpandaProducer, dedup: RedisDeduplicator) -> dict:
    """Queries GDELT DOC 2.0 API, deduplicates, and publishes to Redpanda."""
    stats = {"fetched": 0, "duplicates": 0, "published": 0, "queued": 0, "errors": 0}
    stats["_delivery_start"] = producer.delivered_count(TOPIC_GDELT)
    stats['_cycle_started_at'] = datetime.now(timezone.utc).isoformat()
    session = producer.start_session(TOPIC_GDELT, {'endpoint': GDELT_DOC_API, 'query': MACRO_QUERY,
                                                 'maxrecords': 50, 'fallback': 'GKG audit only'})
    producer.retry_pending(TOPIC_GDELT)

    params = {
        "query": MACRO_QUERY,
        "mode": "ArtList",
        "format": "json",
        "maxrecords": "50",
        "sort": "datedesc"
    }

    logger.info(f"Querying GDELT DOC 2.0 API with query: {MACRO_QUERY}...")
    max_retries = 3
    articles = None
    for attempt in range(1, max_retries + 1):
        request_id = session.begin_request(GDELT_DOC_API, {'params': params, 'attempt': attempt,
                                                         'timeout_seconds': 15})
        r = None
        try:
            r = requests.get(
                GDELT_DOC_API,
                params=params,
                headers={"User-Agent": "FinancialIngestionWorker/1.0"},
                timeout=15.0
            )
            r.raise_for_status()
            data = r.json()
            if not isinstance(data, dict) or not isinstance(data.get('articles'), list):
                raise ValueError('GDELT response requires an articles list')
            if not all(isinstance(art, dict) for art in data['articles']):
                raise ValueError('GDELT articles must be objects')
            articles = data['articles']
        except Exception as e:
            status = r.status_code if r is not None else None
            session.finish_request(request_id, 'failed', http_status=status,
                                   error=f'{type(e).__name__}: {e}',
                                   response_excerpt=r.text[:200] if r is not None else None)
            stats['errors'] += 1
            logger.warning('GDELT DOC attempt %s/%s failed (HTTP %s): %s', attempt, max_retries, status, e)
            # Retrying permanent HTTP errors will not help. Preserve the failure
            # and the separate fallback outcome, rather than relabeling as 429.
            if status is not None and status not in (429, 500, 502, 503, 504):
                break
            if attempt < max_retries:
                time.sleep(6 if status == 429 else 3)
        else:
            session.finish_request(request_id, 'success' if articles else 'empty',
                                   http_status=r.status_code, item_count=len(articles))
            break

    if articles is None:
        logger.warning('GDELT DOC unavailable; trying GKG as separate audit-only coverage')
        return run_gdelt_raw_stream_cycle(producer, dedup, stats)

    try:
        stats["fetched"] = len(articles)
        logger.info(f"Retrieved {len(articles)} articles from GDELT.")

        for art in articles:
            url = art.get("url", "")
            title = art.get("title", "").strip()

            if not url and not title:
                continue

            event_id = hashlib.sha256((url or title).encode("utf-8")).hexdigest()

            seendate = art.get('seendate', '')
            source_time, provenance = parse_gdelt_timestamp_with_provenance(seendate)
            producer.capture_observation(
                source='GDELT', article_id=event_id, title=title, url=url,
                source_item=art, title_provenance='source_api',
                source_time=None if provenance == 'ingestion_fallback' else source_time,
                request_context=session.context(request_id, art.get('domain') or urlsplit(url).hostname),
                source_time_kind='gdelt_seendate', source_time_raw=seendate or None)

            # Extract entities BEFORE dedup: Tier 3's entity gate needs them
            # at merge-decision time (no-entity headlines bypass the gate).
            tickers = extract_financial_entities(title)

            # Check Two-Tier Redis deduplication & near-duplicate clustering
            result = dedup.check_dedup_result(
                event_id, title=title, source="GDELT", tickers=tickers, reserve_for_delivery=True
            )
            if result.is_exact:
                stats["duplicates"] += 1
                continue

            seendate = art.get("seendate", "")
            pub_time, pub_time_provenance = parse_gdelt_timestamp_with_provenance(seendate)
            domain = art.get("domain", "")
            socialimage = art.get("socialimage", "")
            tone = art.get("tone", None)

            event = CommonEvent(
                id=event_id,
                source="GDELT",
                title=title,
                content_snippet=f"Source: {domain}. Language: {art.get('language', 'English')}.",
                url=url,
                published_at=pub_time,
                tickers_mentioned=tickers,
                is_near_duplicate=result.is_near,
                canonical_cluster_id=result.canonical_id,
                embedding=result.embedding,
                semantic_score=result.semantic_score,
                metadata={
                    "domain": domain,
                    "language": art.get("language"),
                    "sourcecountry": art.get("sourcecountry"),
                    "socialimage": socialimage,
                    "tone": tone,
                    "title_provenance": "source_api",
                    "published_time_provenance": pub_time_provenance,
                }
            )


            if producer.produce_event(TOPIC_GDELT, event):
                dedup.confirm_staged(event_id, event.source)
                stats["queued"] += 1
            else:
                dedup.release_unstaged(event_id, event.source)
                stats["errors"] += 1

    except Exception as e:
        session.event('processing_error', request_id=request_id, error=f'{type(e).__name__}: {e}')
        logger.error(f"Error processing GDELT response: {e}")
        stats["errors"] += 1

    return producer.finish_cycle(TOPIC_GDELT, stats)


def run_gdelt_raw_stream_cycle(producer: RedpandaProducer, dedup: RedisDeduplicator, stats: dict) -> dict:
    """
    Fallback Ingestion from GDELT 15-minute Realtime GKG Stream.
    Synthetic GKG titles are audit only. Requests may fail independently of DOC.
    """
    import io
    import zipfile

    GDELT_LASTUPDATE_URL = "https://data.gdeltproject.org/gdeltv2/lastupdate.txt"
    ECON_THEMES = ["ECON_", "INFLATION", "CENTRAL_BANK", "EPU_ECONOMY", "WB_1104_MACROECONOMIC", "UNEMPLOYMENT"]

    session = producer.start_session(TOPIC_GDELT, {'endpoint': GDELT_DOC_API, 'query': MACRO_QUERY,
                                                 'maxrecords': 50, 'fallback': 'GKG audit only'})
    request_id = session.begin_request(GDELT_LASTUPDATE_URL, {'purpose': 'GKG manifest', 'timeout_seconds': 10})
    request_finished = False
    status = None
    try:
        logger.info(f"Fetching GDELT Realtime manifest from {GDELT_LASTUPDATE_URL}...")
        resp = requests.get(GDELT_LASTUPDATE_URL, timeout=10.0)
        status = resp.status_code
        resp.raise_for_status()

        # Lines format: <size> <md5> <url>
        lines = resp.text.strip().split("\n")
        gkg_url = None
        for line in lines:
            parts = line.split()
            if len(parts) >= 3 and "gkg.csv.zip" in parts[2]:
                gkg_url = parts[2]
                break

        if not gkg_url:
            raise ValueError('Could not locate GKG file in manifest')

        session.finish_request(request_id, 'success', http_status=status, item_count=len(lines))
        request_finished = True
        manifest_request_id = request_id
        request_id = session.begin_request(gkg_url, {'purpose': 'GKG audit only',
                                                    'manifest_request_id': manifest_request_id,
                                                    'max_captured': 50, 'themes': ECON_THEMES,
                                                    'timeout_seconds': 30})
        request_finished = False
        status = None

        logger.info(f"Downloading GDELT Realtime GKG batch: {gkg_url}...")
        zip_resp = requests.get(gkg_url, timeout=30.0)
        status = zip_resp.status_code
        zip_resp.raise_for_status()
        z = zipfile.ZipFile(io.BytesIO(zip_resp.content))
        fname = z.namelist()[0]

        logger.info(f"Parsing GDELT GKG batch '{fname}' for macroeconomic events...")
        max_batch = 50
        captured = 0

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

                producer.capture_observation(
                    source='GDELT', article_id=event_id, title=title, url=url,
                    source_item={'gkg_row': line_str, 'batch': fname},
                    request_context=session.context(request_id, urlsplit(url).hostname),
                    title_provenance='url_slug', source_time_kind='gkg_batch_time',
                    source_time_raw=date_str or None)

                # GKG is evidence only: fabricated headlines must not affect
                # genuine RSS/DOC clusters or reserve their exact URL IDs.
                captured += 1
                if captured >= max_batch:
                    break

        stats['research_captured'] = captured
        session.finish_request(request_id, 'success' if captured else 'empty', http_status=status,
                               item_count=captured, capped=captured >= max_batch,
                               count_basis='selected economic rows, not total archive rows')
        request_finished = True
        logger.info('Captured %s GKG observations for audit only', captured)

    except Exception as e:
        if not request_finished:
            session.finish_request(request_id, 'failed', http_status=status, error=f'{type(e).__name__}: {e}')
        logger.error(f"Error in GDELT raw stream cycle: {e}")
        stats["errors"] += 1

    return producer.finish_cycle(TOPIC_GDELT, stats)


def main():
    from utils.collector_lifecycle import run_collector
    run_collector('gdelt_worker', TOPIC_GDELT, GDELT_POLL_INTERVAL_SECONDS,
                  {'endpoint': GDELT_DOC_API, 'query': MACRO_QUERY, 'maxrecords': 50, 'fallback': 'GKG audit only'}, run_gdelt_fetch_cycle)


if __name__ == "__main__":
    main()
