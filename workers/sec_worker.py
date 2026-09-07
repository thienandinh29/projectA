import sys
import time
import re
import logging
import signal
import argparse
from datetime import datetime, timezone
import xml.etree.ElementTree as ET

from config import (
    TOPIC_SEC,
    SEC_POLL_INTERVAL_SECONDS,
    SEC_USER_AGENT,
    SEC_MAX_REQUESTS_PER_SECOND
)
from models.event import CommonEvent
from utils.redis_cache import RedisDeduplicator
from utils.kafka_producer import RedpandaProducer
from utils.rate_limiter import ResilientHttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [SEC-Worker] %(message)s"
)
logger = logging.getLogger("SECWorker")

# SEC EDGAR Current Filings Atom Feed (real-time stream of all submissions)
SEC_CURRENT_EVENTS_ATOM_URL = (
    "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=&company=&dateb=&owner=include&start=0&count=40&output=atom"
)

# Monitored Form Types
MONITORED_FORMS = {"8-K", "8-K/A", "10-Q", "10-Q/A", "10-K", "10-K/A"}

# Regex patterns for parsing SEC Atom feed fields
ACCESSION_REGEX = re.compile(r"AccNo:</b>\s*([\d-]+)")
CIK_REGEX = re.compile(r"\((\d{10}|\d{7,10})\)")
FORM_REGEX = re.compile(r"^([\w\-/]+)\s*-\s*(.+)")


def parse_sec_atom_feed(xml_content: bytes) -> list:
    """Parses SEC EDGAR Atom feed XML into structured raw items."""
    root = ET.fromstring(xml_content)
    # Atom namespace
    ns = {"atom": "http://www.w3.org/2005/Atom"}

    items = []
    for entry in root.findall("atom:entry", ns):
        title_el = entry.find("atom:title", ns)
        summary_el = entry.find("atom:summary", ns)
        link_el = entry.find("atom:link", ns)
        updated_el = entry.find("atom:updated", ns)

        title = title_el.text.strip() if title_el is not None and title_el.text else ""
        summary = summary_el.text.strip() if summary_el is not None and summary_el.text else ""
        link = link_el.attrib.get("href", "") if link_el is not None else ""
        updated_str = updated_el.text.strip() if updated_el is not None and updated_el.text else ""

        # Extract Accession Number (canonical unique SEC filing identifier)
        acc_match = ACCESSION_REGEX.search(summary)
        accession_no = acc_match.group(1).strip() if acc_match else None

        # Extract Form type and Company Name
        form_type = ""
        company_name = title
        form_match = FORM_REGEX.match(title)
        if form_match:
            form_type = form_match.group(1).strip()
            company_name = form_match.group(2).strip()

        # Extract CIK number
        cik_match = CIK_REGEX.search(title)
        cik = cik_match.group(1) if cik_match else None

        items.append({
            "title": title,
            "form_type": form_type,
            "company_name": company_name,
            "cik": cik,
            "accession_number": accession_no,
            "summary": summary,
            "link": link,
            "updated": updated_str
        })
    return items


def run_sec_fetch_cycle(
    http_client: ResilientHttpClient,
    producer: RedpandaProducer,
    dedup: RedisDeduplicator
) -> dict:
    """Fetches SEC filings for monitored forms (8-K, 10-Q, 10-K), deduplicates, and publishes with acks=all."""
    stats = {"fetched": 0, "filtered": 0, "duplicates": 0, "published": 0, "errors": 0}

    # Query SEC for each critical form type
    target_form_queries = ["8-K", "10-Q", "10-K"]

    for form_query in target_form_queries:
        feed_url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type={form_query}&company=&dateb=&owner=include&start=0&count=40&output=atom"
        logger.info(f"Querying SEC EDGAR feed for form '{form_query}' under rate limiter...")

        try:
            resp = http_client.get(
                feed_url,
                headers={"Host": "www.sec.gov"}
            )
            if resp.status_code != 200:
                logger.warning(f"SEC returned HTTP {resp.status_code} for form {form_query}")
                stats["errors"] += 1
                continue

            filings = parse_sec_atom_feed(resp.content)
            stats["fetched"] += len(filings)
            logger.info(f"Parsed {len(filings)} recent {form_query} filings from SEC feed.")

            for item in filings:
                form_type = item["form_type"]

                # Double check form type match
                if not any(form_type.startswith(mf) for mf in MONITORED_FORMS):
                    stats["filtered"] += 1
                    continue

                accession_number = item["accession_number"]
                event_id = accession_number if accession_number else item["link"]

                if not event_id:
                    continue

                # Atomic check & set in Redis
                if dedup.is_duplicate_or_set(event_id, source="SEC"):
                    stats["duplicates"] += 1
                    continue

                clean_summary = re.sub(r"<[^>]+>", " ", item["summary"]).strip()

                try:
                    pub_time = datetime.fromisoformat(item["updated"]).astimezone(timezone.utc)
                except Exception:
                    pub_time = datetime.now(timezone.utc)

                event = CommonEvent(
                    id=event_id,
                    source="SEC",
                    title=f"[{form_type}] {item['company_name']}",
                    content_snippet=clean_summary[:500],
                    url=item["link"],
                    published_at=pub_time,
                    tickers_mentioned=[],
                    metadata={
                        "form_type": form_type,
                        "accession_number": accession_number,
                        "cik": item["cik"],
                        "company_name": item["company_name"]
                    }
                )

                if producer.produce_event(TOPIC_SEC, event):
                    stats["published"] += 1
                else:
                    stats["errors"] += 1

        except Exception as e:
            logger.error(f"Error executing SEC fetch cycle for form {form_query}: {e}")
            stats["errors"] += 1

    producer.flush(timeout=5.0)
    return stats


def main():
    parser = argparse.ArgumentParser(description="SEC EDGAR Regulatory Filings Worker")
    parser.add_argument("--once", action="store_true", help="Run a single fetch cycle and exit")
    args = parser.parse_args()

    # SEC worker uses 'critical' profile (acks=all, idempotence=True)
    producer = RedpandaProducer(profile="critical")
    dedup = RedisDeduplicator()
    http_client = ResilientHttpClient(
        user_agent=SEC_USER_AGENT,
        max_requests_per_sec=SEC_MAX_REQUESTS_PER_SECOND
    )

    if not dedup.ping():
        logger.warning("Redis is currently not reachable.")

    stop_requested = False

    def handle_signal(signum, frame):
        nonlocal stop_requested
        logger.info("Termination signal received. Shutting down SEC Worker...")
        stop_requested = True

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    logger.info(
        f"SEC Worker started (Topic: {TOPIC_SEC}, RateLimit: <= {SEC_MAX_REQUESTS_PER_SECOND} req/s, "
        f"Interval: {SEC_POLL_INTERVAL_SECONDS}s, Reliability: acks=all)"
    )

    while not stop_requested:
        stats = run_sec_fetch_cycle(http_client, producer, dedup)
        logger.info(
            f"SEC Cycle: Fetched={stats['fetched']}, MonitoredForms={stats['fetched'] - stats['filtered']}, "
            f"Duplicates={stats['duplicates']}, Published={stats['published']}, Errors={stats['errors']}"
        )

        if args.once:
            break

        for _ in range(SEC_POLL_INTERVAL_SECONDS):
            if stop_requested:
                break
            time.sleep(1)

    producer.flush(timeout=5.0)
    logger.info("SEC Worker exited cleanly.")


if __name__ == "__main__":
    main()
