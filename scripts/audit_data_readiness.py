"""Measure whether a DuckDB collection is ready for continuous NLP use."""
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb


EXPECTED_SOURCES = ('RSS', 'GDELT', 'SEC')


def _table_exists(conn, table):
    return bool(conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name=?", [table]
    ).fetchone()[0])


def _scalar(conn, sql, default=0):
    value = conn.execute(sql).fetchone()[0]
    return default if value is None else value


def build_report(db_path, now=None, freshness_hours=48, required_span_hours=24):
    now = now or datetime.now(timezone.utc)
    path = Path(db_path).resolve()
    conn = duckdb.connect(str(path), read_only=True)
    try:
        tables = {name: _table_exists(conn, name) for name in (
            'bronze_events_raw', 'silver_financial_news', 'silver_sec_filings',
            'lakehouse_message_outcomes', 'lakehouse_migrations', 'lakehouse_transport_faults')}
        news = _scalar(conn, 'SELECT COUNT(*) FROM silver_financial_news') if tables['silver_financial_news'] else 0
        sec = _scalar(conn, 'SELECT COUNT(*) FROM silver_sec_filings') if tables['silver_sec_filings'] else 0
        bronze = _scalar(conn, 'SELECT COUNT(*) FROM bronze_events_raw') if tables['bronze_events_raw'] else 0
        outcomes = _scalar(conn, 'SELECT COUNT(*) FROM lakehouse_message_outcomes') if tables['lakehouse_message_outcomes'] else 0
        schema_version = _scalar(conn, 'SELECT MAX(version) FROM lakehouse_migrations') if tables['lakehouse_migrations'] else 0
        unresolved = _scalar(conn, 'SELECT COUNT(*) FROM lakehouse_transport_faults WHERE resolved_at IS NULL') \
            if tables['lakehouse_transport_faults'] else None

        by_source = {source: 0 for source in EXPECTED_SOURCES}
        if tables['silver_financial_news']:
            by_source.update(dict(conn.execute(
                'SELECT source,COUNT(*) FROM silver_financial_news GROUP BY source').fetchall()))
        by_source['SEC'] = sec

        first_ingested = last_ingested = None
        if news or sec:
            ranges = []
            if news:
                ranges.extend(conn.execute(
                    'SELECT MIN(ingested_time),MAX(ingested_time) FROM silver_financial_news').fetchall())
            if sec:
                ranges.extend(conn.execute(
                    'SELECT MIN(ingested_time),MAX(ingested_time) FROM silver_sec_filings').fetchall())
            first_ingested = min(row[0] for row in ranges if row[0] is not None)
            last_ingested = max(row[1] for row in ranges if row[1] is not None)
        span_hours = ((last_ingested - first_ingested).total_seconds() / 3600) if first_ingested else 0
        age_hours = ((now - last_ingested.astimezone(timezone.utc)).total_seconds() / 3600) if last_ingested else None

        quality = {
            'blank_titles': 0, 'snippet_rows': 0, 'full_text_rows': 0,
            'metadata_only_gdelt_snippets': 0, 'event_time_rows': 0,
            'near_duplicate_rows': 0, 'broken_canonical_references': 0,
            'published_after_ingested_rows': 0,
        }
        if news:
            row = conn.execute('''SELECT
                COUNT(*) FILTER (WHERE COALESCE(TRIM(title),'')=''),
                COUNT(*) FILTER (WHERE COALESCE(TRIM(content_snippet),'')<>''),
                COUNT(*) FILTER (WHERE COALESCE(TRIM(content_full),'')<>''),
                COUNT(*) FILTER (WHERE source='GDELT' AND
                    (content_snippet LIKE 'Source:%Language:%' OR content_snippet LIKE 'Domain:%Themes:%')),
                COUNT(*) FILTER (WHERE event_time IS NOT NULL),
                COUNT(*) FILTER (WHERE is_near_duplicate),
                COUNT(*) FILTER (WHERE canonical_cluster_id IS NOT NULL AND canonical_cluster_id NOT IN
                    (SELECT id FROM silver_financial_news)),
                COUNT(*) FILTER (WHERE published_time>ingested_time)
                FROM silver_financial_news''').fetchone()
            quality.update(dict(zip(quality, row)))

        gates = {
            'headline_nlp_prototype': news >= 100 and quality['blank_titles'] == 0
                                      and quality['published_after_ingested_rows'] == 0,
            'current_storage_schema': schema_version == 4,
            'bronze_outcome_audit': bronze > 0 and outcomes == bronze,
            'all_sources_present': all(by_source[source] > 0 for source in EXPECTED_SOURCES),
            'fresh_collection': age_hours is not None and 0 <= age_hours <= freshness_hours,
            'sustained_collection_window': span_hours >= required_span_hours,
            'no_unresolved_transport_faults': unresolved == 0,
            'canonical_references_valid': quality['broken_canonical_references'] == 0,
        }
        automated_continuous_ready = all(value for key, value in gates.items()
                                         if key != 'headline_nlp_prototype')
        return {
            'generated_at': now.isoformat(), 'database': str(Path(db_path)),
            'thresholds': {'freshness_hours': freshness_hours,
                           'required_span_hours': required_span_hours},
            'collection': {
                'news_rows': news, 'sec_rows': sec, 'bronze_rows': bronze,
                'outcome_rows': outcomes, 'by_source': by_source,
                'first_ingested': first_ingested.isoformat() if first_ingested else None,
                'last_ingested': last_ingested.isoformat() if last_ingested else None,
                'collection_span_hours': span_hours, 'latest_record_age_hours': age_hours,
            },
            'quality': quality,
            'storage': {'schema_version': schema_version,
                        'unresolved_transport_faults': unresolved},
            'gates': gates,
            'ready_for_headline_nlp_prototype': gates['headline_nlp_prototype'],
            'automated_continuous_data_ready': automated_continuous_ready,
            'manual_gates_remaining': [
                'Verify at least one captured real RSS/GDELT syndication pair.',
                'Verify concurrent canonical assignment under multiple feed workers.',
                'Review the fixed headline sentiment sample and record human labels.',
            ],
        }
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db-path', default='data/lakehouse.duckdb')
    parser.add_argument('--output', type=Path)
    parser.add_argument('--freshness-hours', type=float, default=48)
    parser.add_argument('--required-span-hours', type=float, default=24)
    args = parser.parse_args()
    report = build_report(args.db_path, freshness_hours=args.freshness_hours,
                          required_span_hours=args.required_span_hours)
    rendered = json.dumps(report, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + '\n', encoding='utf-8')
    print(rendered)


if __name__ == '__main__':
    main()
