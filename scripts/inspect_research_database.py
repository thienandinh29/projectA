"""Inspect a DuckDB file without creating or migrating it."""
import argparse
import json
import os
from pathlib import Path

import duckdb


COUNTED_TABLES = (
    'bronze_events_raw',
    'silver_financial_news',
    'silver_sec_filings',
    'research_observations',
    'lakehouse_message_outcomes',
)


def inspect_database(db_path):
    path = Path(db_path).resolve()
    result = {
        'database': str(path),
        'database_id': os.getenv('RESEARCH_DATABASE_ID', f'file:{path}'),
        'exists': path.is_file(),
        'schema_version': None,
        'counts': {},
    }
    if not result['exists']:
        return result

    conn = duckdb.connect(str(path), read_only=True)
    try:
        tables = {row[0] for row in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
        ).fetchall()}
        if 'lakehouse_migrations' in tables:
            result['schema_version'] = conn.execute(
                'SELECT COALESCE(MAX(version),0) FROM lakehouse_migrations'
            ).fetchone()[0]
        result['counts'] = {
            table: conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
            for table in COUNTED_TABLES if table in tables
        }
        result['tables'] = sorted(tables)
    finally:
        conn.close()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db-path', default=os.getenv('LAKEHOUSE_DB_PATH', 'data/lakehouse.duckdb'))
    args = parser.parse_args()
    print(json.dumps(inspect_database(args.db_path), indent=2))


if __name__ == '__main__':
    main()
