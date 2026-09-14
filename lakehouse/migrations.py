"""Transactional migrations; backup existing files before changing schemas."""
from datetime import datetime, timezone

VERSION = 3


def migrate(conn, db_path, schema_path):
    exists = conn.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name='lakehouse_migrations'").fetchone()[0]
    current = conn.execute('SELECT COALESCE(MAX(version),0) FROM lakehouse_migrations').fetchone()[0] if exists else 0
    if current > VERSION:
        raise RuntimeError(f'Database version {current} is newer than supported {VERSION}')
    if current == VERSION:
        return
    # Caller holds DuckDB's exclusive writable connection. Checkpoint makes
    # the backup standalone rather than depending on a WAL sidecar.
    conn.execute('CHECKPOINT')
    has_tables = conn.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_catalog=current_database()").fetchone()[0]
    if has_tables:
        stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
        backup = db_path.with_name(db_path.name + f'.before-v{VERSION}-{stamp}.bak')
        source = conn.execute('SELECT current_database()').fetchone()[0].replace('"', '""')
        # Windows denies copying an exclusively locked DuckDB file via Python.
        # DuckDB's logical database copy retains schemas/data under ownership.
        conn.execute("ATTACH '" + str(backup).replace("'", "''") + "' AS migration_backup")
        try:
            conn.execute(f'COPY FROM DATABASE "{source}" TO migration_backup')
        finally:
            conn.execute('DETACH migration_backup')
    conn.execute('BEGIN TRANSACTION')
    try:
        conn.execute(schema_path.read_text(encoding='utf-8'))
        for statement in (
            'ALTER TABLE bronze_events_raw ALTER COLUMN raw_payload DROP NOT NULL',
            'ALTER TABLE bronze_events_raw ADD COLUMN IF NOT EXISTS raw_bytes BLOB',
            'ALTER TABLE bronze_events_raw ADD COLUMN IF NOT EXISTS kafka_key BLOB',
            'ALTER TABLE bronze_events_raw ADD COLUMN IF NOT EXISTS kafka_headers JSON',
            'ALTER TABLE bronze_events_raw ADD COLUMN IF NOT EXISTS broker_timestamp_ms BIGINT',
            "ALTER TABLE bronze_events_raw ADD COLUMN IF NOT EXISTS provenance_status VARCHAR DEFAULT 'legacy_bytes_unavailable'",
            'ALTER TABLE silver_financial_news ADD COLUMN IF NOT EXISTS embedding FLOAT[384]',
            'ALTER TABLE silver_financial_news ADD COLUMN IF NOT EXISTS semantic_score FLOAT',
            'ALTER TABLE silver_financial_news ADD COLUMN IF NOT EXISTS embedding_model VARCHAR',
            'ALTER TABLE silver_financial_news ADD COLUMN IF NOT EXISTS first_event JSON',
            'ALTER TABLE silver_sec_filings ADD COLUMN IF NOT EXISTS first_event JSON',
        ):
            conn.execute(statement)
        conn.execute('CREATE TABLE IF NOT EXISTS lakehouse_migrations (version INTEGER PRIMARY KEY, applied_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP)')
        conn.execute('''CREATE TABLE IF NOT EXISTS lakehouse_message_outcomes (
            kafka_topic VARCHAR, kafka_partition INTEGER, kafka_offset BIGINT,
            status VARCHAR NOT NULL, event_id VARCHAR, error VARCHAR,
            conflicts JSON, enrichment JSON, provenance JSON,
            processed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(kafka_topic,kafka_partition,kafka_offset))''')
        conn.execute('''CREATE TABLE IF NOT EXISTS lakehouse_transport_faults (
            fault_id VARCHAR PRIMARY KEY,
            kafka_topic VARCHAR,
            kafka_partition BIGINT,
            kafka_offset BIGINT,
            raw_bytes BLOB,
            kafka_key BLOB,
            kafka_headers JSON,
            reason VARCHAR NOT NULL,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            resolved_at TIMESTAMPTZ
        )''')
        conn.execute('''CREATE VIEW IF NOT EXISTS lakehouse_rejections AS
            SELECT * FROM lakehouse_message_outcomes WHERE status='rejected' ''')
        conn.execute('''CREATE VIEW IF NOT EXISTS lakehouse_conflicts AS
            SELECT * FROM lakehouse_message_outcomes WHERE status='conflict' ''')
        conn.execute('''INSERT INTO lakehouse_message_outcomes
            (kafka_topic,kafka_partition,kafka_offset,event_id,status,provenance)
            SELECT kafka_topic,kafka_partition,kafka_offset,event_id,'legacy_unprocessed',
                '{"raw_bytes": "unavailable", "processing_outcome": "unknown"}'
            FROM bronze_events_raw ON CONFLICT DO NOTHING''')
        conn.execute('INSERT INTO lakehouse_migrations(version) VALUES (?)', [VERSION])
        conn.execute('COMMIT')
    except BaseException:
        conn.execute('ROLLBACK')
        raise
    # Persist schema DDL before Kafka can start writing. This also avoids
    # depending on DDL WAL recovery after a hard first-start process exit.
    conn.execute('CHECKPOINT')
