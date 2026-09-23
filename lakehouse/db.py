import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
import duckdb
from models.event import CommonEvent
from models.embedder import MODEL_NAME
from lakehouse.migrations import migrate
from lakehouse.records import (KafkaEnvelope, TransportIdentityError,
                               validate_transport_identity, validate_payload,
                               compare_events, identity)

logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "lakehouse.duckdb"
SCHEMA_SQL_PATH = Path(__file__).resolve().parent / "schema.sql"


class LakehouseManager:
    """
    DuckDB Point-in-Time Lakehouse Manager.
    Provides schema migration, batch insertions, and time-travel query interfaces
        Legacy observation-time queries and separately sealed research observations.
    """
    def __init__(self, db_path: Optional[str] = None, read_only: bool = False):
        self.db_path = (Path(db_path) if db_path else DEFAULT_DB_PATH).resolve()
        self.read_only = read_only
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = duckdb.connect(str(self.db_path), read_only=read_only)
        if not read_only:
            try:
                self._init_schema()
            except BaseException:
                self.conn.close()
                raise

    def _init_schema(self):
        """Fail-fast versioned migration with a standalone pre-migration backup."""
        migrate(self.conn, self.db_path, SCHEMA_SQL_PATH)
        # Do not infer checkpoint provenance for legacy vectors during migration.
        logger.info("Initialized Lakehouse schema at %s", self.db_path)

    def close(self):
        """Closes the DuckDB connection."""
        if self.conn:
            self.conn.close()

    def list_transport_faults(self, unresolved_only: bool = True) -> List[Dict[str, Any]]:
        """Return transport faults without exposing captured payload bytes."""
        where = 'WHERE resolved_at IS NULL' if unresolved_only else ''
        cursor = self.conn.execute(f'''SELECT fault_id,kafka_topic,kafka_partition,kafka_offset,
            reason,created_at,last_seen_at,occurrence_count,resolved_at,resolution_note
            FROM lakehouse_transport_faults {where} ORDER BY created_at,fault_id''')
        return [dict(zip([column[0] for column in cursor.description], row))
                for row in cursor.fetchall()]

    def unresolved_transport_fault_count(self) -> int:
        return self.conn.execute(
            'SELECT COUNT(*) FROM lakehouse_transport_faults WHERE resolved_at IS NULL').fetchone()[0]

    def resolve_transport_fault(self, fault_id: str, resolution_note: str) -> None:
        """Record an explicit operator decision before ingestion may resume."""
        if not isinstance(resolution_note, str) or not resolution_note.strip():
            raise ValueError('A nonblank resolution note is required')
        note = resolution_note.strip()
        self.conn.execute('BEGIN TRANSACTION')
        try:
            row = self.conn.execute('''UPDATE lakehouse_transport_faults
                SET resolved_at=CURRENT_TIMESTAMP,resolution_note=?
                WHERE fault_id=? AND resolved_at IS NULL RETURNING fault_id,resolved_at''',
                [note, fault_id]).fetchone()
            if row is None:
                raise ValueError('Transport fault does not exist or is already resolved')
            self.conn.execute('''INSERT INTO lakehouse_transport_fault_resolutions
                (fault_id,resolved_at,resolution_note) VALUES (?,?,?)''', [row[0], row[1], note])
            self.conn.execute('COMMIT')
        except BaseException:
            self.conn.execute('ROLLBACK')
            raise

    # ─────────────────────────────────────────────────────────────────────────
    #  BATCH INSERTIONS (SILVER LAYER)
    # ─────────────────────────────────────────────────────────────────────────

    def insert_news_batch(self, events: List[CommonEvent]) -> int:
        """
        Batch inserts CommonEvent instances into silver_financial_news.
        Uses INSERT OR IGNORE; preserves the first stored version on primary key `id`.
        Supports optional embedding vectors (384-dim float arrays).
        """
        if not events:
            return 0

        rows = []
        for e in events:
            # Ensure timestamps are ISO strings or datetimes
            event_time = e.event_time.isoformat() if e.event_time else None
            published_time = e.published_at.isoformat() if e.published_at else datetime.now(timezone.utc).isoformat()
            ingested_time = e.ingested_time.isoformat() if e.ingested_time else datetime.now(timezone.utc).isoformat()

            # Handle embedding: convert numpy array to list for DuckDB FLOAT[384]
            embedding_list = None
            raw_emb = getattr(e, '_embedding', None)
            if raw_emb is None:
                raw_emb = getattr(e, 'embedding', None)
            if raw_emb is not None:
                embedding_list = raw_emb.tolist() if hasattr(raw_emb, 'tolist') else list(raw_emb)

            # Handle semantic score
            semantic_score = getattr(e, '_semantic_score', None)
            if semantic_score is None:
                semantic_score = getattr(e, 'semantic_score', None)

            rows.append((
                e.id,
                e.source,
                e.title,
                e.content_snippet,
                e.content_full,
                e.url,
                event_time,
                published_time,
                ingested_time,
                e.tickers_mentioned,
                e.is_near_duplicate,
                e.canonical_cluster_id,
                embedding_list,
                semantic_score,
                MODEL_NAME if embedding_list is not None else None,
                json.dumps(e.metadata),
                e.schema_version,
                e.model_dump_json()
            ))

        sql = """
        INSERT OR IGNORE INTO silver_financial_news (
            id, source, title, content_snippet, content_full, url,
            event_time, published_time, ingested_time,
            tickers_mentioned, is_near_duplicate, canonical_cluster_id,
            embedding, semantic_score, embedding_model,
            metadata, schema_version, first_event
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        ids = list({row[0] for row in rows})
        existing = self.conn.execute(
            'SELECT COUNT(*) FROM silver_financial_news WHERE id IN (SELECT unnest(?))',
            [ids]).fetchone()[0]
        self.conn.executemany(sql, rows)
        inserted = len(ids) - existing
        logger.info("Inserted %s new events into silver_financial_news", inserted)
        return inserted

    def insert_sec_batch(self, events: List[CommonEvent]) -> int:
        """
        Batch inserts SEC CommonEvent instances into silver_sec_filings.
        Uses INSERT OR IGNORE; preserves the first stored version on primary key `accession_number`.
        """
        if not events:
            return 0

        rows = []
        for e in events:
            meta = e.metadata or {}
            accession_no = meta.get("accession_number") or e.id
            cik = str(meta.get("cik", "0000000000"))
            company_name = meta.get("company_name", e.title)
            form_type = meta.get("form_type", "8-K")
            filing_date = e.published_at.date().isoformat() if e.published_at else None
            acceptance_time = e.published_at.isoformat() if e.published_at else datetime.now(timezone.utc).isoformat()
            ingested_time = e.ingested_time.isoformat() if e.ingested_time else datetime.now(timezone.utc).isoformat()

            rows.append((
                accession_no,
                cik,
                company_name,
                form_type,
                filing_date,
                acceptance_time,
                ingested_time,
                e.url,
                json.dumps(meta),
                e.schema_version,
                e.model_dump_json()
            ))

        sql = """
        INSERT OR IGNORE INTO silver_sec_filings (
            accession_number, cik, company_name, form_type,
            filing_date, acceptance_time, ingested_time,
            url, metadata, schema_version, first_event
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        accessions = list({row[0] for row in rows})
        existing = self.conn.execute(
            'SELECT COUNT(*) FROM silver_sec_filings WHERE accession_number IN (SELECT unnest(?))',
            [accessions]).fetchone()[0]
        self.conn.executemany(sql, rows)
        inserted = len(accessions) - existing
        logger.info("Inserted %s new filings into silver_sec_filings", inserted)
        return inserted

    # ─────────────────────────────────────────────────────────────────────────
    #  POINT-IN-TIME (PiT) TIME-TRAVEL QUERY HELPERS
    # ─────────────────────────────────────────────────────────────────────────

    def _stored_event(self, event):
        table, key = ('silver_sec_filings', 'accession_number') if event.source == 'SEC' else ('silver_financial_news', 'id')
        cursor = self.conn.execute(f'SELECT * FROM {table} WHERE {key}=?', [identity(event)])
        row = cursor.fetchone()
        if row is None:
            return None
        old = dict(zip([col[0] for col in cursor.description], row))
        if old.get('first_event'):
            return json.loads(old['first_event'])
        # Legacy Silver rows have no wire snapshot: compare only what was
        # actually persisted, without fabricating raw-byte provenance.
        old['metadata'] = json.loads(old.get('metadata') or '{}')
        if event.source == 'SEC':
            old.update(id=old['accession_number'], source='SEC', title=f"[{old['form_type']}] {old['company_name']}",
                       published_at=old['acceptance_time'], content_full=None,
                       content_snippet=None, is_near_duplicate=False, canonical_cluster_id=None)
            old['metadata'].update(cik=old['cik'], form_type=old['form_type'], company_name=old['company_name'])
        else:
            old['published_at'] = old['published_time']
        return old

    def write_message_batch(self, envelopes: List[KafkaEnvelope], topic_sources=None) -> Dict[str, int]:
        """Atomically account for Kafka messages; never rerun Redis dedup.

        Each new offset gets Bronze plus one outcome. Invalid events are
        durable rejections; storage/transaction errors propagate for replay.
        """
        import base64
        from config import TOPIC_RSS, TOPIC_GDELT, TOPIC_SEC, TOPIC_OBSERVATIONS
        from research.observations import SourceObservation
        sources = topic_sources if topic_sources is not None else {
            TOPIC_RSS: 'RSS', TOPIC_GDELT: 'GDELT', TOPIC_SEC: 'SEC', TOPIC_OBSERVATIONS: 'RESEARCH'}
        counts = dict.fromkeys(('bronze_inserted', 'silver_inserted', 'replayed', 'rejected', 'transport_rejected',
                                'conflicts', 'enrichment', 'redelivered', 'research_inserted'), 0)
        self.conn.execute('BEGIN TRANSACTION')
        try:
            for message in envelopes:
                try:
                    validate_transport_identity(message)
                except TransportIdentityError as exc:
                    import base64
                    import hashlib
                    headers = [[name, base64.b64encode(value).decode('ascii') if value is not None else None]
                               for name, value in message.headers or []]
                    fault_material = json.dumps({
                        'topic': repr(message.topic), 'partition': repr(message.partition),
                        'offset': repr(message.offset), 'payload': base64.b64encode(message.payload or b'').decode('ascii'),
                        'key': base64.b64encode(message.key or b'').decode('ascii'), 'headers': headers,
                    }, sort_keys=True).encode('utf-8')
                    fault_id = hashlib.sha256(fault_material).hexdigest()
                    self.conn.execute('''INSERT INTO lakehouse_transport_faults
                        (fault_id,kafka_topic,kafka_partition,kafka_offset,raw_bytes,kafka_key,kafka_headers,reason)
                        VALUES (?,?,?,?,?,?,?,?)
                        ON CONFLICT(fault_id) DO UPDATE SET
                            last_seen_at=now(),
                            occurrence_count=lakehouse_transport_faults.occurrence_count+1,
                            resolved_at=NULL,resolution_note=NULL''', [fault_id, str(message.topic),
                        message.partition if isinstance(message.partition, int) and not isinstance(message.partition, bool) else None,
                        message.offset if isinstance(message.offset, int) and not isinstance(message.offset, bool) else None,
                        message.payload, message.key, json.dumps(headers), str(exc)])
                    counts['transport_rejected'] += 1
                    continue

                transport = [message.topic, message.partition, message.offset]
                try:
                    parsed_json = json.dumps(json.loads(message.payload), allow_nan=False) if message.payload is not None else None
                except (ValueError, TypeError, UnicodeError):
                    parsed_json = None
                headers = [[name, base64.b64encode(value).decode('ascii') if value is not None else None]
                           for name, value in message.headers or []]
                inserted = self.conn.execute('''INSERT INTO bronze_events_raw
                    (kafka_topic,kafka_partition,kafka_offset,raw_payload,raw_bytes,kafka_key,kafka_headers,broker_timestamp_ms,provenance_status)
                    VALUES (?,?,?,?,?,?,?,?, 'captured')
                    ON CONFLICT (kafka_topic,kafka_partition,kafka_offset) DO NOTHING
                    RETURNING kafka_offset''', transport + [parsed_json, message.payload, message.key, json.dumps(headers), message.broker_timestamp_ms]).fetchone()
                if inserted is None:
                    counts['replayed'] += 1
                    continue
                counts['bronze_inserted'] += 1
                event_id, error = None, None
                conflicts, enrichment, provenance = {}, {}, {}
                try:
                    if message.topic not in sources:
                        raise ValueError('Unexpected Kafka topic')
                    if sources[message.topic] == 'RESEARCH':
                        event = SourceObservation.model_validate_json(message.payload)
                    else:
                        event = validate_payload(message.payload, sources[message.topic])
                except (ValueError, TypeError, UnicodeError, OverflowError) as exc:
                    status, error = 'rejected', str(exc)
                    counts['rejected'] += 1
                else:
                    event_id = event.id if isinstance(event, SourceObservation) else identity(event)
                    self.conn.execute('UPDATE bronze_events_raw SET event_id=? WHERE kafka_topic=? AND kafka_partition=? AND kafka_offset=?', [event_id] + transport)
                    old = None if isinstance(event, SourceObservation) else self._stored_event(event)
                    if isinstance(event, SourceObservation):
                        inserted = self.conn.execute('''INSERT INTO research_observations
                            (version_id,article_id,source,title,url,observed_at,source_time,
                             source_time_kind,title_provenance,nlp_eligible,content_hash,snapshot,
                             kafka_topic,kafka_partition,kafka_offset)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                            ON CONFLICT(version_id) DO NOTHING RETURNING version_id''', [
                                event.id, event.article_id, event.source, event.title, event.url,
                                event.observed_at, event.source_time, event.source_time_kind,
                                event.title_provenance, event.nlp_eligible, event.content_hash,
                                event.model_dump_json(), *transport]).fetchone()
                        status = 'research_inserted' if inserted else 'research_redelivery'
                        counts['research_inserted'] += bool(inserted)
                    elif old is None:
                        if event.source == 'SEC':
                            self.insert_sec_batch([event])
                        else:
                            self.insert_news_batch([event])
                        status = 'inserted'
                        counts['silver_inserted'] += 1
                    else:
                        conflicts, enrichment, provenance = compare_events(old, event.model_dump(mode='json'))
                        status = 'conflict' if conflicts else 'enrichment' if enrichment else 'redelivery'
                        counts['redelivered'] += 1
                        counts['conflicts'] += bool(conflicts)
                        counts['enrichment'] += bool(enrichment)
                self.conn.execute('''INSERT INTO lakehouse_message_outcomes
                    (kafka_topic,kafka_partition,kafka_offset,status,event_id,error,conflicts,enrichment,provenance)
                    VALUES (?,?,?,?,?,?,?,?,?)''', transport + [status, event_id, error, json.dumps(conflicts), json.dumps(enrichment), json.dumps(provenance)])
            self.conn.execute('COMMIT')
        except BaseException:
            self.conn.execute('ROLLBACK')
            raise
        # This clock reading occurs AFTER the data commit. A separate durable
        # marker bounds availability conservatively. Failed sealing leaves rows
        # invisible to strict research queries; replay seals without reinsertion.
        self.seal_research_observations()
        return counts

    def seal_research_observations(self):
        marker = datetime.now(timezone.utc)
        self.conn.execute('''UPDATE research_observations SET available_at=GREATEST(?, observed_at)
            WHERE available_at IS NULL''', [marker])

    def query_research_as_of(self, as_of_time, eligible_only=True):
        """Versioned, pre-dedup observations durably captured before the cutoff.

        Legacy Silver rows have no inferred research availability and are absent.
        Publication and event dates never backdate actual observation availability.
        """
        if as_of_time.tzinfo is None or as_of_time.utcoffset() is None:
            raise ValueError('Research cutoff must be timezone-aware')
        cursor = self.conn.execute('''SELECT * FROM research_observations
            WHERE available_at IS NOT NULL AND available_at <= ?
            AND (? = FALSE OR nlp_eligible)
            ORDER BY available_at, observed_at, version_id''', [as_of_time, eligible_only])
        return [dict(zip([c[0] for c in cursor.description], row)) for row in cursor.fetchall()]

    def query_news_as_of(
        self,
        as_of_time: datetime,
        ticker: Optional[str] = None,
        source: Optional[str] = None,
        exclude_duplicates: bool = True,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Return legacy news with effective_time <= as_of_time, assuming the
        supplied publication and worker observation timestamps are accurate.
        Legacy observation-time reconstruction only. It does not establish
        lakehouse or model availability. Use query_research_as_of for research.
        """
        if as_of_time.tzinfo is None:
            as_of_time = as_of_time.replace(tzinfo=timezone.utc)

        conditions = ["effective_time <= ?"]
        params: List[Any] = [as_of_time.isoformat()]

        if exclude_duplicates:
            conditions.append("is_near_duplicate = FALSE")

        if source:
            conditions.append("source = ?")
            params.append(source.upper())

        if ticker:
            conditions.append("list_contains(tickers_mentioned, ?)")
            params.append(ticker.upper())

        where_clause = " AND ".join(conditions)
        sql = f"""
        SELECT 
            id, source, title, content_snippet, url,
            event_time, published_time, ingested_time, effective_time,
            tickers_mentioned, is_near_duplicate, canonical_cluster_id
        FROM silver_financial_news
        WHERE {where_clause}
        ORDER BY effective_time DESC
        LIMIT {limit}
        """

        cursor = self.conn.execute(sql, params)
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def query_sec_as_of(
        self,
        as_of_time: datetime,
        cik: Optional[str] = None,
        form_type: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Point-in-Time query for SEC regulatory filings as-of a past timestamp.
        """
        if as_of_time.tzinfo is None:
            as_of_time = as_of_time.replace(tzinfo=timezone.utc)

        conditions = ["effective_time <= ?"]
        params: List[Any] = [as_of_time.isoformat()]

        if cik:
            conditions.append("cik = ?")
            params.append(str(cik))

        if form_type:
            conditions.append("form_type = ?")
            params.append(form_type.upper())

        where_clause = " AND ".join(conditions)
        sql = f"""
        SELECT 
            accession_number, cik, company_name, form_type,
            filing_date, acceptance_time, ingested_time, effective_time, url
        FROM silver_sec_filings
        WHERE {where_clause}
        ORDER BY effective_time DESC
        LIMIT {limit}
        """

        cursor = self.conn.execute(sql, params)
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def get_stats(self) -> Dict[str, Any]:
        """Returns row counts and metadata across Lakehouse tables."""
        news_count = self.conn.execute("SELECT COUNT(*) FROM silver_financial_news").fetchone()[0]
        canonical_count = self.conn.execute("SELECT COUNT(*) FROM silver_financial_news WHERE is_near_duplicate = FALSE").fetchone()[0]
        near_dup_count = self.conn.execute("SELECT COUNT(*) FROM silver_financial_news WHERE is_near_duplicate = TRUE").fetchone()[0]
        sec_count = self.conn.execute("SELECT COUNT(*) FROM silver_sec_filings").fetchone()[0]
        embedded_count = self.conn.execute("SELECT COUNT(*) FROM silver_financial_news WHERE embedding IS NOT NULL").fetchone()[0]

        return {
            "db_path": str(self.db_path),
            "total_news_events": news_count,
            "canonical_events": canonical_count,
            "near_duplicate_events": near_dup_count,
            "embedded_events": embedded_count,
            "sec_filings": sec_count,
            "file_size_bytes": self.db_path.stat().st_size if self.db_path.exists() else 0
        }

    def query_similar_news(
        self,
        query_embedding: list,
        as_of_time: Optional[datetime] = None,
        exclude_duplicates: bool = True,
        limit: int = 5,
        embedding_model: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Semantic similarity search using DuckDB's array_cosine_similarity.
        Finds the most semantically similar articles to a given embedding vector.

        Args:
            query_embedding: List of 384 floats (L2-normalized).
            as_of_time:      Optional PiT cutoff (only articles available at this time).
            exclude_duplicates: Whether to exclude near-duplicates.
            embedding_model: Optional EXPLICIT opt-in filter — restricts search
                             to rows stamped with this model. Never default-on:
                             rows with a different/NULL model are silently
                             excluded only when the caller passes this. Pre-
                             existing rows are backfilled at init, so NULLs
                             should not occur in practice.
            limit:           Max results to return.

        Returns:
            List of dicts with article fields + cosine similarity score.
        """
        conditions = ["embedding IS NOT NULL"]
        params: List[Any] = []

        if embedding_model:
            conditions.append("embedding_model = ?")
            params.append(embedding_model)

        if as_of_time:
            if as_of_time.tzinfo is None:
                as_of_time = as_of_time.replace(tzinfo=timezone.utc)
            conditions.append("effective_time <= ?")
            params.append(as_of_time.isoformat())

        if exclude_duplicates:
            conditions.append("is_near_duplicate = FALSE")

        where_clause = " AND ".join(conditions)
        sql = f"""
        SELECT
            id, source, title, content_snippet, url,
            effective_time,
            array_cosine_similarity(embedding, ?::FLOAT[384]) AS score
        FROM silver_financial_news
        WHERE {where_clause}
        ORDER BY score DESC
        LIMIT {limit}
        """
        params.insert(0, query_embedding)

        cursor = self.conn.execute(sql, params)
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
