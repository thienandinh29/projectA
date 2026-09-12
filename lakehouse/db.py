import os
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
import duckdb
from models.event import CommonEvent
from models.embedder import MODEL_NAME

logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "lakehouse.duckdb"
SCHEMA_SQL_PATH = Path(__file__).resolve().parent / "schema.sql"


class LakehouseManager:
    """
    DuckDB Point-in-Time Lakehouse Manager.
    Provides schema migration, batch insertions, and time-travel query interfaces
    guaranteed to eliminate future data leakage during backtesting.
    """
    def __init__(self, db_path: Optional[str] = None, read_only: bool = False):
        self.db_path = Path(db_path) if db_path else DEFAULT_DB_PATH
        self.read_only = read_only
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = duckdb.connect(str(self.db_path), read_only=read_only)
        if not read_only:
            self._init_schema()

    def _init_schema(self):
        """Initializes tables, indexes, and views from schema.sql (Idempotent)."""
        with open(SCHEMA_SQL_PATH, "r", encoding="utf-8") as f:
            ddl_statements = f.read()
        self.conn.execute(ddl_statements)

        # Idempotent column migrations for existing databases
        migration_statements = [
            "ALTER TABLE silver_financial_news ADD COLUMN IF NOT EXISTS embedding FLOAT[384];",
            "ALTER TABLE silver_financial_news ADD COLUMN IF NOT EXISTS semantic_score FLOAT;",
            # Embedding versioning: vectors from different models are not
            # comparable. Stamp the producing model on every row.
            "ALTER TABLE silver_financial_news ADD COLUMN IF NOT EXISTS embedding_model VARCHAR;",
        ]
        for stmt in migration_statements:
            try:
                self.conn.execute(stmt)
            except Exception as e:
                logger.debug(f"Migration note: {e}")

        # Backfill: every embedding in this database was produced by the one
        # model this pipeline has ever run (MODEL_NAME), so stamping NULL rows
        # is safe and prevents them from being excluded by model-filtered
        # queries. Idempotent: only touches still-NULL rows.
        try:
            stamped = self.conn.execute(
                "UPDATE silver_financial_news SET embedding_model = ? "
                "WHERE embedding IS NOT NULL AND embedding_model IS NULL",
                [MODEL_NAME],
            ).fetchone()
            if stamped and stamped[0]:
                logger.info(f"Backfilled embedding_model={MODEL_NAME} on {stamped[0]} rows")
        except Exception as e:
            logger.debug(f"Backfill note: {e}")

        logger.info(f"Initialized Lakehouse schema at {self.db_path}")

    def close(self):
        """Closes the DuckDB connection."""
        if self.conn:
            self.conn.close()

    # ─────────────────────────────────────────────────────────────────────────
    #  BATCH INSERTIONS (SILVER LAYER)
    # ─────────────────────────────────────────────────────────────────────────

    def insert_news_batch(self, events: List[CommonEvent]) -> int:
        """
        Batch inserts CommonEvent instances into silver_financial_news.
        Uses INSERT OR REPLACE on primary key `id`.
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
                e.schema_version
            ))

        sql = """
        INSERT OR REPLACE INTO silver_financial_news (
            id, source, title, content_snippet, content_full, url,
            event_time, published_time, ingested_time,
            tickers_mentioned, is_near_duplicate, canonical_cluster_id,
            embedding, semantic_score, embedding_model,
            metadata, schema_version
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.conn.executemany(sql, rows)
        logger.info(f"Inserted {len(rows)} events into silver_financial_news")
        return len(rows)

    def insert_sec_batch(self, events: List[CommonEvent]) -> int:
        """
        Batch inserts SEC CommonEvent instances into silver_sec_filings.
        Uses INSERT OR REPLACE on primary key `accession_number`.
        """
        if not events:
            return 0

        rows = []
        for e in events:
            meta = e.metadata or {}
            accession_no = meta.get("accession_number", e.id)
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
                e.schema_version
            ))

        sql = """
        INSERT OR REPLACE INTO silver_sec_filings (
            accession_number, cik, company_name, form_type,
            filing_date, acceptance_time, ingested_time,
            url, metadata, schema_version
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.conn.executemany(sql, rows)
        logger.info(f"Inserted {len(rows)} filings into silver_sec_filings")
        return len(rows)

    # ─────────────────────────────────────────────────────────────────────────
    #  POINT-IN-TIME (PiT) TIME-TRAVEL QUERY HELPERS
    # ─────────────────────────────────────────────────────────────────────────

    def query_news_as_of(
        self,
        as_of_time: datetime,
        ticker: Optional[str] = None,
        source: Optional[str] = None,
        exclude_duplicates: bool = True,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Point-in-Time query: Returns only news items that were legally and physically
        available at `as_of_time` (effective_time <= as_of_time).
        Guarantees ZERO future data leakage for backtesting.
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

        df = self.conn.execute(sql, params).df()
        return df.to_dict(orient="records")

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

        df = self.conn.execute(sql, params).df()
        return df.to_dict(orient="records")

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

        df = self.conn.execute(sql, params).df()
        return df.to_dict(orient="records")

