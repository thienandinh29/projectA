-- ============================================================================
-- POINT-IN-TIME (PiT) FINANCIAL LAKEHOUSE SCHEMA
-- Designed for DuckDB Columnar Engine with Strict 3-Timestamp Model
-- Eliminates Look-Ahead Bias & Future Data Leakage for ML / Backtesting
-- ============================================================================

-- 1. BRONZE LAYER: Raw Append-Only Kafka Stream Log
CREATE TABLE IF NOT EXISTS bronze_events_raw (
    kafka_topic VARCHAR NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    event_id VARCHAR,
    raw_payload JSON NOT NULL,
    ingested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (kafka_topic, kafka_partition, kafka_offset)
);

-- 2. SILVER LAYER: Cleaned, Point-in-Time Financial News
CREATE TABLE IF NOT EXISTS silver_financial_news (
    id VARCHAR PRIMARY KEY,
    source VARCHAR NOT NULL,                      -- 'RSS', 'GDELT'
    title VARCHAR NOT NULL,
    content_snippet VARCHAR,
    content_full VARCHAR,                         -- Full article text (for downstream NLP)
    url VARCHAR,

    -- Strict 3-Timestamp Model
    event_time TIMESTAMPTZ,                        -- Physical real-world occurrence
    published_time TIMESTAMPTZ NOT NULL,          -- When wire/source published document
    ingested_time TIMESTAMPTZ NOT NULL,           -- When pipeline captured the event
    effective_time TIMESTAMPTZ GENERATED ALWAYS AS (GREATEST(published_time, ingested_time)), -- Leak-free horizon

    -- Entity & Cluster Tracking
    tickers_mentioned VARCHAR[],
    is_near_duplicate BOOLEAN DEFAULT FALSE,      -- Flagged by LSH sliding window
    canonical_cluster_id VARCHAR,                 -- Pointer to original story in cluster (LSH or semantic)

    -- Tier 3: Semantic Embedding (all-MiniLM-L6-v2, 384-dim)
    embedding FLOAT[384],                         -- L2-normalized dense vector for semantic search
    embedding_model VARCHAR,                      -- Model that produced `embedding` (vectors from different models are not comparable)
    semantic_score FLOAT,                         -- Cosine similarity score when flagged as semantic dup

    metadata JSON,
    schema_version VARCHAR DEFAULT '1.0'
);

-- 3. SILVER LAYER: Cleaned, Point-in-Time SEC Filings
CREATE TABLE IF NOT EXISTS silver_sec_filings (
    accession_number VARCHAR PRIMARY KEY,
    cik VARCHAR NOT NULL,
    company_name VARCHAR NOT NULL,
    form_type VARCHAR NOT NULL,                   -- '8-K', '10-Q', '10-K'
    filing_date DATE,

    -- SEC Timestamps
    acceptance_time TIMESTAMPTZ NOT NULL,         -- SEC EDGAR official acceptance timestamp
    ingested_time TIMESTAMPTZ NOT NULL,           -- Pipeline capture timestamp
    effective_time TIMESTAMPTZ GENERATED ALWAYS AS (GREATEST(acceptance_time, ingested_time)),

    url VARCHAR,
    metadata JSON,
    schema_version VARCHAR DEFAULT '1.0'
);

-- 4. B-Tree Indexes for Sub-Second Time-Travel Lookups
CREATE INDEX IF NOT EXISTS idx_news_effective_time ON silver_financial_news (effective_time);
CREATE INDEX IF NOT EXISTS idx_sec_effective_time ON silver_sec_filings (effective_time);
CREATE INDEX IF NOT EXISTS idx_news_source ON silver_financial_news (source);

-- 5. Helper Views for Quant Backtesting & Modeling
-- Canonical news stream (strictly novel events, zero duplicates, leak-free)
CREATE OR REPLACE VIEW view_canonical_news_pit AS
SELECT 
    id,
    source,
    title,
    content_snippet,
    url,
    effective_time,
    tickers_mentioned,
    metadata
FROM silver_financial_news
WHERE is_near_duplicate = FALSE;
