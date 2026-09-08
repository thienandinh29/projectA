from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Literal
from pydantic import BaseModel, Field, field_serializer


class CommonEvent(BaseModel):
    """
    Standardized Common Event Format for financial news and regulatory filings.
    Designed for Point-in-Time Lakehouse storage with zero look-ahead bias
    and soft deduplication clustering tracking.
    """
    id: str = Field(description="Unique deterministic hash or accession number for deduplication")
    source: Literal["RSS", "GDELT", "SEC"] = Field(description="Data source category")
    title: str = Field(description="Title or headline of the news / filing")
    content_snippet: Optional[str] = Field(default="", description="Summary, abstract, or short snippet of the content")
    content_full: Optional[str] = Field(default=None, description="Full article body text (populated downstream or on-demand)")
    url: Optional[str] = Field(default=None, description="Direct URL to article or SEC filing document")

    # Point-in-Time Temporal Milestones
    event_time: Optional[datetime] = Field(
        default=None,
        description="UTC timestamp of when the physical/economic event occurred"
    )
    published_at: datetime = Field(
        description="UTC timestamp of when the news source/wire published the article"
    )
    ingested_time: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC timestamp of when the pipeline ingested the event"
    )

    tickers_mentioned: List[str] = Field(default_factory=list, description="Stock tickers mentioned or identified in the event")

    # Deduplication & Clustering Tracking
    is_near_duplicate: bool = Field(default=False, description="Flag indicating if this event is a semantic/LSH near-duplicate")
    canonical_cluster_id: Optional[str] = Field(default=None, description="ID of the first canonical event in this 120-min cluster")
    embedding: Optional[List[float]] = Field(default=None, description="Dense 384-dim vector embedding from all-MiniLM-L6-v2")
    semantic_score: Optional[float] = Field(default=None, description="Cosine similarity score if semantic near-duplicate")

    metadata: Dict[str, Any] = Field(default_factory=dict, description="Source-specific extra fields (e.g. CIK, form type, tone)")
    schema_version: str = Field(default="1.0", description="Schema version tag")

    @field_serializer("published_at")
    def serialize_published_at(self, dt: datetime, _info) -> str:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()

    @field_serializer("event_time")
    def serialize_event_time(self, dt: Optional[datetime], _info) -> Optional[str]:
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()

    @field_serializer("ingested_time")
    def serialize_ingested_time(self, dt: datetime, _info) -> str:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()

    def to_json_bytes(self) -> bytes:
        """Serializes model to UTF-8 encoded JSON bytes for Kafka / Redpanda produce."""
        return self.model_dump_json().encode("utf-8")
