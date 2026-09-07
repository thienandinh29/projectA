from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Literal
from pydantic import BaseModel, Field, field_serializer


class CommonEvent(BaseModel):
    """
    Standardized Common Event Format for financial news and regulatory filings.
    Designed for high interoperability and future migration to Avro / Protobuf.
    """
    id: str = Field(description="Unique deterministic hash or accession number for deduplication")
    source: Literal["RSS", "GDELT", "SEC"] = Field(description="Data source category")
    title: str = Field(description="Title or headline of the news / filing")
    content_snippet: Optional[str] = Field(default="", description="Summary, abstract, or short snippet of the content")
    url: Optional[str] = Field(default=None, description="Direct URL to article or SEC filing document")
    published_at: datetime = Field(description="UTC timestamp of when the event was published or filed")
    tickers_mentioned: List[str] = Field(default_factory=list, description="Stock tickers mentioned or identified in the event")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Source-specific extra fields (e.g. CIK, form type, tone)")
    schema_version: str = Field(default="1.0", description="Schema version tag")

    @field_serializer("published_at")
    def serialize_published_at(self, dt: datetime, _info) -> str:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()

    def to_json_bytes(self) -> bytes:
        """Serializes model to UTF-8 encoded JSON bytes for Kafka / Redpanda produce."""
        return self.model_dump_json().encode("utf-8")
