"""Versioned source items captured before any live deduplication decision."""
import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, model_validator


def canonical_json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False)


def version_hash(material):
    material = dict(material)
    if material.get('source_time'):
        material['source_time'] = datetime.fromisoformat(
            material['source_time'].replace('Z', '+00:00')).astimezone(timezone.utc).isoformat()
    return hashlib.sha256(canonical_json(material).encode('utf-8')).hexdigest()


class SourceObservation(BaseModel):
    model_config = ConfigDict(extra='forbid')
    kind: Literal['source_observation_v1'] = 'source_observation_v1'
    id: str
    article_id: str
    source: Literal['RSS', 'GDELT', 'SEC']
    title: str
    url: str
    observed_at: AwareDatetime
    source_time: AwareDatetime | None = None
    source_time_kind: str
    source_time_raw: str | None = None
    title_provenance: Literal['source_feed', 'source_api', 'url_slug', 'constructed_from_filing_identity']
    source_item: dict[str, Any]
    content_hash: str
    schema_version: Literal['research-v1'] = 'research-v1'

    @model_validator(mode='after')
    def verify_identity(self):
        if not self.article_id.strip():
            raise ValueError('article_id must not be blank')
        material = self.model_dump(mode='json', exclude={'id', 'observed_at', 'content_hash'})
        digest = version_hash(material)
        if self.content_hash != digest or self.id != digest:
            raise ValueError('Observation content hash or version ID mismatch')
        return self

    @property
    def nlp_eligible(self):
        return bool(self.title.strip()) and (
            self.source == 'RSS' and self.title_provenance == 'source_feed'
            or self.source == 'GDELT' and self.title_provenance == 'source_api')

    def to_json_bytes(self):
        return self.model_dump_json().encode('utf-8')


def make_observation(*, source, article_id, title, url, source_item,
                     title_provenance, source_time=None, source_time_kind='unknown',
                     source_time_raw=None, observed_at=None):
    # Parsed source items, not a claim of original HTTP response-byte capture.
    item = json.loads(json.dumps(source_item, default=str, ensure_ascii=False, allow_nan=False))
    values = dict(kind='source_observation_v1', article_id=article_id, source=source,
                  title=title, url=url, source_item=item, title_provenance=title_provenance,
                  source_time=source_time.isoformat() if source_time else None,
                  source_time_kind=source_time_kind, source_time_raw=source_time_raw,
                  schema_version='research-v1')
    digest = version_hash(values)
    return SourceObservation(id=digest, content_hash=digest,
                             observed_at=observed_at or datetime.now(timezone.utc), **values)


# The legacy sample is explicitly a labeling pilot, not historical evidence.
# COALESCE is essential: SQL NULL metadata must not silently erase valid rows.
LEGACY_HEADLINE_ELIGIBILITY_SQL = """(
    NOT COALESCE(is_near_duplicate, FALSE)
    AND COALESCE(TRIM(title), '') <> ''
    AND COALESCE(json_extract_string(metadata, '$.nlp_eligible'), 'true') <> 'false'
    AND (source = 'RSS' OR (source = 'GDELT'
         AND COALESCE(json_extract_string(metadata, '$.title_provenance'), '') = 'source_api'))
)"""
