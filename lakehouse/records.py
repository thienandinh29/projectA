"""Wire validation and field-level audit rules, independent of Redis/models."""
import json
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
from models.event import CommonEvent


class TransportIdentityError(ValueError):
    """An envelope cannot be safely acknowledged as a Kafka position."""


@dataclass(frozen=True)
class KafkaEnvelope:
    topic: str
    partition: int
    offset: int
    payload: Optional[bytes]
    broker_timestamp_ms: Optional[int] = None
    key: Optional[bytes] = None
    headers: list = field(default_factory=list)


def validate_transport_identity(message: KafkaEnvelope) -> None:
    """Validate the coordinates required to acknowledge a Kafka record."""
    if not isinstance(message.topic, str) or not message.topic:
        raise TransportIdentityError('Kafka topic must be a non-empty string')
    if isinstance(message.partition, bool) or not isinstance(message.partition, int) or message.partition < 0:
        raise TransportIdentityError('Kafka partition must be a nonnegative integer')
    if isinstance(message.offset, bool) or not isinstance(message.offset, int) or message.offset < 0:
        raise TransportIdentityError('Kafka offset must be a nonnegative integer')


def validate_payload(raw: Optional[bytes], expected_source: Optional[str] = None) -> CommonEvent:
    if raw is None:
        raise ValueError('Null Kafka payload')
    obj = json.loads(raw)
    if not isinstance(obj, dict):
        raise ValueError('Expected an event object')
    # JSON's NaN/Infinity extensions (including exponent overflow) must be
    # rejected before arbitrary metadata reaches DuckDB's strict JSON type.
    json.dumps(obj, allow_nan=False)
    for name in ('published_at', 'ingested_time', 'event_time'):
        value = obj.get(name)
        if value is None and name == 'event_time':
            continue
        if not isinstance(value, str):
            raise ValueError(f'{name} must be an explicit timezone-aware ISO timestamp')
        timestamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            raise ValueError(f'{name} is timezone-ambiguous')
        obj[name] = timestamp.astimezone(timezone.utc)
    event = CommonEvent.model_validate(obj)
    if not event.id.strip() or not event.title.strip():
        raise ValueError('Event ID and title must not be blank')
    if expected_source is not None and event.source != expected_source:
        raise ValueError('Kafka topic and event source disagree')
    if event.embedding is not None and (len(event.embedding) != 384 or
                                        not all(math.isfinite(x) for x in event.embedding)):
        raise ValueError('Embedding must contain 384 finite floats')
    if event.semantic_score is not None and not math.isfinite(event.semantic_score):
        raise ValueError('Semantic score must be finite')
    if event.source == 'SEC':
        for name, types in (('cik', (str, int)), ('form_type', (str,)), ('company_name', (str,))):
            if name in event.metadata and (not isinstance(event.metadata[name], types) or
                                            isinstance(event.metadata[name], bool)):
                raise ValueError(f'SEC {name} has an invalid identity-field type')
        accession = event.metadata.get('accession_number')
        if accession and accession != event.id:
            raise ValueError('SEC accession number disagrees with event ID')
    return event


def text(value):
    return ' '.join((value or '').split())


def instant(value):
    if not value:
        return None
    dt = value if isinstance(value, datetime) else datetime.fromisoformat(value.replace('Z', '+00:00'))
    return dt.astimezone(timezone.utc).isoformat()


def identity(event):
    return (event.metadata.get('accession_number') or event.id) if event.source == 'SEC' else event.id


def compare_events(old: dict, new: dict):
    """Return separate conflicts, enrichment, and provenance; never modify Silver.

    SEC identity and filing-text changes are always conflicts, including
    missing-to-populated text. Only supplementary annotations use enrichment.
    """
    conflicts, enrichment, provenance = {}, {}, {}
    sec = new['source'] == 'SEC'
    def record(dest, name, a, b):
        if a != b:
            dest[name] = {'old': a, 'new': b}
    record(conflicts, 'title', text(old.get('title')), text(new.get('title')))
    record(conflicts, 'published_at', instant(old.get('published_at')), instant(new.get('published_at')))
    for name in ('content_snippet', 'content_full'):
        a, b = text(old.get(name)), text(new.get(name))
        if name == 'content_snippet' and not sec and old['source'] != new['source']:
            record(provenance, name, a, b)
        elif sec:
            record(conflicts, name, a, b)
        elif not a and b:
            record(enrichment, name, a, b)
        elif a and b:
            record(conflicts, name, a, b)
        elif a and not b:
            record(conflicts if sec else provenance, name, a, b)
    a, b = instant(old.get('event_time')), instant(new.get('event_time'))
    record(enrichment if not a and b else conflicts if a and b else provenance, 'event_time', a, b)
    for name in ('is_near_duplicate', 'canonical_cluster_id'):
        a, b = old.get(name), new.get(name)
        if name == 'canonical_cluster_id':
            a, b = a or old['id'], b or new['id']
        record(conflicts, name, a, b)
    old_meta, new_meta = dict(old.get('metadata') or {}), dict(new.get('metadata') or {})
    if sec:
        for name, normalize in (('cik', lambda x: text(str(x)).zfill(10) if x is not None and text(str(x)) else None),
                                ('form_type', lambda x: text(x).upper() or None),
                                ('company_name', lambda x: text(x) or None)):
            # Missing identity is not interchangeable with a guessed default
            # (notably form 8-K). Adding/removing an identity field is audited.
            a = old_meta.pop(name, None)
            b = new_meta.pop(name, None)
            record(conflicts, name, normalize(a), normalize(b))
        old_meta.pop('accession_number', None)
        new_meta.pop('accession_number', None)
    for name in ('embedding', 'semantic_score', 'schema_version'):
        record(enrichment, name, old.get(name), new.get(name))
    normalize_tickers = lambda values: sorted({str(x).upper() for x in values or []})
    record(enrichment, 'tickers_mentioned', normalize_tickers(old.get('tickers_mentioned')),
           normalize_tickers(new.get('tickers_mentioned')))
    for name in ('source', 'url', 'ingested_time'):
        a, b = old.get(name), new.get(name)
        if name == 'ingested_time':
            a, b = instant(a), instant(b)
        record(provenance, name, a, b)
    record(provenance, 'metadata', old_meta, new_meta)
    return conflicts, enrichment, provenance
