"""Validated prospective collection boundaries and stored delivery evidence."""
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path


def load_baseline(path, topics, writer_group):
    raw = Path(path).read_bytes()
    document = json.loads(raw)
    if not isinstance(document, dict) or document.get('kind') != 'verified_collection_baseline_v1':
        raise ValueError('Baseline kind is not active and supported')
    if document.get('activation_gate', {}).get('active') is not True:
        raise ValueError('Baseline activation gate is not active')
    captured = datetime.fromisoformat(document['captured_at_utc'].replace('Z', '+00:00'))
    if captured.tzinfo is None:
        raise ValueError('Baseline timestamp must be timezone-aware')
    kafka = document.get('kafka', {})
    if kafka.get('writer_group') != writer_group:
        raise ValueError('Baseline writer group does not match configured writer group')
    configured_topics, partitions = set(topics), {}
    for item in kafka.get('partitions', []):
        key = (item.get('topic'), item.get('partition'))
        start, committed = item.get('post_baseline_start_offset'), item.get('writer_committed_next_offset')
        if key in partitions:
            raise ValueError(f'Duplicate baseline partition: {key}')
        if key[0] not in configured_topics or type(key[1]) is not int or key[1] < 0:
            raise ValueError(f'Invalid baseline partition: {key}')
        if type(start) is not int or start < 0:
            raise ValueError(f'Invalid baseline start offset: {key}')
        if committed is not None and (type(committed) is not int or committed < 0 or committed > start):
            raise ValueError(f'Invalid baseline committed offset: {key}')
        partitions[key] = {'start': start, 'committed_at_capture': committed}
    if {topic for topic, _ in partitions} != configured_topics:
        raise ValueError('Baseline topics do not match configured topics')
    return {'path': str(path), 'sha256': hashlib.sha256(raw).hexdigest(),
            'captured_at_utc': captured.astimezone(timezone.utc).isoformat(),
            'partitions': partitions, 'database': document.get('duckdb', {})}


def reconcile_storage(conn, baseline, report_path, database_id):
    """Read-only check after stopping the writer; commits alone are insufficient.

    These topics use delete retention and nontransactional/idempotent producers.
    Require every integer delivery offset in each declared range to have raw
    bytes and a matching processed outcome. Compacted/transactional streams need
    a different protocol, never an automatic relaxation of this check.
    """
    raw = Path(report_path).read_bytes()
    report = json.loads(raw)
    if report.get('drained') is not True or report.get('pending') != 0:
        raise ValueError('A successful empty-outbox drain report is required')
    if report.get('baseline', {}).get('sha256') != baseline['sha256']:
        raise ValueError('Drain report baseline hash mismatch')
    if baseline['database'].get('database_id') != database_id or report.get('database_id') != database_id:
        raise ValueError('Baseline/drain database identity mismatch')
    expected = {f'{t}:{p}': (t, p) for t, p in baseline['partitions']}
    progress = report.get('writer_progress', {})
    if set(progress) != set(expected):
        raise ValueError('Drain report partition set mismatch')
    ranges = []
    for label, key in expected.items():
        part = progress[label]
        start, high = baseline['partitions'][key]['start'], part.get('broker_high_watermark_next_offset')
        commit = part.get('writer_committed_next_offset')
        if type(high) is not int or high < start or part.get('baseline_next_offset') != start:
            raise ValueError(f'Invalid reported range: {label}')
        if part.get('lag') != 0 or (high > start and (type(commit) is not int or commit != high)):
            raise ValueError(f'Incomplete reported writer progress: {label}')
        counts = conn.execute('''SELECT COUNT(*),
            COUNT(*) FILTER (WHERE b.raw_bytes IS NOT NULL AND o.status IS NOT NULL
                AND o.status <> 'legacy_unprocessed' AND b.event_id IS NOT DISTINCT FROM o.event_id
                AND (o.status NOT IN ('research_inserted','research_redelivery') OR r.version_id IS NOT NULL))
            FROM bronze_events_raw b LEFT JOIN lakehouse_message_outcomes o
            USING(kafka_topic,kafka_partition,kafka_offset)
            LEFT JOIN research_observations r ON r.version_id=o.event_id
            WHERE b.kafka_topic=? AND b.kafka_partition=? AND b.kafka_offset>=? AND b.kafka_offset<?''',
            [*key, start, high]).fetchone()
        if counts != (high-start, high-start):
            raise ValueError(f'Stored delivery range is incomplete: {label}; expected {high-start}, found {counts}')
        # Detect deliveries received after this report or using an undeclared partition.
        beyond = conn.execute('''SELECT COUNT(*) FROM bronze_events_raw
            WHERE kafka_topic=? AND kafka_partition=? AND kafka_offset>=?''', [*key, high]).fetchone()[0]
        if beyond:
            raise ValueError(f'Drain report is stale for database deliveries: {label}')
        ranges.append({'topic': key[0], 'partition': key[1], 'start': start, 'end': high,
                       'bronze_rows': counts[0], 'processed_outcomes': counts[1]})
    for topic, partition in conn.execute('SELECT DISTINCT kafka_topic,kafka_partition FROM research_observations').fetchall():
        if (topic, partition) not in baseline['partitions']:
            raise ValueError('Stored research observations use a partition outside the baseline')
    if conn.execute('SELECT COUNT(*) FROM lakehouse_transport_faults WHERE resolved_at IS NULL').fetchone()[0]:
        raise ValueError('Unresolved transport faults prevent prospective export')
    if conn.execute('SELECT COUNT(*) FROM research_observations WHERE available_at IS NULL').fetchone()[0]:
        raise ValueError('Unsealed research observations prevent prospective export')
    stored_counts = {}
    for table in ('bronze_events_raw', 'lakehouse_message_outcomes', 'research_observations',
                  'silver_financial_news', 'silver_sec_filings'):
        count = conn.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
        minimum = baseline['database'].get('counts', {}).get(table, 0)
        if type(minimum) is not int or minimum < 0 or count < minimum:
            raise ValueError(f'Database count fell below the saved baseline: {table}')
        stored_counts[table] = count
    return {'drain_report_sha256': hashlib.sha256(raw).hexdigest(), 'ranges': ranges,
            'current_database_counts': stored_counts,
            'basis': 'exact topic/partition/offset ranges with raw Bronze bytes and matching processed outcomes'}
