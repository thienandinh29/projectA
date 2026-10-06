"""Export a sealed observation snapshot and a reproducibility manifest."""
import argparse
import hashlib
import importlib.metadata
import json
import os
import platform
from pathlib import Path

from lakehouse.db import LakehouseManager
from lakehouse.migrations import VERSION
from research.observations import canonical_json
from research.protocol import PROTOCOL_VERSION, utc
from research.provenance import code_identity
from research.collection_boundary import load_baseline, reconcile_storage
from research.sessions import read_session_evidence, verify_request_links
from config import TOPIC_RSS, TOPIC_GDELT, TOPIC_SEC, TOPIC_OBSERVATIONS, LAKEHOUSE_GROUP_ID, OUTBOX_PATH


COLLECTION_CONFIG_DEFAULTS = {
    'KAFKA_BOOTSTRAP_SERVERS': 'localhost:9092',
    'TOPIC_RSS': 'financial.news.rss',
    'TOPIC_GDELT': 'financial.news.gdelt',
    'TOPIC_OBSERVATIONS': 'financial.research.observations',
    'RSS_POLL_INTERVAL_SECONDS': '180',
    'GDELT_POLL_INTERVAL_SECONDS': '900',
    'LAKEHOUSE_GROUP_ID': 'lakehouse-writer-v1',
    'LAKEHOUSE_BATCH_SIZE': '500',
    'LAKEHOUSE_BATCH_SECONDS': '2',
}


def export_dataset(db_path, output_dir, cutoff, *, baseline_path=None, drain_report_path=None,
                   development=False, session_ledger_path=None):
    cutoff = utc(cutoff)
    if development and (baseline_path or drain_report_path):
        raise ValueError('Choose development mode or a verified prospective export')
    if not development and (not baseline_path or not drain_report_path):
        raise ValueError('Prospective exports require baseline and drain report; use explicit development mode for pilot data')
    baseline = None if development else load_baseline(
        baseline_path, (TOPIC_RSS, TOPIC_GDELT, TOPIC_SEC, TOPIC_OBSERVATIONS), LAKEHOUSE_GROUP_ID)
    if baseline and cutoff < utc(baseline['captured_at_utc']):
        raise ValueError('Export cutoff precedes the collection baseline')
    database_path = str(Path(db_path).resolve())
    database_id = os.getenv('RESEARCH_DATABASE_ID', f'file:{database_path}')
    lake = LakehouseManager(db_path, read_only=True)
    try:
        rows = lake.query_research_as_of(cutoff)
        candidates = len(rows)
        storage_evidence = reconcile_storage(lake.conn, baseline, drain_report_path, database_id) if baseline else None
        if baseline:
            starts = baseline['partitions']
            captured = utc(baseline['captured_at_utc'])
            rows = [row for row in rows if
                    row['kafka_offset'] >= starts[(row['kafka_topic'], row['kafka_partition'])]['start']
                    and row['observed_at'] >= captured and row['available_at'] >= captured]
        schema_version = lake.conn.execute(
            'SELECT COALESCE(MAX(version),0) FROM lakehouse_migrations'
        ).fetchone()[0]
        if baseline and schema_version != VERSION:
            raise ValueError('Prospective export requires the current storage schema')
        total_versions, sealed_versions, unsealed_versions = lake.conn.execute('''
            SELECT COUNT(*),
                   COUNT(*) FILTER (WHERE available_at IS NOT NULL),
                   COUNT(*) FILTER (WHERE available_at IS NULL)
            FROM research_observations
        ''').fetchone()
    finally:
        lake.close()
    evidence = read_session_evidence(session_ledger_path) if session_ledger_path else None
    if evidence is None and any(json.loads(row['snapshot']).get('request_context') for row in rows):
        raise ValueError('Request-linked observations require --session-ledger evidence')
    boundary_selected = len(rows)
    invalid_runs = {run['run_id'] for run in evidence['sessions'] if run['invalidated']} if evidence else set()
    if evidence is not None:
        verify_request_links(rows, evidence)
        rows = [row for row in rows if
                (json.loads(row['snapshot']).get('collection_run') or {}).get('run_id') not in invalid_runs]
    links = verify_request_links(rows, evidence) if evidence is not None else None
    evidence_bytes = canonical_json(evidence) + '\n' if evidence is not None else None
    rendered = ''.join(canonical_json({key: value.isoformat() if hasattr(value, 'isoformat') else value
                                      for key, value in row.items()}) + '\n' for row in rows)
    runs, unknown = {}, 0
    for row in rows:
        snapshot = json.loads(row['snapshot']) if isinstance(row['snapshot'], str) else row['snapshot']
        run = snapshot.get('collection_run')
        if not run:
            unknown += 1
            continue
        run_id = run['run_id']
        if run_id in runs and runs[run_id] != run:
            raise ValueError(f'Conflicting collection provenance for run {run_id}')
        runs[run_id] = run
    manifest = {'protocol': PROTOCOL_VERSION, 'cutoff': cutoff.isoformat(),
                'rows': len(rows), 'dataset_sha256': hashlib.sha256(rendered.encode('utf-8')).hexdigest(),
                'manifest_version': 3,
                'dataset_scope': 'development' if development else 'prospective_since_baseline',
                'boundary': None if development else {
                    **{key: baseline[key] for key in ('path','sha256','captured_at_utc')},
                    'selection': 'first persisted version offset >= partition baseline AND observed_at >= baseline time AND sealed available_at <= cutoff',
                    'eligible_versions_excluded_by_boundary': candidates - boundary_selected,
                },
                'storage_reconciliation': storage_evidence,
                'session_evidence': None if evidence is None else {
                    'file': 'collection-sessions.json',
                    'sha256': hashlib.sha256(evidence_bytes.encode()).hexdigest(),
                    'basis': 'operational ledger snapshot at export, not model input or cutoff-filtered features',
                    'invalidated_run_ids': sorted(invalid_runs),
                    'versions_excluded_from_invalidated_runs': boundary_selected - len(rows),
                    **links},
                'exporter_provenance': {'code': code_identity(), 'configuration': {
                    name: os.getenv(name, default)
                    for name, default in COLLECTION_CONFIG_DEFAULTS.items()
                }},
                'collection_provenance': {
                    'basis': 'first persisted observation of each exported version',
                    'runs': [runs[key] for key in sorted(runs)],
                    'versions_with_unknown_provenance': unknown,
                },
                'database': {'id': database_id,
                             'path': database_path, 'schema_version': schema_version,
                             'expected_schema_version': VERSION,
                             'total_research_versions': total_versions,
                             'sealed_research_versions': sealed_versions,
                             'unsealed_research_versions': unsealed_versions},
                'python': platform.python_version(),
                'packages': {name: importlib.metadata.version(name) for name in ('duckdb', 'pydantic', 'datasketch', 'numpy')},
                'eligibility': 'verified headline provenance; includes duplicates and revisions',
                'availability_basis': 'conservative marker sampled after database data commit',
                'legacy_rows_included': False, 'labels_included': False,
                'paper_ready': False,
                'limitations': ['Source snapshots are parsed items, not original HTTP response bytes.',
                                'Collection runs describe first persisted versions, not every fetch or redelivery; Bronze retains wire evidence.',
                                'Declared revisions are operator assertions. Application file hashes identify collected code but do not archive it or pin dependencies.',
                                'Model versions, annotations and experiment results must be recorded separately.',
                                'Drain reports and baseline are trusted local evidence; Kafka commits cannot prove source coverage or detect operator offset resets on their own.',
                                'Prospective exports keep first-version identity: a pre-baseline version redelivered later stays excluded; a corrected post-baseline version may qualify.',
                                'A dirty worktree hash does not archive untracked source files. Freeze a clean commit before experiments.']}
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    # Never overwrite a frozen experiment artifact.
    if any((output / name).exists() for name in ('dataset.jsonl','manifest.json','collection-sessions.json')):
        raise FileExistsError('Choose a new output directory for each snapshot')
    (output / 'dataset.jsonl').write_text(rendered, encoding='utf-8', newline='\n')
    if evidence_bytes is not None:
        (output / 'collection-sessions.json').write_text(evidence_bytes, encoding='utf-8', newline='\n')
    (output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n', encoding='utf-8')
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db-path', default=os.getenv('LAKEHOUSE_DB_PATH', 'data/lakehouse.duckdb'))
    parser.add_argument('--output-dir', required=True)
    parser.add_argument('--cutoff', required=True, help='Explicit timezone-aware ISO instant')
    parser.add_argument('--baseline-path', default=os.getenv('COLLECTION_BASELINE_PATH'))
    parser.add_argument('--drain-report', help='Successful saved drain report for this quiet snapshot')
    parser.add_argument('--development', action='store_true', help='Explicit unverified development/pilot export')
    parser.add_argument('--session-ledger', default=OUTBOX_PATH,
                        help='Read-only shared SQLite collector ledger to archive with the snapshot')
    args = parser.parse_args()
    baseline_path = None if args.development else args.baseline_path
    try:
        manifest = export_dataset(args.db_path, args.output_dir, args.cutoff,
                                  baseline_path=baseline_path, drain_report_path=args.drain_report,
                                  development=args.development, session_ledger_path=args.session_ledger)
    except ValueError as exc:
        parser.error(str(exc))
    print(json.dumps(manifest, indent=2))


if __name__ == '__main__':
    main()
