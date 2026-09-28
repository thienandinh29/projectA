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


def export_dataset(db_path, output_dir, cutoff):
    cutoff = utc(cutoff)
    lake = LakehouseManager(db_path, read_only=True)
    try:
        rows = lake.query_research_as_of(cutoff)
        schema_version = lake.conn.execute(
            'SELECT COALESCE(MAX(version),0) FROM lakehouse_migrations'
        ).fetchone()[0]
        total_versions, sealed_versions, unsealed_versions = lake.conn.execute('''
            SELECT COUNT(*),
                   COUNT(*) FILTER (WHERE available_at IS NOT NULL),
                   COUNT(*) FILTER (WHERE available_at IS NULL)
            FROM research_observations
        ''').fetchone()
    finally:
        lake.close()
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
    database_path = str(Path(db_path).resolve())
    manifest = {'protocol': PROTOCOL_VERSION, 'cutoff': cutoff.isoformat(),
                'rows': len(rows), 'dataset_sha256': hashlib.sha256(rendered.encode('utf-8')).hexdigest(),
                'manifest_version': 2,
                'exporter_provenance': {'code': code_identity(), 'configuration': {
                    name: os.getenv(name, default)
                    for name, default in COLLECTION_CONFIG_DEFAULTS.items()
                }},
                'collection_provenance': {
                    'basis': 'first persisted observation of each exported version',
                    'runs': [runs[key] for key in sorted(runs)],
                    'versions_with_unknown_provenance': unknown,
                },
                'database': {'id': os.getenv('RESEARCH_DATABASE_ID', f'file:{database_path}'),
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
                                'A dirty worktree hash does not archive untracked source files. Freeze a clean commit before experiments.']}
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    # Never overwrite a frozen experiment artifact.
    if (output / 'dataset.jsonl').exists() or (output / 'manifest.json').exists():
        raise FileExistsError('Choose a new output directory for each snapshot')
    (output / 'dataset.jsonl').write_text(rendered, encoding='utf-8', newline='\n')
    (output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n', encoding='utf-8')
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db-path', default=os.getenv('LAKEHOUSE_DB_PATH', 'data/lakehouse.duckdb'))
    parser.add_argument('--output-dir', required=True)
    parser.add_argument('--cutoff', required=True, help='Explicit timezone-aware ISO instant')
    args = parser.parse_args()
    print(json.dumps(export_dataset(args.db_path, args.output_dir, args.cutoff), indent=2))


if __name__ == '__main__':
    main()
