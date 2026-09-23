"""Export a sealed observation snapshot and a reproducibility manifest."""
import argparse
import hashlib
import importlib.metadata
import json
import os
import platform
import subprocess
from pathlib import Path

from lakehouse.db import LakehouseManager
from lakehouse.migrations import VERSION
from research.observations import canonical_json
from research.protocol import PROTOCOL_VERSION, utc


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
    try:
        git_commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
        dirty = bool(subprocess.check_output(['git', 'status', '--porcelain'], text=True).strip())
        diff = subprocess.check_output(['git', '-c', 'core.safecrlf=false', 'diff', 'HEAD', '--binary'])
    except (OSError, subprocess.CalledProcessError):
        git_commit, dirty, diff = None, None, b''
    database_path = str(Path(db_path).resolve())
    collection_revision = os.getenv('COLLECTION_CODE_REVISION') or git_commit
    manifest = {'protocol': PROTOCOL_VERSION, 'cutoff': cutoff.isoformat(),
                'rows': len(rows), 'dataset_sha256': hashlib.sha256(rendered.encode('utf-8')).hexdigest(),
                'git_commit': git_commit, 'working_tree_dirty': dirty,
                'tracked_diff_sha256': hashlib.sha256(diff).hexdigest(),
                'collection_code_revision': collection_revision,
                'collection_worktree_state': os.getenv('COLLECTION_WORKTREE_STATE',
                                                       'dirty' if dirty else 'clean' if dirty is not None else 'unrecorded'),
                'collection_configuration': {
                    name: os.getenv(name, default)
                    for name, default in COLLECTION_CONFIG_DEFAULTS.items()
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
