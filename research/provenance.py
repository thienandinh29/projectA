"""Capture runtime evidence without treating operator-supplied revisions as proof."""
import hashlib
import importlib.metadata
import json
import os
import platform
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def code_identity(root=ROOT):
    root = Path(root)
    # Explicit application scope: no data, credentials, virtualenvs or Git internals.
    paths = list(root.glob('*.py'))
    for directory in ('workers', 'utils', 'models', 'research', 'lakehouse', 'scripts'):
        paths.extend((root / directory).rglob('*.py'))
    paths.extend(root / name for name in ('requirements.txt', 'Dockerfile') if (root / name).exists())
    files = {p.relative_to(root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
             for p in sorted(paths)}
    digest = hashlib.sha256(json.dumps(files, sort_keys=True).encode()).hexdigest()
    try:
        def git(*args):
            return subprocess.check_output(['git', *args], cwd=root, stderr=subprocess.DEVNULL)
        commit = git('rev-parse', 'HEAD').decode().strip()
        dirty = bool(git('status', '--porcelain').strip())
        diff_hash = hashlib.sha256(git('diff', 'HEAD', '--binary')).hexdigest()
    except (OSError, subprocess.CalledProcessError):
        commit = dirty = diff_hash = None
    return {'application_sha256': digest, 'application_files': files,
            'git_commit': commit, 'working_tree_dirty': dirty, 'tracked_diff_sha256': diff_hash,
            'python': platform.python_version()}


def collection_run(bootstrap_servers, profile):
    import config
    names = ('TOPIC_RSS', 'TOPIC_GDELT', 'TOPIC_SEC', 'TOPIC_OBSERVATIONS',
             'RSS_POLL_INTERVAL_SECONDS', 'GDELT_POLL_INTERVAL_SECONDS', 'SEC_POLL_INTERVAL_SECONDS',
             'REDIS_TTL_SECONDS', 'REDIS_LSH_TTL_SECONDS', 'REDIS_SEMANTIC_TTL_SECONDS',
             'SEMANTIC_COSINE_THRESHOLD', 'SEMANTIC_SCAN_LIMIT', 'DEDUP_RESERVATION_SECONDS',
             'SEC_MAX_REQUESTS_PER_SECOND')
    packages = {}
    for name in ('confluent-kafka', 'feedparser', 'requests', 'pydantic', 'redis',
                 'datasketch', 'fastembed', 'numpy'):
        try:
            packages[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            packages[name] = None
    return {'run_id': str(uuid.uuid4()), 'started_at': datetime.now(timezone.utc).isoformat(),
            'code': code_identity(), 'packages': packages,
            'configuration': {**{name: getattr(config, name) for name in names},
                              'KAFKA_BOOTSTRAP_SERVERS': bootstrap_servers, 'producer_profile': profile},
            'declared_revision': os.getenv('COLLECTION_CODE_REVISION'),
            'declared_worktree_state': os.getenv('COLLECTION_WORKTREE_STATE')}
