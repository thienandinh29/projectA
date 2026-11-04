"""Freeze one exact Hugging Face model revision as hashed local files plus a manifest.

Accepts only a full commit SHA, checks every download against the hub's own LFS SHA-256 or git
blob id, and publishes the directory only after all checks pass. It refuses to overwrite. The label
order is read from the model's config, never assumed. Safetensors only: no pickle file is fetched,
and nothing here loads or runs the model. This proves which bytes were frozen, not model quality.
"""
import argparse
import hashlib
import json
import os
import re
import shutil
import struct
import sys
from datetime import datetime, timezone
from pathlib import Path

HUB = 'https://huggingface.co'
FILES = ('README.md', 'config.json', 'model.safetensors', 'special_tokens_map.json', 'tokenizer_config.json', 'vocab.txt')
SENTIMENT_LABELS = {'positive', 'negative', 'neutral'}
CHUNK = 1024 * 1024


def _hash_file(path, algorithm, prefix=b''):
    digest = hashlib.new(algorithm, prefix)
    with open(path, 'rb') as handle:
        for chunk in iter(lambda: handle.read(CHUNK), b''):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_file(path):
    return _hash_file(path, 'sha256')


def git_blob_sha1(path):
    """The id git, and so the hub, gives a file that is not stored in LFS."""
    return _hash_file(path, 'sha1', f'blob {Path(path).stat().st_size}\0'.encode())


def http_get(url, stream=False):
    import requests
    response = requests.get(url, stream=stream, timeout=60, allow_redirects=True)
    response.raise_for_status()
    return response


def fetch_info(repo, revision, get=http_get):
    """Hub metadata for the exact revision; a mismatch means the pin did not resolve to itself."""
    if not re.fullmatch(r'[0-9a-f]{40}', revision):
        raise ValueError('Revision must be a full 40-character commit SHA, not a branch or tag')
    info = get(f'{HUB}/api/models/{repo}/revision/{revision}?blobs=true').json()
    if info.get('sha') != revision:
        raise ValueError(f"Hub resolved {revision} to {info.get('sha')}")
    return info


def _expected(info, name):
    for sibling in info.get('siblings', []):
        if sibling.get('rfilename') == name:
            return sibling
    raise ValueError(f'{name} is not in this revision')


def check_download(path, sibling):
    """Raises unless size and the hub's hash match. Returns the hub hash that was checked."""
    name = sibling['rfilename']
    if Path(path).stat().st_size != sibling.get('size'):
        raise ValueError(f"{name}: size {Path(path).stat().st_size} is not the hub's {sibling.get('size')}")
    lfs = (sibling.get('lfs') or {}).get('sha256')
    actual, wanted, kind = (sha256_file(path), lfs, 'lfs_sha256') if lfs else (git_blob_sha1(path), sibling.get('blobId'), 'git_blob_sha1')
    if not wanted or actual != wanted:
        raise ValueError(f"{name}: {kind} {actual} is not the hub's {wanted}")
    return {kind: wanted}


def read_labels(config):
    """Index-to-label map from config.json, in index order. Must be exactly the three sentiment labels."""
    raw = config.get('id2label')
    if not isinstance(raw, dict) or not raw:
        raise ValueError('config.json has no id2label')
    labels = {int(index): label for index, label in raw.items()}
    if sorted(labels) != list(range(len(labels))) or set(labels.values()) != SENTIMENT_LABELS or len(labels) != 3:
        raise ValueError(f'Unexpected label map: {raw}')
    if {label: int(index) for index, label in raw.items()} != config.get('label2id'):
        raise ValueError('id2label and label2id disagree')
    return {str(index): labels[index] for index in sorted(labels)}


def read_safetensors_header(path):
    """Tensor names and shapes from the file header; parses JSON only, loads no weights."""
    with open(path, 'rb') as handle:
        size = struct.unpack('<Q', handle.read(8))[0]
        if size > Path(path).stat().st_size:
            raise ValueError('safetensors header is longer than the file')
        header = json.loads(handle.read(size))
    return {name: value['shape'] for name, value in header.items() if name != '__metadata__'}


def check_classifier(shapes, config, labels):
    """The weights must really output one score per label in the config."""
    wanted = [len(labels), config.get('hidden_size')]
    if shapes.get('classifier.weight') != wanted:
        raise ValueError(f"classifier.weight is {shapes.get('classifier.weight')}, expected {wanted}")


def build_manifest(repo, revision, info, files, labels, shapes, config, tokenizer, frozen_at, tool_revision):
    return {
        'kind': 'frozen_model_v1',
        'repository': repo,
        'revision': revision,
        'revision_last_modified': info.get('lastModified'),
        'license': (info.get('cardData') or {}).get('license'),
        'frozen_at_utc': frozen_at.astimezone(timezone.utc).isoformat(),
        'tool_revision': tool_revision,
        'files': files,
        'label_map': labels,
        'architecture': config.get('architectures'),
        'tokenizer': {key: tokenizer.get(key) for key in ('do_lower_case', 'model_max_length', 'name_or_path')},
        'tensor_count': len(shapes),
        'classifier_weight_shape': shapes['classifier.weight'],
    }


def freeze(repo, revision, root, *, get=http_get, now=None, tool_revision=None):
    """Download, check and publish <root>/<owner>__<name>@<sha>/ and its manifest beside it."""
    info = fetch_info(repo, revision, get)
    target = Path(root) / f"{repo.replace('/', '__')}@{revision}"
    manifest_path = target.with_name(target.name + '.manifest.json')
    partial = target.with_name(target.name + '.partial')
    if target.exists() or manifest_path.exists():
        raise FileExistsError(f'{target} is already frozen; a frozen model is never replaced')
    partial.mkdir(parents=True)  # Fails if an interrupted run left one: inspect it, do not reuse it.
    try:
        files = {}
        for name in FILES:
            sibling = _expected(info, name)
            path = partial / name
            with get(f'{HUB}/{repo}/resolve/{revision}/{name}', stream=True) as response, open(path, 'xb') as handle:
                for chunk in response.iter_content(CHUNK):
                    handle.write(chunk)
            checked = check_download(path, sibling)
            files[name] = {'size': path.stat().st_size, 'sha256': sha256_file(path), **checked}
        config = json.loads((partial / 'config.json').read_text(encoding='utf-8'))
        tokenizer = json.loads((partial / 'tokenizer_config.json').read_text(encoding='utf-8'))
        labels = read_labels(config)
        shapes = read_safetensors_header(partial / 'model.safetensors')
        check_classifier(shapes, config, labels)
        manifest = build_manifest(repo, revision, info, files, labels, shapes, config, tokenizer,
                                  now or datetime.now(timezone.utc), tool_revision)
        partial.rename(target)
    except BaseException:
        shutil.rmtree(partial, ignore_errors=True)
        raise
    with manifest_path.open('x', encoding='utf-8', newline='\n') as handle:
        handle.write(json.dumps(manifest, indent=2, ensure_ascii=False) + '\n')
    return target, manifest_path


def verify_frozen(directory, manifest):
    """Problems with a frozen directory: changed, missing or extra files. Empty means exact bytes."""
    directory = Path(directory)
    problems = [f'extra file: {path.name}' for path in sorted(directory.iterdir()) if path.name not in manifest['files']]
    for name, recorded in sorted(manifest['files'].items()):
        path = directory / name
        if not path.is_file():
            problems.append(f'missing file: {name}')
        elif sha256_file(path) != recorded['sha256']:
            problems.append(f'changed file: {name}')
    return problems


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', default='ProsusAI/finbert')
    parser.add_argument('--revision', required=True, help='Full commit SHA')
    parser.add_argument('--root', type=Path, default=Path('/app/data/models'))
    parser.add_argument('--verify', type=Path, help='Check the frozen directory against this manifest; download nothing')
    args = parser.parse_args()
    if args.verify:
        manifest = json.loads(args.verify.read_text(encoding='utf-8'))
        problems = verify_frozen(args.root / f"{manifest['repository'].replace('/', '__')}@{manifest['revision']}", manifest)
        print(json.dumps({'exact_bytes': not problems, 'problems': problems}, indent=2))
        raise SystemExit(1 if problems else 0)
    target, manifest_path = freeze(args.repo, args.revision, args.root, tool_revision=os.getenv('COLLECTION_CODE_REVISION'))
    print(json.dumps({'frozen': str(target), 'manifest': str(manifest_path)}, indent=2))


if __name__ == '__main__':
    sys.exit(main())
