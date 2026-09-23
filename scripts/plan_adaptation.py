"""Prepare chronological adaptation windows from a hashed research export."""
import argparse
import hashlib
import json
from pathlib import Path
from research.protocol import assign_clusters, build_windows, utc


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset-dir', required=True)
    parser.add_argument('--labels', required=True, help='JSONL human labels; never generated pseudo-ground-truth')
    parser.add_argument('--cutoffs', nargs='+', required=True)
    parser.add_argument('--policy', choices=['frozen', 'retrain', 'sequential', 'replay'], default='replay')
    parser.add_argument('--dedup', choices=['article', 'exact', 'lexical'], default='lexical')
    parser.add_argument('--replay-budget', type=int, default=256)
    parser.add_argument('--seed', type=int, default=17)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    folder = Path(args.dataset_dir)
    raw = (folder / 'dataset.jsonl').read_bytes()
    manifest = json.loads((folder / 'manifest.json').read_text(encoding='utf-8'))
    if hashlib.sha256(raw).hexdigest() != manifest['dataset_sha256']:
        raise ValueError('Dataset hash does not match manifest')
    if utc(args.cutoffs[-1]) > utc(manifest['cutoff']):
        raise ValueError('Evaluation end exceeds the frozen snapshot cutoff')
    rows = assign_clusters([json.loads(line) for line in raw.decode('utf-8').splitlines() if line], args.dedup)
    label_bytes = Path(args.labels).read_bytes()
    labels = [json.loads(line) for line in label_bytes.decode('utf-8').splitlines() if line]
    plan = build_windows(rows, labels, args.cutoffs, args.policy, args.replay_budget, args.seed)
    if not plan['windows'][0]['initial_labels_available']:
        raise ValueError('No released initial training labels; this is not a runnable baseline')
    plan.update(dataset_sha256=manifest['dataset_sha256'], labels_sha256=hashlib.sha256(label_bytes).hexdigest(),
                clustering=args.dedup, cluster_decisions=[{'version_id': r['version_id'], 'cluster_id': r['cluster_id'],
                    'decided_at': r['cluster_decided_at']} for r in rows])
    with Path(args.output).open('x', encoding='utf-8') as handle:
        json.dump(plan, handle, indent=2)
        handle.write('\n')
    print(f"Planned {len(plan['windows'])} windows; no model training or scoring performed.")


if __name__ == '__main__':
    main()
