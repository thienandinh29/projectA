"""Prepare human reviews, append decisions, or freeze a development eligibility selection."""
import argparse
import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from research.eligibility import (load_snapshot, load_policy, append_reviews, read_decisions,
                                  select_as_of, recover_pending, confirm_legacy)
from research.observations import canonical_json
from research.protocol import utc


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['prepare', 'record', 'recover', 'confirm-legacy', 'select'])
    parser.add_argument('--snapshot-dir')
    parser.add_argument('--policy-file')
    parser.add_argument('--ledger', default='data/research/eligibility.sqlite3')
    parser.add_argument('--output', help='New JSONL review template file, or new selection directory')
    parser.add_argument('--reviews', help='Human-completed JSONL records; no sentiment labels')
    parser.add_argument('--reviewer', help='Actual human reviewer identifier')
    parser.add_argument('--cutoff', help='Explicit timezone-aware selection cutoff; strictly before')
    args = parser.parse_args()
    if args.action == 'recover':
        print(json.dumps(recover_pending(args.ledger), indent=2))
        return
    if not args.snapshot_dir or not args.policy_file:
        parser.error('snapshot-dir and policy-file are required')
    rows, manifest, dataset_hash, manifest_hash = load_snapshot(args.snapshot_dir)
    policy = load_policy(args.policy_file)
    if args.action == 'confirm-legacy':
        print(json.dumps(confirm_legacy(args.ledger, rows, policy[0]), indent=2))
        return
    if args.action == 'prepare':
        if not args.output:
            parser.error('prepare requires --output')
        path = Path(args.output)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('x', encoding='utf-8', newline='\n') as handle:
            for row in rows:
                handle.write(canonical_json({'version_id': row['version_id'], 'content_hash': row['content_hash'],
                    'english': None, 'financially_relevant': None, 'genuine_headline': None,
                    'reason': '', 'supersedes_decision_id': None})+'\n')
        print(f'Prepared {len(rows)} blank reviews. Read matching titles in the frozen dataset; no judgments generated.')
    elif args.action == 'record':
        if not args.reviews or not args.reviewer:
            parser.error('record requires --reviews and --reviewer')
        reviews = [json.loads(line) for line in Path(args.reviews).read_bytes().splitlines() if line.strip()]
        print(json.dumps(append_reviews(args.ledger, rows, dataset_hash, policy, args.reviewer, reviews), indent=2))
    else:
        if not args.output or not args.cutoff:
            parser.error('select requires --output and --cutoff')
        cutoff = utc(args.cutoff)
        if cutoff > datetime.now(timezone.utc):
            raise ValueError('Selection cutoff cannot be in the future')
        decisions = read_decisions(args.ledger, policy[0]) if Path(args.ledger).exists() else []
        version_ids = {r['version_id'] for r in rows}
        decisions = [d for d in decisions if d['version_id'] in version_ids
                     and d['availability_contract'] == 'post_content_commit_v2'
                     and d['decision_available_at'] is not None
                     and utc(d['decision_available_at']) < cutoff]
        selected = select_as_of(rows, decisions, cutoff)
        included = {item['version_id'] for item in selected if item['status']=='included'}
        rendered = ''.join(canonical_json(row)+'\n' for row in rows if row['version_id'] in included)
        history = ''.join(canonical_json(d)+'\n' for d in decisions)
        report = {'kind': 'development_eligibility_selection_v2', 'paper_ready': False,
                  'timing_contract': 'post_content_commit_v2',
                  'selected_at_utc': datetime.now(timezone.utc).isoformat(), 'cutoff': cutoff.isoformat(),
                  'snapshot_directory': args.snapshot_dir, 'input_dataset_sha256': dataset_hash,
                  'input_manifest_sha256': manifest_hash, 'input_dataset_scope': manifest.get('dataset_scope'),
                  'policy_sha256': policy[0], 'eligible_rows_sha256': hashlib.sha256(rendered.encode()).hexdigest(),
                  'decision_history_sha256': hashlib.sha256(history.encode()).hexdigest(),
                  'counts': dict(Counter(item['status'] for item in selected)), 'versions': selected,
                  'limitations': ['Development selection only; not a prospective final-test certificate.',
                      'Eligibility is not sentiment labeling, syndication validation or independent-event counting.',
                      'Data and decisions must be available strictly before the cutoff.',
                      'Historical cutoffs do not turn present-day human reviews into historical information.',
                      'Release clock is sampled after contents commit; the release marker commits separately.',
                      'Legacy judgments require explicit present-time confirmation for this timing contract.',
                      'One shared version/policy revision chain; not independent multi-reviewer adjudication.',
                      'Duplicate/revised headlines remain for later controlled grouping.']}
        output = Path(args.output)
        output.mkdir(parents=True, exist_ok=False)
        (output/'eligible.jsonl').write_text(rendered, encoding='utf-8', newline='\n')
        (output/'decisions.jsonl').write_text(history, encoding='utf-8', newline='\n')
        (output/'policy.md').write_bytes(policy[1].encode('utf-8'))
        (output/'selection.json').write_text(json.dumps(report, indent=2)+'\n', encoding='utf-8', newline='\n')
        print(json.dumps(report['counts']))


if __name__ == '__main__':
    main()
