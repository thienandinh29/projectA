"""Prepare blank human annotations, append judgments, recover, or select by cutoff."""
import argparse
import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from research.annotations import (append_annotations, load_guide, load_selection,
                                  read_annotations, recover_pending, select_as_of)
from research.observations import canonical_json
from research.protocol import SentimentLabel, utc


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['prepare', 'record', 'recover', 'select'])
    parser.add_argument('--snapshot-dir')
    parser.add_argument('--eligibility-selection')
    parser.add_argument('--guide-file')
    parser.add_argument('--ledger', default='data/research/annotations.sqlite3')
    parser.add_argument('--output', help='Fresh template JSON file or selection directory')
    parser.add_argument('--reviews', help='Human-completed template JSON file')
    parser.add_argument('--annotator', help='Actual human identifier, also required for selection')
    parser.add_argument('--cutoff', help='Timezone-aware cutoff; label and data must precede it')
    parser.add_argument('--limit', type=int, help='Prepare first N eligible versions in chronological order')
    parser.add_argument('--version-id', action='append', help='Explicit teaching subset; record its selection reason')
    parser.add_argument('--selection-reason', help='Disclose why an explicit teaching subset was chosen')
    parser.add_argument('--allow-legacy-eligibility', action='store_true',
                        help='Explicit development-only replay of archived v1 eligibility recording times')
    args = parser.parse_args()
    if args.annotator is not None:
        args.annotator = args.annotator.strip()
    if args.action == 'recover':
        print(json.dumps(recover_pending(args.ledger), indent=2))
        return
    if not all((args.snapshot_dir, args.eligibility_selection, args.guide_file)):
        parser.error('snapshot-dir, eligibility-selection and guide-file are required')
    cutoff = None
    if args.action == 'select':
        if not args.output or not args.cutoff or not args.annotator:
            parser.error('select requires a fresh output directory, cutoff and explicit annotator')
        cutoff = utc(args.cutoff)
        if cutoff > datetime.now(timezone.utc):
            raise ValueError('Selection cutoff cannot be in the future')
    rows, _, dataset_hash, selection_hash = load_selection(
        args.snapshot_dir, args.eligibility_selection, eligibility_cutoff=cutoff,
        allow_legacy=args.allow_legacy_eligibility)
    eligibility_report = json.loads((Path(args.eligibility_selection)/'selection.json').read_bytes())
    eligibility_contract = 'legacy_precommit_v1' if eligibility_report['kind'].endswith('_v1') \
        else eligibility_report['timing_contract']
    guide = load_guide(args.guide_file)
    context = {'dataset_sha256': dataset_hash, 'eligibility_selection_sha256': selection_hash,
               'guide_sha256': guide[0]}
    if args.action == 'prepare':
        if not args.output or (args.limit is not None and args.limit <= 0):
            parser.error('prepare needs a fresh output file and a positive limit if specified')
        if args.version_id and (args.limit is not None or not (args.selection_reason or '').strip()):
            parser.error('explicit version IDs require selection-reason and cannot combine with limit')
        selected = sorted(rows, key=lambda r: (utc(r['available_at']), utc(r['observed_at']), r['version_id']))
        if args.version_id:
            wanted = set(args.version_id)
            if len(wanted) != len(args.version_id) or not wanted <= {r['version_id'] for r in rows}:
                raise ValueError('Teaching versions must be distinct included exact versions')
            selected = [r for r in selected if r['version_id'] in wanted]
        elif args.limit is not None:
            selected = selected[:args.limit]
        if not selected:
            raise ValueError('No included versions to prepare')
        template = {'kind': 'development_sentiment_template_v1', **context,
            'eligibility_timing_contract': eligibility_contract,
            'prepared_at_utc': datetime.now(timezone.utc).isoformat(),
            'selection_reason': args.selection_reason if args.version_id else
                'First eligible versions ordered by availability, observation and version ID; development only.',
            'inputs': [{'version_id': r['version_id'], 'content_hash': r['content_hash'],
                        'title': r['title'], 'source': r['source']} for r in selected],
            'annotations': [{'version_id': r['version_id'], 'content_hash': r['content_hash'],
                'subject': '', 'status': None, 'sentiment': None, 'reason': '',
                'mixed': None, 'entity_dependent': None, 'forecast': None, 'quotation': None,
                'question': None, 'assisted': None, 'assistance_note': '',
                'supersedes_decision_id': None} for r in selected]}
        path = Path(args.output)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('x', encoding='utf-8', newline='\n') as handle:
            handle.write(json.dumps(template, ensure_ascii=False, indent=2)+'\n')
        print(json.dumps({'prepared': len(selected), 'judgments_generated': 0, **context}))
    elif args.action == 'record':
        if not args.reviews or not args.annotator:
            parser.error('record requires human reviews and annotator')
        template = json.loads(Path(args.reviews).read_bytes())
        if template.get('kind') != 'development_sentiment_template_v1' or any(
                template.get(key) != value for key, value in context.items()):
            raise ValueError('Review template belongs to a different snapshot, selection or guide')
        if template.get('eligibility_timing_contract', 'legacy_precommit_v1') != eligibility_contract:
            raise ValueError('Review template eligibility timing contract mismatch')
        by_id = {r['version_id']: r for r in rows}
        inputs = template['inputs']
        annotations = template['annotations']
        if len({r['version_id'] for r in inputs}) != len(inputs) or \
                {r['version_id'] for r in inputs} != {a['version_id'] for a in annotations}:
            raise ValueError('Template inputs and annotations must cover the same distinct versions')
        for item in inputs:
            row = by_id.get(item['version_id'])
            if row is None or item != {key: row[key] for key in ('version_id', 'content_hash', 'title', 'source')}:
                raise ValueError('Displayed annotation input differs from the eligible snapshot')
        print(json.dumps(append_annotations(args.ledger, rows, dataset_hash, selection_hash,
                                           guide, args.annotator, annotations), indent=2))
    else:
        history = read_annotations(args.ledger, guide[0]) if Path(args.ledger).exists() else []
        selected = select_as_of(rows, history, cutoff, args.annotator)
        labels = [SentimentLabel(version_id=r['version_id'], content_hash=r['content_hash'],
            sentiment=r['sentiment'], label_available_at=r['label_available_at'],
            annotator=args.annotator, annotation_policy=guide[0]).model_dump(mode='json')
            for r in selected if r['status'] == 'labeled']
        # Archive history known at this cutoff; unsealed/later decisions stay in the ledger.
        row_ids = {r['version_id'] for r in rows}
        history = [d for d in history if d['annotator'] == args.annotator and d['version_id'] in row_ids
                   and d['label_available_at'] is not None and utc(d['label_available_at']) < cutoff]
        outputs = {'labels.jsonl': ''.join(canonical_json(r)+'\n' for r in labels).encode(),
                   'decisions.jsonl': ''.join(canonical_json(r)+'\n' for r in history).encode(),
                   'guide.md': guide[1].encode()}
        report = {'kind': 'development_sentiment_selection_v1', 'paper_ready': False, **context,
            'eligibility_timing_contract': eligibility_contract,
            'annotator': args.annotator, 'cutoff': cutoff.isoformat(), 'eligible_versions_at_cutoff': len(rows),
            'selected_at_utc': datetime.now(timezone.utc).isoformat(),
            'counts': dict(Counter(r['status'] for r in selected)), 'versions': selected,
            'files_sha256': {name: hashlib.sha256(raw).hexdigest() for name, raw in outputs.items()},
            'limitations': ['One explicit annotator; not adjudicated multi-annotator ground truth.',
                'Current material is development data, not a final-test certificate.',
                'Unresolved labels are not neutral and cannot enter three-class scores.',
                'Availability is sampled after decision-content commit; unsealed decisions are excluded.',
                'Later labels may score frozen earlier predictions, never supply earlier training inputs.']}
        folder = Path(args.output)
        folder.mkdir(parents=True, exist_ok=False)
        for name, raw in outputs.items():
            (folder/name).write_bytes(raw)
        (folder/'selection.json').write_text(json.dumps(report, indent=2)+'\n', encoding='utf-8', newline='\n')
        print(json.dumps(report['counts']))


if __name__ == '__main__':
    main()
