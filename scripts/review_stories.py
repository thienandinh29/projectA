"""Prepare a story-grouping worksheet, record your groups, or freeze a development selection.

Reads the snapshot, the eligibility selection and the guide; never sentiment labels or predictions.
Generates no groups: `compare:` hints only suggest what to read side by side.
"""
import argparse
import hashlib
import json
import os
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from research.protocol import utc
from research.stories import append_review, load_cohort, parse_answers, recover_pending, render_worksheet, select_as_of

LIMITATIONS = [
    'Development grouping only; not a final-test certificate or proof that no related article was missed.',
    'Reviewed alone means no related headline was found in this cohort, not that none exists elsewhere.',
    'The reviewer also assigned the sentiment labels: the worksheet hides them, but this is not independent review.',
    'Uncertain versions stay visible and are not scored as single-headline stories.']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['prepare', 'record', 'recover', 'select'])
    parser.add_argument('--snapshot-dir', type=Path)
    parser.add_argument('--selection-dir', type=Path, help='Eligibility selection holding eligible.jsonl')
    parser.add_argument('--guide-file', type=Path, default=Path('/app/docs/story-grouping-dev-v1.md'))
    parser.add_argument('--ledger', type=Path, default=Path('/app/data/research/story-groups.sqlite3'))
    parser.add_argument('--worksheet-dir', type=Path, help='prepare: new folder. record: the folder it made')
    parser.add_argument('--answers', type=Path, help='record: your completed answers file')
    parser.add_argument('--reviewer', default='', help='record: actual human reviewer')
    parser.add_argument('--supersedes', help='record: review_id this revision replaces')
    parser.add_argument('--cutoff', help='select: timezone-aware; strictly before')
    parser.add_argument('--output', type=Path, help='select: new folder')
    args = parser.parse_args()
    if args.action == 'recover':
        print(json.dumps({'released': recover_pending(args.ledger)}, indent=2))
        return
    if not args.snapshot_dir or not args.selection_dir:
        parser.error('--snapshot-dir and --selection-dir are required')
    rows, dataset_sha256, eligible_sha256 = load_cohort(args.snapshot_dir, args.selection_dir)
    if args.action == 'prepare':
        if not args.worksheet_dir:
            parser.error('prepare requires --worksheet-dir')
        text, index = render_worksheet(rows)
        args.worksheet_dir.mkdir(parents=True, exist_ok=False)
        (args.worksheet_dir / 'worksheet.txt').write_text(text, encoding='utf-8', newline='\n')
        (args.worksheet_dir / 'index.json').write_text(json.dumps(
            {'eligible_sha256': eligible_sha256, 'versions': index}, indent=2) + '\n', encoding='utf-8', newline='\n')
        print(f'Prepared {len(rows)} headlines in {args.worksheet_dir}. No groups were generated.')
    elif args.action == 'record':
        if not args.worksheet_dir or not args.answers or not args.reviewer.strip():
            parser.error('record requires --worksheet-dir, --answers and --reviewer')
        worksheet = json.loads((args.worksheet_dir / 'index.json').read_bytes())
        if worksheet['eligible_sha256'] != eligible_sha256:
            raise ValueError('Worksheet was made from a different eligibility selection')
        revision = os.getenv('COLLECTION_CODE_REVISION')
        if not revision or revision == 'unrecorded' or os.getenv('COLLECTION_WORKTREE_STATE') != 'clean':
            parser.error('record requires a recorded clean COLLECTION_CODE_REVISION')
        grouping = parse_answers(args.answers.read_text(encoding='utf-8'), worksheet['versions'])
        review_id = append_review(
            args.ledger, grouping, dataset_sha256=dataset_sha256, eligible_sha256=eligible_sha256,
            guide_sha256=hashlib.sha256(args.guide_file.read_bytes()).hexdigest(), reviewer=args.reviewer,
            tool_revision=revision, supersedes=args.supersedes)
        print(json.dumps({'review_id': review_id, 'counts': _counts(grouping)}, indent=2))
    else:
        if not args.cutoff or not args.output:
            parser.error('select requires --cutoff and --output')
        cutoff = utc(args.cutoff)
        if cutoff > datetime.now(timezone.utc):
            raise ValueError('Selection cutoff cannot be in the future')
        selected = select_as_of(args.ledger, rows, eligible_sha256, cutoff)
        report = {'kind': 'development_story_selection_v1', 'paper_ready': False, 'cutoff': cutoff.isoformat(),
                  'selected_at_utc': datetime.now(timezone.utc).isoformat(), 'input_dataset_sha256': dataset_sha256,
                  'input_eligible_sha256': eligible_sha256, 'counts': _counts(selected), 'limitations': LIMITATIONS,
                  **selected}
        args.output.mkdir(parents=True, exist_ok=False)
        (args.output / 'story-groups.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8', newline='\n')
        print(json.dumps(report['counts'], indent=2))


def _counts(grouping):
    kinds = Counter(story['kind'] for story in grouping['stories'])
    return {'multi_headline_stories': kinds['multi'], 'reviewed_alone': kinds['alone'],
            'versions_in_stories': sum(len(story['members']) for story in grouping['stories']),
            'uncertain_versions': sum(len(item['members']) for item in grouping['uncertain'])}


if __name__ == '__main__':
    main()
