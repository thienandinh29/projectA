"""Score frozen predictions against released labels under conditions A, B and C (label-aware).

Opens the sentiment labels, the story selection and the prediction ledger (read-only) and writes one
new report folder. It verifies every input's hash links to the same snapshot and eligibility selection,
recomputes each story's anchor, and refuses any mismatch. The report holds no wall-clock time, so the
same inputs always give the same bytes. Development pilot output: not a result about the model.
"""
import argparse
import hashlib
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

from research import scoring
from research.annotations import Annotation, load_selection
from research.observations import canonical_json
from research.protocol import utc

CLASS_ORDER_SOURCE = 'model label map order'


def _arrival(row):
    return utc(row['available_at']), utc(row['observed_at']), row['version_id']


def _read(path):
    return Path(path).read_bytes()


def load_labels(sentiment_dir, dataset_sha256, eligibility_selection_sha256, rows):
    """Labeled and unresolved versions from a saved sentiment selection, with assisted flags."""
    folder = Path(sentiment_dir)
    report = json.loads(_read(folder / 'selection.json'))
    if report.get('kind') != 'development_sentiment_selection_v1':
        raise ValueError('A saved development sentiment selection is required')
    if report['dataset_sha256'] != dataset_sha256 or report['eligibility_selection_sha256'] != eligibility_selection_sha256:
        raise ValueError('Sentiment selection was made from another snapshot or eligibility selection')
    for name, expected in report['files_sha256'].items():
        if hashlib.sha256(_read(folder / name)).hexdigest() != expected:
            raise ValueError(f'Sentiment selection file {name} differs from its report')
    by_id = {row['version_id']: row for row in rows}
    versions = report['versions']
    if sorted(v['version_id'] for v in versions) != sorted(by_id):
        raise ValueError('Sentiment selection does not cover exactly the eligible versions')
    decisions = {}
    for line in _read(folder / 'decisions.jsonl').splitlines():
        decision = json.loads(line)
        decisions[decision['decision_id']] = Annotation.model_validate_json(decision['annotation_json'])
    labels = {}
    for line in _read(folder / 'labels.jsonl').splitlines():
        label = json.loads(line)
        labels[label['version_id']] = label
    truth, assisted, released, statuses = {}, set(), {}, {}
    for item in versions:
        row = by_id[item['version_id']]
        if item['content_hash'] != row['content_hash']:
            raise ValueError('Sentiment selection content hash differs from the snapshot')
        statuses[item['version_id']] = item['status']
        if item['status'] != 'labeled':
            continue
        label = labels.get(item['version_id'])
        if not label or label['sentiment'] != item['sentiment'] or label['content_hash'] != row['content_hash']:
            raise ValueError('Label file does not match the selection report')
        truth[item['version_id']] = item['sentiment']
        released[item['version_id']] = label['label_available_at']
        annotation = decisions.get(item['decision_id'])
        if annotation is None or annotation.sentiment != item['sentiment']:
            raise ValueError('Label has no matching stored decision')
        if annotation.assisted:
            assisted.add(item['version_id'])
    if set(labels) != set(truth):
        raise ValueError('Label file contains versions the selection did not label')
    return {'truth': truth, 'assisted': assisted, 'released': released, 'statuses': statuses,
            'annotator': report['annotator'], 'guide_sha256': report['guide_sha256']}


def load_stories(story_dir, eligibility_dir, dataset_sha256, rows):
    """Stories from the saved selection, checked against the eligible rows, with anchors recomputed."""
    report = json.loads(_read(Path(story_dir) / 'story-groups.json'))
    if report.get('kind') != 'development_story_selection_v1':
        raise ValueError('A saved development story selection is required')
    eligible_sha256 = hashlib.sha256(_read(Path(eligibility_dir) / 'eligible.jsonl')).hexdigest()
    if report['input_dataset_sha256'] != dataset_sha256 or report['input_eligible_sha256'] != eligible_sha256:
        raise ValueError('Story selection was made from another snapshot or eligible set')
    bare = {'stories': [{k: v for k, v in s.items() if k != 'anchor_version_id'} for s in report['stories']],
            'uncertain': report['uncertain']}
    if hashlib.sha256(canonical_json(bare).encode()).hexdigest() != report['grouping_sha256']:
        raise ValueError('Story grouping does not match its recorded hash')
    by_id = {row['version_id']: row for row in rows}
    seen, stories = [], []
    for story in report['stories']:
        members = [m['version_id'] for m in story['members']]
        if any(m['content_hash'] != by_id[m['version_id']]['content_hash'] for m in story['members']):
            raise ValueError('Story member content hash differs from the snapshot')
        ordered = sorted(members, key=lambda v: _arrival(by_id[v]))
        if story['anchor_version_id'] != ordered[0]:
            raise ValueError('Story anchor is not the first available member')
        seen += members
        stories.append({'story_id': story['story_id'], 'members': ordered, 'anchor': ordered[0]})
    uncertain = [m['version_id'] for item in report['uncertain'] for m in item['members']]
    if sorted(seen + uncertain) != sorted(by_id):
        raise ValueError('Stories and uncertain versions must cover exactly the eligible versions')
    return {'stories': stories, 'uncertain': uncertain, 'released_at': report['released_at'],
            'review_id': report['review_id'], 'guide_sha256': report['guide_sha256']}


def load_predictions(ledger, model_manifest, rows):
    """Released predictions for the frozen model's revision; each input must equal the version's title."""
    raw = _read(model_manifest)
    manifest = json.loads(raw)
    if manifest.get('kind') != 'frozen_model_v1':
        raise ValueError('Not a frozen model manifest')
    manifest_sha256 = hashlib.sha256(raw).hexdigest()
    conn = sqlite3.connect(Path(ledger).resolve().as_uri() + '?mode=ro', uri=True)
    conn.row_factory = sqlite3.Row
    try:
        found = conn.execute('''SELECT p.*, r.released_at, u.model_manifest_sha256 FROM predictions p
            JOIN prediction_releases r USING (content_hash, model_revision)
            JOIN prediction_runs u ON u.run_id = p.run_id WHERE p.model_revision=?''', (manifest['revision'],)).fetchall()
    finally:
        conn.close()
    by_hash = {row['content_hash']: row for row in rows}
    predicted, released = {}, {}
    for item in found:
        row = by_hash.get(item['content_hash'])
        if row is None:
            continue
        if item['model_manifest_sha256'] != manifest_sha256:
            raise ValueError('A prediction was made with a different model manifest')
        if item['input_text'] != row['title']:
            raise ValueError('A stored prediction input differs from the snapshot title')
        if manifest['label_map'][str(item['predicted_index'])] != item['predicted_label']:
            raise ValueError('A stored prediction label does not follow the frozen label map')
        predicted[row['version_id']] = item['predicted_label']
        released[row['version_id']] = item['released_at']
    return {'predicted': predicted, 'released': released, 'revision': manifest['revision'],
            'manifest_sha256': manifest_sha256, 'classes': list(manifest['label_map'].values())}


def build_report(snapshot_dir, eligibility_dir, sentiment_dir, story_dir, ledger, model_manifest, *, seed, resamples):
    rows, _, dataset_sha256, eligibility_selection_sha256 = load_selection(snapshot_dir, eligibility_dir)
    labels = load_labels(sentiment_dir, dataset_sha256, eligibility_selection_sha256, rows)
    grouping = load_stories(story_dir, eligibility_dir, dataset_sha256, rows)
    predictions = load_predictions(ledger, model_manifest, rows)
    classes, truth = predictions['classes'], labels['truth']
    in_cohort = {v for s in grouping['stories'] for v in s['members']}
    scored = {v for v in truth if v in predictions['predicted'] and v in in_cohort}
    unassisted = scored - labels['assisted']
    evaluations = {}
    for name, keep in (('all_scorable_labels', scored), ('unassisted_labels_only', unassisted)):
        evaluations[name] = scoring.evaluate(
            grouping['stories'], {v: truth[v] for v in keep}, {v: predictions['predicted'][v] for v in keep},
            classes, seed=seed, resamples=resamples)
    earlier = sorted(v for v in scored if utc(labels['released'][v]) < utc(predictions['released'][v]))
    stamps = lambda values: [min(values), max(values)] if values else None
    return {
        'kind': 'development_score_report_v1', 'paper_ready': False,
        'inputs': {'snapshot_dataset_sha256': dataset_sha256, 'eligibility_selection_sha256': eligibility_selection_sha256,
                   'sentiment_guide_sha256': labels['guide_sha256'], 'annotator': labels['annotator'],
                   'story_review_id': grouping['review_id'], 'story_guide_sha256': grouping['guide_sha256'],
                   'model_revision': predictions['revision'], 'model_manifest_sha256': predictions['manifest_sha256'],
                   'classes': classes, 'class_order': CLASS_ORDER_SOURCE},
        'counts': {'snapshot_versions': len(load_snapshot_rows(snapshot_dir)), 'eligible_versions': len(rows),
                   'labeled': len(truth), 'unresolved': sum(1 for s in labels['statuses'].values() if s == 'unresolved'),
                   'labeled_assisted': len(labels['assisted']), 'with_prediction': len(predictions['predicted']),
                   'in_story_cohort': len(in_cohort), 'uncertain_story_membership': len(grouping['uncertain']),
                   'scored_all_labels': len(scored), 'scored_unassisted_only': len(unassisted),
                   'labeled_without_prediction': len([v for v in truth if v not in predictions['predicted']])},
        'timing': {'label_released_range': stamps([labels['released'][v] for v in scored]),
                   'story_review_released_at': grouping['released_at'],
                   'prediction_released_range': stamps([predictions['released'][v] for v in scored]),
                   'scored_versions_whose_label_was_released_before_their_prediction': len(earlier)},
        'bootstrap': {'method': 'percentile over whole stories, same draws for every condition', 'seed': seed,
                      'resamples': resamples},
        'results': evaluations,
        'limitations': [
            'Development pilot on a small cohort; the numbers describe whether the method runs, not the model.',
            'One annotator, who also assigned the story groups; not adjudicated multi-annotator ground truth.',
            'Labels were released before these predictions. The prediction tool never opened them (tested), but the '
            'order was not enforced by time as it must be in the final study.',
            'Metric conventions (undefined values, macro-F1 averaging, interval method) are development choices '
            'to be frozen before any final test.',
            'When no story has several scorable versions, conditions A, B and C coincide by construction.']}


def load_snapshot_rows(snapshot_dir):
    from research.eligibility import load_snapshot
    return load_snapshot(snapshot_dir)[0]


def _fmt(value):
    return 'n/a' if value is None else f'{value:.3f}'


def render_table(report):
    lines = ['# Pilot score table (development; not a finding about the model)', '',
             f"Model revision `{report['inputs']['model_revision']}`; classes {', '.join(report['inputs']['classes'])}.",
             '', '| Counts | |', '| --- | ---: |']
    lines += [f'| {name.replace("_", " ")} | {value} |' for name, value in report['counts'].items()]
    for name, result in report['results'].items():
        lines += ['', f"## {name.replace('_', ' ')} — {result['scorable_versions']} scorable versions, "
                      f"{result['stories_with_scorable_anchor']} of {result['stories_in_cohort']} stories with a scorable anchor", '',
                  '| Block | Condition | Total weight | Accuracy | Macro-F1 | Macro-F1 95% interval |', '| --- | --- | ---: | ---: | ---: | --- |']
        for block, title in (('paired_A_B_C', 'paired'), ('unrestricted_A_B', 'unrestricted')):
            for condition, values in result[block]['conditions'].items():
                interval = values['bootstrap']['macro_f1']['interval_95']
                text = 'n/a' if interval is None else f'{interval[0]:.3f} to {interval[1]:.3f}'
                lines.append(f"| {title} | {condition} | {values['total_weight']:.2f} | {_fmt(values['accuracy'])} | "
                             f"{_fmt(values['macro_f1'])} | {text} |")
    lines += ['', '## Limitations', ''] + [f'- {text}' for text in report['limitations']]
    return '\n'.join(lines) + '\n'


def _rounded(value):
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {k: _rounded(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_rounded(v) for v in value]
    return value


def report_files(report):
    """The exact text of each output file, keyed by file name (shared by `main` and the reproduction check)."""
    return {'score-report.json': json.dumps(report, indent=2, sort_keys=True) + '\n',
            'score-table.md': render_table(report)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot-dir', type=Path, required=True)
    parser.add_argument('--eligibility-selection', type=Path, required=True)
    parser.add_argument('--sentiment-selection', type=Path, required=True)
    parser.add_argument('--story-selection', type=Path, required=True)
    parser.add_argument('--predictions', type=Path, default=Path('/app/data/research/predictions.sqlite3'))
    parser.add_argument('--model-manifest', type=Path, default=Path('/app/docs/model-freeze-finbert-20261104.json'))
    parser.add_argument('--output', type=Path, required=True, help='New folder; refuses to overwrite')
    parser.add_argument('--seed', type=int, default=20261111)
    parser.add_argument('--resamples', type=int, default=2000)
    args = parser.parse_args()
    report = _rounded(build_report(args.snapshot_dir, args.eligibility_selection, args.sentiment_selection,
                                   args.story_selection, args.predictions, args.model_manifest,
                                   seed=args.seed, resamples=args.resamples))
    args.output.mkdir(parents=True, exist_ok=False)
    for name, text in report_files(report).items():
        (args.output / name).write_text(text, encoding='utf-8', newline='\n')
    print(json.dumps({'written': str(args.output), 'counts': report['counts']}, indent=2))


if __name__ == '__main__':
    sys.exit(main())
