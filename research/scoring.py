"""Weighted three-class scoring under the counting draft's conditions A, B and C.

Pure arithmetic over versions, stories, labels and predictions; it opens no files. A: every
scorable version weighs 1. B: each story's scorable members share weight 1. C: the story's
pre-fixed first available member weighs 1 and the story scores nothing if that anchor is not
scorable (a later member is never substituted). The paired result restricts A, B and C to the same
stories, those whose anchor is scorable. The unrestricted result shows A and B on every story with a
scorable member. Metrics come from weighted confusion matrices, not averaged per-example scores.

Conventions to freeze before any final test: precision is undefined when nothing is predicted as a
class, recall when the class has no true weight, F1 when both are zero; macro-F1 averages the defined
F1 values; the interval is a seeded percentile bootstrap over whole stories.
"""
import math
import random

CONDITIONS = ('A', 'B', 'C')


def story_weights(members, anchor, scorable):
    """Per-version weights for one story. `members` are its version IDs; unscorable ones weigh nothing."""
    present = [v for v in members if v in scorable]
    return {'A': {v: 1.0 for v in present},
            'B': {v: 1.0 / len(present) for v in present},
            'C': {anchor: 1.0} if anchor in scorable else {}}


def confusion(weights, truth, predicted, classes):
    matrix = {t: {p: 0.0 for p in classes} for t in classes}
    for version, weight in weights.items():
        matrix[truth[version]][predicted[version]] += weight
    return matrix


def add(total, matrix, times=1):
    for t, row in matrix.items():
        for p, value in row.items():
            total[t][p] += value * times


def blank(classes):
    return {t: {p: 0.0 for p in classes} for t in classes}


def metrics(matrix, classes):
    """Accuracy, per-class precision/recall/F1 (None where undefined) and macro-F1 over defined F1s."""
    total = sum(sum(row.values()) for row in matrix.values())
    per_class, defined = {}, []
    for c in classes:
        tp = matrix[c][c]
        actual = sum(matrix[c].values())
        predicted = sum(matrix[t][c] for t in classes)
        f1 = 2 * tp / (actual + predicted) if actual + predicted > 0 else None
        per_class[c] = {'precision': tp / predicted if predicted > 0 else None,
                        'recall': tp / actual if actual > 0 else None, 'f1': f1,
                        'true_weight': actual, 'predicted_weight': predicted}
        if f1 is not None:
            defined.append(f1)
    return {'total_weight': total,
            'accuracy': sum(matrix[c][c] for c in classes) / total if total > 0 else None,
            'macro_f1': sum(defined) / len(defined) if defined else None,
            'classes_in_macro_f1': [c for c in classes if per_class[c]['f1'] is not None],
            'per_class': per_class}


def _interval(values, level=0.95):
    values = sorted(values)
    if not values:
        return None
    tail = (1 - level) / 2
    return [values[int(tail * (len(values) - 1))], values[math.ceil((1 - tail) * (len(values) - 1))]]


def bootstrap(per_story, conditions, classes, seed, resamples):
    """Resample whole stories with the same draws for every condition, so differences are paired."""
    rng = random.Random(seed)
    n = len(per_story)
    draws = {c: {'macro_f1': [], 'accuracy': []} for c in conditions}
    differences = {f'{c}-A': [] for c in conditions if c != 'A' and 'A' in conditions}
    for _ in range(resamples if n else 0):
        picks = [rng.randrange(n) for _ in range(n)]
        macro = {}
        for c in conditions:
            total = blank(classes)
            for index in picks:
                add(total, per_story[index][c])
            result = metrics(total, classes)
            macro[c] = result['macro_f1']
            for key in ('macro_f1', 'accuracy'):
                if result[key] is not None:
                    draws[c][key].append(result[key])
        for name in differences:
            c = name[0]
            if macro[c] is not None and macro['A'] is not None:
                differences[name].append(macro[c] - macro['A'])
    return ({c: {k: {'interval_95': _interval(v), 'valid_resamples': len(v)} for k, v in kinds.items()}
             for c, kinds in draws.items()},
            {name: {'interval_95': _interval(v), 'valid_resamples': len(v)} for name, v in differences.items()})


def _block(stories, conditions, truth, predicted, classes, weights, seed, resamples):
    per_story = []
    for story in stories:
        per_story.append({c: confusion(weights[story['story_id']][c], truth, predicted, classes) for c in conditions})
    result = {}
    for c in conditions:
        total = blank(classes)
        for matrices in per_story:
            add(total, matrices[c])
        result[c] = {**metrics(total, classes), 'confusion_true_by_predicted': total,
                     'versions_with_weight': sum(len([w for w in weights[s['story_id']][c].values() if w > 0]) for s in stories)}
    intervals, differences = bootstrap(per_story, conditions, classes, seed, resamples)
    for c in conditions:
        result[c]['bootstrap'] = intervals[c]
    return {'stories': len(stories), 'conditions': result, 'macro_f1_difference_bootstrap': differences}


def evaluate(stories, truth, predicted, classes, *, seed, resamples):
    """`stories`: dicts with story_id, members (version IDs) and anchor. `truth` holds labeled versions only."""
    scorable = set(truth) & set(predicted)
    weights = {s['story_id']: story_weights(s['members'], s['anchor'], scorable) for s in stories}
    with_scorable = [s for s in stories if weights[s['story_id']]['A']]
    paired = [s for s in with_scorable if s['anchor'] in scorable]
    return {
        'scorable_versions': len([v for s in stories for v in s['members'] if v in scorable]),
        'stories_in_cohort': len(stories), 'stories_with_a_scorable_member': len(with_scorable),
        'stories_with_scorable_anchor': len(paired),
        'stories_with_several_scorable_members': len([s for s in with_scorable if len(weights[s['story_id']]['A']) > 1]),
        'paired_A_B_C': _block(paired, CONDITIONS, truth, predicted, classes, weights, seed, resamples),
        'unrestricted_A_B': _block(with_scorable, ('A', 'B'), truth, predicted, classes, weights, seed, resamples)}
