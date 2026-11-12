"""Demo: how duplicate-aware weighting changes a score. All headlines, labels and predictions are INVENTED.

No live feeds, no model, no Docker: it runs the real scoring code (`research.scoring`) on a made-up
cohort of three stories so the three counting conditions can be seen to differ. It is an illustration of
the method, not data and not a result about any model.

A: every headline version counts 1.  B: the versions of one story share weight 1.
C: only the story's first available headline (its fixed anchor) counts 1.
"""
import sys

from research import scoring

CLASSES = ['positive', 'negative', 'neutral']

# story -> (true label, [(headline version, model prediction), ...]); the first version is the anchor.
COHORT = {
    'Acme profit': ('positive', [
        ('Acme Corp posts quarterly profit above forecasts', 'neutral'),
        ('Acme earnings top analyst estimates', 'positive'),
        ('Acme beats profit expectations, shares rise', 'positive'),
        ('Acme quarterly profit beats forecast', 'positive')]),
    'Borealis recall': ('negative', [
        ('Borealis Motors recalls 40,000 vehicles', 'positive')]),
    'Central bank pause': ('neutral', [
        ('Central bank leaves rates unchanged', 'neutral')]),
}


def evaluate():
    stories, truth, predicted = [], {}, {}
    for name, (label, versions) in COHORT.items():
        ids = [f'{name}/{n}' for n in range(1, len(versions) + 1)]
        stories.append({'story_id': name, 'members': ids, 'anchor': ids[0]})
        for version, (_, guess) in zip(ids, versions):
            truth[version], predicted[version] = label, guess
    return scoring.evaluate(stories, truth, predicted, CLASSES, seed=1, resamples=0)['paired_A_B_C']['conditions']


def render():
    result = evaluate()
    lines = ['INVENTED headlines, labels and predictions: an illustration of the method, not a result.', '',
             'story               versions  true      predictions', '-' * 70]
    for name, (label, versions) in COHORT.items():
        lines.append(f"{name:<20}{len(versions):<10}{label:<10}{', '.join(guess for _, guess in versions)}")
    lines += ['', 'condition  what counts                                   accuracy  macro-F1', '-' * 70]
    what = {'A': 'every headline version', 'B': 'each story once (versions share weight)',
            'C': "each story's first headline only"}
    for c in scoring.CONDITIONS:
        lines.append(f"{c:<11}{what[c]:<46}{result[c]['accuracy']:<10.3f}{result[c]['macro_f1']:.3f}")
    lines += ['', 'Acme was syndicated four times, so condition A lets one story dominate the score.',
              'The first Acme headline was misread, so C scores that story as wrong even though later copies were right.']
    return '\n'.join(lines) + '\n'


if __name__ == '__main__':
    sys.stdout.write(render())
