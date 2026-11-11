"""Scoring arithmetic is checked against hand-computed examples, never against model output."""
import unittest

from research import scoring

CLASSES = ('positive', 'negative', 'neutral')


class TestWeights(unittest.TestCase):
    def test_three_versions_of_one_story_match_the_counting_draft(self):
        weights = scoring.story_weights(['v1', 'v2', 'v3'], 'v1', {'v1', 'v2', 'v3'})
        self.assertEqual(weights['A'], {'v1': 1.0, 'v2': 1.0, 'v3': 1.0})
        self.assertEqual(weights['B'], {v: 1 / 3 for v in ('v1', 'v2', 'v3')})
        self.assertEqual(weights['C'], {'v1': 1.0})

    def test_unscorable_anchor_scores_nothing_and_is_never_replaced(self):
        weights = scoring.story_weights(['v1', 'v2', 'v3'], 'v1', {'v2', 'v3'})
        self.assertEqual(weights['C'], {})
        self.assertEqual(weights['B'], {'v2': 0.5, 'v3': 0.5})
        self.assertEqual(weights['A'], {'v2': 1.0, 'v3': 1.0})


class TestMetrics(unittest.TestCase):
    def matrix(self):
        truth = {'v1': 'positive', 'v2': 'positive', 'v3': 'negative', 'v4': 'neutral'}
        predicted = {'v1': 'positive', 'v2': 'negative', 'v3': 'negative', 'v4': 'neutral'}
        return scoring.confusion({v: 1.0 for v in truth}, truth, predicted, CLASSES)

    def test_hand_computed_confusion_and_scores(self):
        result = scoring.metrics(self.matrix(), CLASSES)
        self.assertEqual(result['accuracy'], 0.75)
        positive, negative, neutral = (result['per_class'][c] for c in CLASSES)
        self.assertEqual((positive['precision'], positive['recall']), (1.0, 0.5))
        self.assertAlmostEqual(positive['f1'], 2 / 3)
        self.assertEqual((negative['precision'], negative['recall']), (0.5, 1.0))
        self.assertAlmostEqual(negative['f1'], 2 / 3)
        self.assertEqual(neutral['f1'], 1.0)
        self.assertAlmostEqual(result['macro_f1'], 7 / 9)

    def test_weighted_matrix_uses_weights_not_counts(self):
        truth = {'a': 'positive', 'b': 'negative'}
        predicted = {'a': 'positive', 'b': 'positive'}
        result = scoring.metrics(scoring.confusion({'a': 1.0, 'b': 3.0}, truth, predicted, CLASSES), CLASSES)
        self.assertEqual(result['accuracy'], 0.25)
        self.assertEqual(result['per_class']['positive']['precision'], 0.25)

    def test_undefined_values_are_none_and_stay_out_of_the_macro_average(self):
        truth, predicted = {'a': 'positive', 'b': 'positive'}, {'a': 'positive', 'b': 'negative'}
        result = scoring.metrics(scoring.confusion({'a': 1.0, 'b': 1.0}, truth, predicted, CLASSES), CLASSES)
        neutral = result['per_class']['neutral']
        self.assertEqual((neutral['precision'], neutral['recall'], neutral['f1']), (None, None, None))
        negative = result['per_class']['negative']
        self.assertEqual((negative['precision'], negative['recall'], negative['f1']), (0.0, None, 0.0))
        self.assertEqual(result['classes_in_macro_f1'], ['positive', 'negative'])
        self.assertAlmostEqual(result['macro_f1'], (2 / 3 + 0.0) / 2)

    def test_no_weight_gives_no_scores(self):
        result = scoring.metrics(scoring.blank(CLASSES), CLASSES)
        self.assertEqual((result['accuracy'], result['macro_f1'], result['classes_in_macro_f1']), (None, None, []))


def story(story_id, members, anchor=None):
    return {'story_id': story_id, 'members': members, 'anchor': anchor or members[0]}


class TestEvaluate(unittest.TestCase):
    """One three-version story where only the first (the anchor) is wrong, plus two single stories."""

    def setUp(self):
        self.stories = [story('g', ['v1', 'v2', 'v3']), story('h', ['v4']), story('i', ['v5'])]
        self.truth = {'v1': 'positive', 'v2': 'positive', 'v3': 'positive', 'v4': 'negative', 'v5': 'neutral'}
        self.predicted = {'v1': 'negative', 'v2': 'positive', 'v3': 'positive', 'v4': 'negative', 'v5': 'neutral'}

    def run_it(self, **kwargs):
        return scoring.evaluate(self.stories, kwargs.get('truth', self.truth), kwargs.get('predicted', self.predicted),
                                CLASSES, seed=1, resamples=200)

    def test_conditions_differ_when_a_story_repeats(self):
        result = self.run_it()['paired_A_B_C']['conditions']
        self.assertAlmostEqual(result['A']['accuracy'], 4 / 5)
        self.assertAlmostEqual(result['B']['accuracy'], (2 / 3 + 1 + 1) / 3)
        self.assertAlmostEqual(result['C']['accuracy'], 2 / 3)
        self.assertEqual(result['C']['versions_with_weight'], 3)
        self.assertAlmostEqual(result['A']['total_weight'], 5.0)
        self.assertAlmostEqual(result['B']['total_weight'], 3.0)
        self.assertAlmostEqual(result['C']['total_weight'], 3.0)

    def test_all_single_version_stories_make_the_conditions_identical(self):
        self.stories = [story(f'g{i}', [f'v{i}']) for i in range(1, 6)]
        conditions = self.run_it()['paired_A_B_C']['conditions']
        self.assertEqual(conditions['A']['macro_f1'], conditions['B']['macro_f1'])
        self.assertEqual(conditions['A']['macro_f1'], conditions['C']['macro_f1'])
        self.assertEqual(self.run_it()['stories_with_several_scorable_members'], 0)

    def test_unresolved_versions_are_not_scored_as_neutral(self):
        del self.truth['v5']
        result = self.run_it()
        self.assertEqual(result['scorable_versions'], 4)
        matrix = result['unrestricted_A_B']['conditions']['A']['confusion_true_by_predicted']
        self.assertEqual(matrix['neutral'], {'positive': 0.0, 'negative': 0.0, 'neutral': 0.0})
        self.assertEqual(result['stories_with_a_scorable_member'], 2)

    def test_missing_anchor_label_drops_the_story_from_the_paired_result_only(self):
        del self.truth['v1']
        result = self.run_it()
        self.assertEqual(result['stories_with_scorable_anchor'], 2)
        self.assertEqual(result['paired_A_B_C']['stories'], 2)
        self.assertEqual(result['unrestricted_A_B']['stories'], 3)
        self.assertAlmostEqual(result['paired_A_B_C']['conditions']['A']['total_weight'], 2.0)
        self.assertAlmostEqual(result['unrestricted_A_B']['conditions']['A']['total_weight'], 4.0)

    def test_missing_prediction_is_not_scored(self):
        del self.predicted['v5']
        self.assertEqual(self.run_it()['scorable_versions'], 4)

    def test_bootstrap_is_seeded_and_paired_across_conditions(self):
        first, second = self.run_it(), self.run_it()
        self.assertEqual(first, second)
        block = first['paired_A_B_C']
        self.assertEqual(block['macro_f1_difference_bootstrap'].keys(), {'B-A', 'C-A'})
        self.assertGreater(block['conditions']['A']['bootstrap']['accuracy']['valid_resamples'], 0)
        stories = [story(f'g{i}', [f'v{i}']) for i in range(40)]
        truth = {f'v{i}': CLASSES[i % 3] for i in range(40)}
        predicted = {v: (c if int(v[1:]) % 4 else CLASSES[(CLASSES.index(c) + 1) % 3]) for v, c in truth.items()}
        runs = [scoring.evaluate(stories, truth, predicted, CLASSES, seed=s, resamples=300)['paired_A_B_C']
                ['conditions']['A']['bootstrap'] for s in (1, 1, 2)]
        self.assertEqual(runs[0], runs[1])
        self.assertNotEqual(runs[0], runs[2])

    def test_interval_brackets_the_point_estimate_on_a_larger_cohort(self):
        stories = [story(f'g{i}', [f'v{i}']) for i in range(40)]
        truth = {f'v{i}': CLASSES[i % 3] for i in range(40)}
        predicted = {v: (c if int(v[1:]) % 4 else CLASSES[(CLASSES.index(c) + 1) % 3]) for v, c in truth.items()}
        result = scoring.evaluate(stories, truth, predicted, CLASSES, seed=7, resamples=500)['paired_A_B_C']['conditions']['A']
        low, high = result['bootstrap']['macro_f1']['interval_95']
        self.assertLessEqual(low, result['macro_f1'])
        self.assertGreaterEqual(high, result['macro_f1'])
        self.assertLess(low, high)

    def test_empty_cohort_gives_empty_results_not_errors(self):
        result = scoring.evaluate([], {}, {}, CLASSES, seed=1, resamples=50)
        self.assertEqual(result['paired_A_B_C']['conditions']['A']['accuracy'], None)
        self.assertEqual(result['paired_A_B_C']['conditions']['A']['bootstrap']['macro_f1']['interval_95'], None)


if __name__ == '__main__':
    unittest.main()
