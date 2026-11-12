"""The README demo must keep showing the three conditions disagreeing, with hand-computed numbers."""
import unittest

from scripts import demo_weighting


class TestDemo(unittest.TestCase):
    def test_conditions_give_the_hand_computed_accuracies(self):
        result = demo_weighting.evaluate()
        self.assertAlmostEqual(result['A']['accuracy'], 4 / 6)
        self.assertAlmostEqual(result['B']['accuracy'], 1.75 / 3)
        self.assertAlmostEqual(result['C']['accuracy'], 1 / 3)

    def test_macro_f1_for_condition_a_by_hand(self):
        # positive 2*3/(4+4), negative 0, neutral 2*1/(1+2)
        self.assertAlmostEqual(demo_weighting.evaluate()['A']['macro_f1'], (0.75 + 0 + 2 / 3) / 3)

    def test_output_is_labeled_invented_and_is_repeatable(self):
        text = demo_weighting.render()
        self.assertEqual(text, demo_weighting.render())
        self.assertIn('INVENTED', text.splitlines()[0])
        self.assertIn('not a result', text.splitlines()[0])


if __name__ == '__main__':
    unittest.main()
