import unittest

from scripts.capture_syndication_fixture import rank_pairs


class TestSyndicationCapture(unittest.TestCase):
    def test_ranking_uses_exact_score_and_polarity_guard(self):
        rss = [{'title':'Federal Reserve raises interest rates by 25 basis points to curb inflation'}]
        gdelt = [
            {'title':'Unrelated market report'},
            {'title':'Federal Reserve raises interest rates by 25 basis points to battle inflation'},
            {'title':'Federal Reserve cuts interest rates by 25 basis points to curb inflation'},
        ]
        ranked = rank_pairs(rss, gdelt)
        self.assertEqual(ranked[0]['gdelt']['title'], gdelt[1]['title'])
        self.assertGreaterEqual(ranked[0]['exact_jaccard'], .75)
        self.assertNotIn(gdelt[2]['title'], [row['gdelt']['title'] for row in ranked])


if __name__ == '__main__':
    unittest.main()
