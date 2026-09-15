"""Capture a real RSS/GDELT pair using the production Tier 2 acceptance rules."""
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import feedparser
import requests

from utils.polarity import has_conflict, polarity_profile
from utils.redis_cache import RedisDeduplicator
from workers.gdelt_worker import GDELT_DOC_API, MACRO_QUERY
from workers.rss_worker import MACRO_FEEDS


def rank_pairs(rss_items, gdelt_items):
    ranked = []
    for rss in rss_items:
        left = RedisDeduplicator._create_shingles(rss['title'])
        for gdelt in gdelt_items:
            right = RedisDeduplicator._create_shingles(gdelt['title'])
            score = RedisDeduplicator._exact_jaccard(left, right)
            if has_conflict(polarity_profile(rss['title']), polarity_profile(gdelt['title'])):
                continue
            ranked.append({'exact_jaccard': score, 'rss': rss, 'gdelt': gdelt})
    return sorted(ranked, key=lambda row: (-row['exact_jaccard'],
                                           row['rss']['title'], row['gdelt']['title']))


def capture(timeout=20, threshold=.75):
    headers = {'User-Agent': 'FinancialIngestionPipeline/1.0 research@example.com'}
    rss_items = []
    source_errors = []
    for feed in MACRO_FEEDS:
        try:
            response = requests.get(feed['url'], headers=headers, timeout=timeout)
            response.raise_for_status()
            parsed = feedparser.parse(response.content)
            for entry in parsed.entries:
                title = getattr(entry, 'title', '').strip()
                if title:
                    rss_items.append({
                        'feed_name': feed['name'], 'feed_url': feed['url'],
                        'title': title, 'article_url': getattr(entry, 'link', ''),
                        'published_raw': getattr(entry, 'published', None),
                    })
        except Exception as exc:
            source_errors.append({'source': feed['name'], 'error': str(exc)})

    gdelt_items = []
    try:
        response = requests.get(GDELT_DOC_API, params={
            'query': MACRO_QUERY, 'mode': 'ArtList', 'format': 'json',
            'maxrecords': '250', 'sort': 'datedesc'}, headers=headers, timeout=timeout)
        response.raise_for_status()
        for article in response.json().get('articles', []):
            title = str(article.get('title') or '').strip()
            if title:
                gdelt_items.append({
                    'title': title, 'article_url': article.get('url'),
                    'seen_date_raw': article.get('seendate'),
                    'domain': article.get('domain'), 'language': article.get('language'),
                })
    except Exception as exc:
        source_errors.append({'source': 'GDELT DOC API', 'error': str(exc)})

    ranked = rank_pairs(rss_items, gdelt_items)
    qualifying = [pair for pair in ranked if pair['exact_jaccard'] >= threshold]
    return {
        'captured_at': datetime.now(timezone.utc).isoformat(),
        'method': 'Live HTTP responses from configured RSS feeds and GDELT DOC API; '
                  'production unigram/bigram shingles, polarity guard and exact Jaccard.',
        'threshold': threshold, 'rss_items': len(rss_items),
        'gdelt_items': len(gdelt_items), 'source_errors': source_errors,
        'status': 'qualifying_pair_captured' if qualifying else 'no_qualifying_pair',
        'qualifying_pair': qualifying[0] if qualifying else None,
        'top_candidates': ranked[:10],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path,
                        default=Path('docs/real-syndication-validation.json'))
    parser.add_argument('--timeout', type=float, default=20)
    parser.add_argument('--threshold', type=float, default=.75)
    args = parser.parse_args()
    report = capture(timeout=args.timeout, threshold=args.threshold)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
