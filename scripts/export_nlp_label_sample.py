"""Export a deterministic, source-balanced headline sample for human review."""
import argparse
import csv
import hashlib
import json
import re
from pathlib import Path

import duckdb
from research.observations import LEGACY_HEADLINE_ELIGIBILITY_SQL


def quality_flag(title, source, snippet, metadata):
    words = re.findall(r"[A-Za-z]+", title or '')
    if len(words) < 4 or re.search(r'\b[0-9a-f]{12,}\b', title or '', re.I):
        return 'inspect_identifier_like_title'
    if metadata.get('title_provenance') == 'url_slug':
        return 'synthetic_url_slug'
    if source == 'GDELT' and (snippet or '').startswith(('Source:', 'Domain:')):
        return 'metadata_only_snippet'
    return ''


def export_sample(db_path, output, total=100, seed='week3-nlp-v1'):
    if total < 1:
        raise ValueError('Sample size must be positive')
    conn = duckdb.connect(str(Path(db_path).resolve()), read_only=True)
    try:
        rows = conn.execute('''SELECT id,source,title,published_time,ingested_time,content_snippet,metadata
            FROM silver_financial_news WHERE ''' + LEGACY_HEADLINE_ELIGIBILITY_SQL).fetchall()
    finally:
        conn.close()
    sources = sorted({row[1] for row in rows})
    selected = []
    quota = max(1, total // max(1, len(sources)))
    for source in sources:
        candidates = [row for row in rows if row[1] == source]
        candidates.sort(key=lambda row: hashlib.sha256(
            f'{seed}:{row[0]}'.encode('utf-8')).hexdigest())
        selected.extend(candidates[:quota])
    if len(selected) < total:
        remaining = [row for row in rows if row not in selected]
        remaining.sort(key=lambda row: hashlib.sha256(
            f'{seed}:remainder:{row[0]}'.encode('utf-8')).hexdigest())
        selected.extend(remaining[:total - len(selected)])
    selected = selected[:total]
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open('w', encoding='utf-8-sig', newline='') as handle:
        writer = csv.writer(handle)
        writer.writerow(['sample_id','event_id','source','title','published_time','ingested_time',
                         'title_provenance','published_time_provenance','quality_flag',
                         'sentiment_label','confidence_1_to_5','reviewer_notes'])
        for index, (event_id, source, title, published, ingested, snippet, raw_metadata) in enumerate(selected, 1):
            metadata = json.loads(raw_metadata or '{}')
            writer.writerow([index,event_id,source,title,published,ingested,
                             metadata.get('title_provenance','legacy_unknown'),
                             metadata.get('published_time_provenance','legacy_unknown'),
                             quality_flag(title, source, snippet, metadata),'','',''])
    return len(selected)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db-path', default='data/lakehouse.duckdb')
    parser.add_argument('--output', default='data/nlp-headline-evaluation.csv')
    parser.add_argument('--total', type=int, default=100)
    parser.add_argument('--seed', default='week3-nlp-v1')
    args = parser.parse_args()
    count = export_sample(args.db_path, args.output, args.total, args.seed)
    print(f'Exported {count} rows to {args.output}')


if __name__ == '__main__':
    main()
