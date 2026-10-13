"""Review frozen headline data and collector attempts without opening live DuckDB."""
import argparse
import hashlib
import json
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

from config import OUTBOX_PATH
from research.observations import canonical_json
from research.protocol import utc
from research.provenance import code_identity
from research.sessions import read_session_evidence, verify_request_links


def build_review(rows, evidence):
    """Summarize attempts within each run; never interpret repeated counts as news."""
    links = verify_request_links(rows, evidence)
    sessions = {s['run_id']: s for s in evidence['sessions']}
    requests = {r['request_id']: r for r in evidence['requests']}
    groups = defaultdict(list)
    for request in evidence['requests']:
        if request['run_id'] not in sessions:
            raise ValueError('Request has no session evidence')
        groups[(request['run_id'], request['endpoint'])].append(request)
    source_results = []
    for (run_id, endpoint), attempts in sorted(groups.items()):
        attempts = sorted(attempts, key=lambda r: (utc(r['started_at']), r['request_id']))
        starts = [utc(r['started_at']) for r in attempts]
        intervals = [(b-a).total_seconds() for a, b in zip(starts, starts[1:])]
        source_results.append({
            'run_id': run_id, 'endpoint': endpoint,
            'session_status': sessions[run_id]['status'],
            'invalidated_run': sessions[run_id]['invalidated'],
            'attempts': len(attempts), 'outcomes': dict(Counter(r['outcome'] for r in attempts)),
            'http_statuses': dict(Counter(str(r['http_status']) if r['http_status'] is not None
                                         else 'unknown' for r in attempts)),
            'reported_items_across_attempts': sum(r['item_count'] or 0 for r in attempts),
            'attempts_without_item_count': sum(r['item_count'] is None for r in attempts),
            'first_attempt_started_at': attempts[0]['started_at'],
            'last_attempt_started_at': attempts[-1]['started_at'],
            'max_interval_between_attempt_starts_seconds': max(intervals) if intervals else None,
            'errors': sorted({r['error'] for r in attempts if r['error']}),
        })
    lags = defaultdict(list)
    for row in rows:
        snapshot = json.loads(row['snapshot'])
        context = snapshot.get('request_context')
        if not context or not row.get('source_time'):
            continue
        # Updated/feed-created/GDELT discovery times must not be called publication.
        kind = snapshot['source_time_kind']
        endpoint = requests[context['request_id']]['endpoint']
        basis = 'publication' if kind.startswith('feed:published') else kind
        lags[(endpoint, basis)].append((utc(row['observed_at'])-utc(row['source_time'])).total_seconds()/3600)
    distributions = [
        {'endpoint': endpoint, 'source_time_basis': basis, 'selected_versions': len(values),
         'min_hours': min(values), 'median_hours': statistics.median(values),
         'max_hours': max(values), 'negative_lag_versions': sum(v < 0 for v in values)}
        for (endpoint, basis), values in sorted(lags.items())]
    return {'kind': 'source_quality_review_v2',
            'database_opened': False, 'selected_versions': len(rows),
            'selected_article_ids': len({r['article_id'] for r in rows}),
            'request_links': links, 'source_requests_by_run': source_results,
            'source_to_observation_lags': distributions,
            'limitations': [
                'Returned/selected items across repeated attempts are not unique headlines or independent events.',
                'A successful HTTP request does not establish fresh news or successful downstream delivery.',
                'Invalidated attempts remain debugging evidence and are identified separately.',
                'Intervals describe attempt starts within one run, not verified continuous coverage or outage duration.',
                'Lag distributions describe only the selected snapshot, not all feed items.',
                'Publication, updated and GDELT discovery times are different source-time bases.',
                'This review does not certify a drain, final eligibility, annotation quality or paper readiness.']}


def review_sources(snapshot_dir, ledger_path, output_dir):
    snapshot = Path(snapshot_dir)
    manifest_bytes = (snapshot/'manifest.json').read_bytes()
    manifest = json.loads(manifest_bytes)
    data = (snapshot/'dataset.jsonl').read_bytes()
    digest = hashlib.sha256(data).hexdigest()
    if digest != manifest.get('dataset_sha256'):
        raise ValueError('Frozen dataset hash mismatch')
    rows = [json.loads(line) for line in data.splitlines()]
    if len(rows) != manifest.get('rows') or len({r['version_id'] for r in rows}) != len(rows):
        raise ValueError('Frozen dataset count or unique-version identity mismatch')
    if (snapshot/'INVALID_FOR_RESEARCH.json').exists():
        raise ValueError('Use a valid corrected snapshot for selected headline quality')
    evidence = read_session_evidence(ledger_path)
    report = build_review(rows, evidence)
    serialized = canonical_json(evidence) + '\n'
    report.update({'reviewed_at_utc': datetime.now(timezone.utc).isoformat(),
                   'snapshot_directory': str(snapshot), 'dataset_sha256': digest,
                   'snapshot_manifest_sha256': hashlib.sha256(manifest_bytes).hexdigest(),
                   'ledger_sha256': hashlib.sha256(serialized.encode()).hexdigest(),
                   'reviewer_code': code_identity()})
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=False)
    (output/'collection-sessions.json').write_text(serialized, encoding='utf-8', newline='\n')
    (output/'report.json').write_text(json.dumps(report, indent=2)+'\n', encoding='utf-8', newline='\n')
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot-dir', required=True)
    parser.add_argument('--ledger', default=OUTBOX_PATH)
    parser.add_argument('--output-dir', required=True)
    args = parser.parse_args()
    report = review_sources(args.snapshot_dir, args.ledger, args.output_dir)
    print(json.dumps({key: report[key] for key in ('reviewed_at_utc', 'selected_versions',
          'request_links', 'source_requests_by_run', 'source_to_observation_lags')}, indent=2))


if __name__ == '__main__':
    main()
