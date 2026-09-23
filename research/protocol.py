"""Deterministic, causal split planning for the four adaptation baselines.

This module plans experiments; it does not train FinBERT or report model scores.
Live Redis flags are deliberately never used to construct research clusters.
"""
import hashlib
from datetime import datetime, timedelta, timezone
from pydantic import AwareDatetime, BaseModel, ConfigDict
from typing import Literal
from utils.redis_cache import RedisDeduplicator
from utils.polarity import polarity_profile, has_conflict


PROTOCOL_VERSION = 'financial-sentiment-v1'


def utc(value):
    dt = datetime.fromisoformat(value.replace('Z', '+00:00')) if isinstance(value, str) else value
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError('All research timestamps must have an explicit timezone')
    return dt.astimezone(timezone.utc)


class SentimentLabel(BaseModel):
    model_config = ConfigDict(extra='forbid')
    version_id: str
    content_hash: str
    sentiment: Literal['negative', 'neutral', 'positive']
    label_available_at: AwareDatetime
    annotator: str
    annotation_policy: str


def assign_clusters(rows, method='lexical', threshold=.75, window_hours=24):
    """Forward-only reference grouping; adding a future suffix preserves prefixes.

    Article revisions stay in one family in every arm. Lexical matching is
    exhaustive inside the window, with the production shingles/polarity guard.
    This is an offline reference algorithm, not a claim of LSH throughput.
    """
    if method not in ('article', 'exact', 'lexical'):
        raise ValueError('Unknown clustering method')
    if not 0 < threshold <= 1 or window_hours <= 0:
        raise ValueError('Invalid clustering threshold/window')
    ordered = sorted(rows, key=lambda r: (utc(r['available_at']), utc(r['observed_at']), r['version_id']))
    if len({r['version_id'] for r in ordered}) != len(ordered):
        raise ValueError('Dataset must contain unique version IDs')
    families, active, result = {}, [], []
    for row in ordered:
        now = utc(row['available_at'])
        if now < utc(row['observed_at']):
            raise ValueError('Availability precedes observation')
        # Group by URL identity across sources; content versions remain distinct.
        family = row['article_id']
        shingles = RedisDeduplicator._create_shingles(row['title'])
        normalized = ' '.join(row['title'].lower().split())
        profile = polarity_profile(row['title'])
        active = [candidate for candidate in active if candidate[0] >= now - timedelta(hours=window_hours)]
        root = families.get(family)
        best = 0.0
        if root is None and method != 'article':
            for _, previous, previous_shingles, previous_text, previous_profile in active:
                if has_conflict(profile, previous_profile):
                    continue
                score = (1.0 if normalized == previous_text else 0.0) if method == 'exact' else RedisDeduplicator._exact_jaccard(shingles, previous_shingles)
                if score >= threshold and score > best:
                    root, best = previous['cluster_id'], score
        root = root or row['version_id']
        families[family] = root
        annotated = {**row, 'cluster_id': root, 'cluster_method': method,
                     'cluster_decided_at': now.isoformat()}
        active.append((now, annotated, shingles, normalized, profile))
        result.append(annotated)
    return result


def build_windows(rows, labels, cutoffs, policy='replay', replay_budget=256, seed=17):
    """Plan fit-at-start/predict-in-window evaluation with label-delay controls.

    All arms share the same eligible population. Primary evaluation excludes
    clusters ever used for fitting, including ones dropped from replay memory.
    Scoring labels are not returned in evaluation records.
    """
    if policy not in ('frozen', 'retrain', 'sequential', 'replay'):
        raise ValueError('Unknown adaptation policy')
    if replay_budget < 0:
        raise ValueError('Replay budget must be nonnegative')
    boundaries = [utc(c) for c in cutoffs]
    if len(boundaries) < 2 or any(b <= a for a, b in zip(boundaries, boundaries[1:])):
        raise ValueError('At least two strictly increasing cutoffs are required')
    by_id = {r['version_id']: r for r in rows}
    if len(by_id) != len(rows):
        raise ValueError('Duplicate dataset version ID')
    for row in rows:
        if 'cluster_id' not in row or utc(row['available_at']) < utc(row['observed_at']):
            raise ValueError('Missing cluster or invalid availability')
    validated = [label if isinstance(label, SentimentLabel) else SentimentLabel.model_validate(label) for label in labels]
    seen_labels = set()
    for label in validated:
        if label.version_id not in by_id or label.content_hash != by_id[label.version_id]['content_hash']:
            raise ValueError('Label does not match an exact dataset content version')
        if not label.annotator.strip() or not label.annotation_policy.strip():
            raise ValueError('Labels require annotator and annotation policy')
        if utc(label.label_available_at) < utc(by_id[label.version_id]['available_at']):
            raise ValueError('Label availability precedes research data availability')
        identity = (label.version_id, utc(label.label_available_at))
        if identity in seen_labels:
            raise ValueError('Ambiguous label revision at the same timestamp')
        seen_labels.add(identity)
    used, trained_clusters, plans = {}, set(), []
    # One common first-seen-event cohort for all model arms, independent of
    # whether an arm actually trained on that cluster (e.g. frozen vs replay).
    evaluation_seen = {r['cluster_id'] for r in rows if utc(r['available_at']) < boundaries[0]}
    for index, (start, end) in enumerate(zip(boundaries, boundaries[1:])):
        eligible = {}
        for label in sorted(validated, key=lambda l: (utc(l.label_available_at), l.version_id)):
            if utc(label.label_available_at) < start and utc(by_id[label.version_id]['available_at']) < start:
                eligible[label.version_id] = label
        changed = {vid: label for vid, label in eligible.items()
                   if used.get(vid) != label.model_dump(mode='json')}
        fit = eligible if index == 0 or policy == 'retrain' else changed
        if policy == 'frozen' and index > 0:
            fit = {}
        elif policy == 'replay' and index > 0:
            # Fixed-seed priority sampling: reproducible uniform bounded memory.
            older = sorted(set(eligible) - set(changed), key=lambda vid: hashlib.sha256(f'{seed}:{vid}'.encode()).hexdigest())
            fit = {**changed, **{vid: eligible[vid] for vid in older[:replay_budget]}}
        for vid, label in fit.items():
            used[vid] = label.model_dump(mode='json')
            trained_clusters.add(by_id[vid]['cluster_id'])
        window = [r for r in rows if start <= utc(r['available_at']) < end]
        window.sort(key=lambda r: (utc(r['available_at']), r['version_id']))
        independent, repeated = [], []
        seen_groups = evaluation_seen
        for row in window:
            target = repeated if row['cluster_id'] in seen_groups else independent
            target.append(row['version_id'])
            seen_groups.add(row['cluster_id'])
        plans.append({'fit_before': start.isoformat(), 'predict_until': end.isoformat(),
                      'reset_model': policy == 'retrain' or index == 0,
                      'training_labels': [fit[vid].model_dump(mode='json') for vid in sorted(fit)],
                      'primary_evaluation_ids': independent, 'repeated_cluster_ids': repeated,
                      'initial_labels_available': bool(eligible)})
    return {'protocol': PROTOCOL_VERSION, 'policy': policy, 'seed': seed,
            'replay_budget': replay_budget, 'windows': plans,
            'status': 'split_plan_only_not_model_results'}
