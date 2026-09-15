import json
import logging
import re
import threading
from time import perf_counter
from dataclasses import dataclass
from typing import Optional, Tuple, Set, List
import redis
from datasketch import MinHash
from config import REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_TTL_SECONDS, REDIS_LSH_TTL_SECONDS, REDIS_SEMANTIC_TTL_SECONDS
from config import DEDUP_RESERVATION_SECONDS
from utils.polarity import base_form, polarity_profile, has_conflict

logger = logging.getLogger(__name__)

# Lazy-loaded semantic components (avoid model download at import time)
_semantic_dedup: Optional["SemanticDeduplicator"] = None
_SEMANTIC_INIT_LOCK = threading.Lock()


@dataclass(frozen=True)
class DedupResult:
    is_exact: bool = False
    is_near: bool = False
    canonical_id: Optional[str] = None
    embedding: Optional[List[float]] = None
    semantic_score: Optional[float] = None


class RedisDeduplicator:
    """
    Two-Tier Deduplication & Clustering Engine:
      1. Tier 1 (Exact Match): URL/ID hash via atomic SET NX EX (7-day TTL).
      2. Tier 2 (Near-Duplicate Clustering): MinHash LSH on headlines
         with candidate Jaccard verification (24-hour sliding window TTL).

    LSH Math Tuning (High-Recall Filter + Precision Gate):
      - num_perm (m) = 128, num_bands (b) = 32, rows_per_band (r) = 4.
      - Theoretical S-curve inflection point: (1/b)^(1/r) = (1/32)^(1/4) ≈ 0.420.
      - Theoretical candidate retrieval probability at s=0.75: 1 - (1 - 0.75^4)^32 ≈ 99.999%.
      - Step 1 (LSH) acts as a high-recall candidate net.
      - Step 2 verifies exact shingle-set Jaccard >= 0.75.
    """
    def __init__(
        self,
        host: str = REDIS_HOST,
        port: int = REDIS_PORT,
        db: int = REDIS_DB,
        exact_ttl_seconds: int = REDIS_TTL_SECONDS,      # 7 days (604,800s)
        lsh_ttl_seconds: int = REDIS_LSH_TTL_SECONDS,     # 24 hours (86,400s)
        num_perm: int = 128,                              # 128 perms reduces Jaccard estimation error to ~8.8%
        num_bands: int = 32,                              # b=32, r=4 -> 99.999% recall at s=0.75
        jaccard_threshold: float = 0.75
    ):
        self.exact_ttl = exact_ttl_seconds
        self.lsh_ttl = lsh_ttl_seconds
        self.num_perm = num_perm
        self.num_bands = num_bands
        self.jaccard_threshold = jaccard_threshold
        if num_perm <= 0 or num_bands <= 0 or num_perm % num_bands:
            raise ValueError("num_perm must be positive and divisible by num_bands")
        if not 0 < jaccard_threshold <= 1:
            raise ValueError("jaccard_threshold must be in (0, 1]")
        self.rows_per_band = num_perm // num_bands
        self._legacy_state = threading.local()

        self.client = redis.Redis(
            host=host,
            port=port,
            db=db,
            decode_responses=False,
            socket_timeout=5.0
        )

    def ping(self) -> bool:
        try:
            return bool(self.client.ping())
        except Exception as e:
            logger.error(f"Failed to ping Redis at {self.client.connection_pool.connection_kwargs.get('host')}: {e}")
            return False

    # ─────────────────────────────────────────────────────────────────────────
    #  TIER 1: EXACT MATCH DEDUPLICATION
    # ─────────────────────────────────────────────────────────────────────────

    def is_exact_duplicate(self, event_id: str, source: Optional[str] = None,
                           reserve_for_delivery: bool = False) -> bool:
        """
        Atomically checks if event_id exists via SET NX EX.
        Returns True if duplicate (already seen), False if newly set.
        """
        prefix = f"dedup:exact:{source.lower()}:" if source else "dedup:exact:"
        key = f"{prefix}{event_id}".encode("utf-8")
        try:
            ttl = DEDUP_RESERVATION_SECONDS if reserve_for_delivery else self.exact_ttl
            was_set = self.client.set(key, b"pending" if reserve_for_delivery else b"1", nx=True, ex=ttl)
            return not bool(was_set)
        except Exception as e:
            logger.error(f"Redis exact match error for key {key}: {e}. Fail-open.")
            return False

    # ─────────────────────────────────────────────────────────────────────────
    #  TIER 2: MINHASH LSH NEAR-DUPLICATE CLUSTERING
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _create_shingles(text: str) -> Set[str]:
        """
        Normalizes text into word unigrams + bigrams and adds weighted polarity anchors
        for both LSH retrieval and exact verification. Polarity anchors affect similarity;
        they are not a substitute for an explicit polarity guard.
        """
        clean_text = re.sub(r"[^\w\s]", " ", text.lower()).strip()
        tokens = clean_text.split()

        # Word unigrams + bigrams provide robust phrase matching for short headlines
        shingles: Set[str] = set(tokens)
        for i in range(len(tokens) - 1):
            shingles.add(f"{tokens[i]} {tokens[i+1]}")

        # Inject polarity keywords with amplified weight to protect market reversals.
        # Anchored by base form so inflections ("beats"/"missed"/"tumbled") share anchors.
        for token in tokens:
            base = base_form(token)
            if base:
                shingles.add(f"__POLAR_ANCHOR_1__{base}")
                shingles.add(f"__POLAR_ANCHOR_2__{base}")

        return shingles

    @staticmethod
    def _exact_jaccard(left: Set[str], right: Set[str]) -> float:
        """Empty headlines provide no evidence for a duplicate match."""
        union = left | right
        return len(left & right) / len(union) if union else 0.0

    def _minhash_shingles(self, shingles: Set[str]) -> MinHash:
        m = MinHash(num_perm=self.num_perm)
        for s in shingles:
            m.update(s.encode("utf-8"))
        return m

    def _create_minhash(self, text: str) -> MinHash:
        return self._minhash_shingles(self._create_shingles(text))

    def check_near_duplicate(self, event_id: str, title: str, source: str = "RSS", metrics: Optional[dict] = None) -> Tuple[bool, Optional[str]]:
        """Serialize the V2 lookup/insert decision across all feed workers.

        The 30-second lease is far above the measured sub-second candidate pass.
        A crashed holder eventually releases the key through expiry. Redis
        outages retain the existing fail-open ingestion behavior.
        """
        if source.upper() == "SEC" or not title or not self._create_shingles(title):
            return False, None
        lock = None
        acquired = False
        started = perf_counter() if metrics is not None else 0
        try:
            lock = self.client.lock(b'lsh:news:v2:assignment-lock', timeout=30,
                                    blocking_timeout=None, sleep=.01)
            acquired = bool(lock.acquire())
            if metrics is not None:
                metrics['lock_wait_ms'] = (perf_counter() - started) * 1000
            if not acquired:
                raise TimeoutError('Tier 2 assignment lock was not acquired')
            return self._check_near_duplicate_serial(event_id, title, source, metrics)
        except Exception as exc:
            logger.error("LSH assignment lock error for %s: %s. Fail-open.", event_id, exc)
            return False, None
        finally:
            if acquired:
                try:
                    lock.release()
                except Exception as exc:
                    # The lease bounds recovery even if an unlock response is lost.
                    logger.warning("Could not release LSH assignment lock for %s: %s", event_id, exc)

    def _check_near_duplicate_serial(self, event_id: str, title: str, source: str = "RSS", metrics: Optional[dict] = None) -> Tuple[bool, Optional[str]]:
        """
        Checks if the headline is a near-duplicate of any active headline
        within the configured LSH window (24 hours by default).

        Returns:
            (is_near_duplicate: bool, canonical_cluster_id: Optional[str])
        """
        # SEC regulatory filings must never use fuzzy deduplication
        if source.upper() == "SEC" or not title:
            return False, None

        try:
            if metrics is not None:
                metrics.update(dict.fromkeys(('retrieval_ms', 'record_fetch_ms', 'decode_ms', 'jaccard_ms', 'polarity_ms'), 0.0))
            shingles = self._create_shingles(title)
            if not shingles:
                return False, None
            m = self._minhash_shingles(shingles)

            # Query all LSH bands via Redis pipeline
            band_keys = []
            pipeline = self.client.pipeline()
            for b in range(self.num_bands):
                start = b * self.rows_per_band
                end = start + self.rows_per_band
                band_hash = m.hashvalues[start:end].tobytes()
                bkey = f"lsh:news:v2:b:{b}:".encode("utf-8") + band_hash
                band_keys.append(bkey)
                pipeline.smembers(bkey)

            started = perf_counter() if metrics is not None else 0
            candidate_sets = pipeline.execute()
            event_id_bytes = event_id.encode("utf-8")
            candidates = {cand for c_set in candidate_sets for cand in c_set if cand != event_id_bytes}
            if metrics is not None:
                metrics['candidate_count'] = len(candidates)
                metrics['retrieval_ms'] += (perf_counter() - started) * 1000

            # Shared news namespace allows RSS/GDELT syndication matching.
            # V2 stores ordered text for polarity and the original cluster ID.
            payload_prefix = b"lsh:news:v2:event:"
            new_profile = polarity_profile(title)
            best_match_id = None
            highest_jaccard = 0.0
            # Verify every retrieved candidate, in bounded Redis request batches.
            # Sorting also gives deterministic tie-breaking for equal scores.
            if candidates:
                cand_list = sorted(candidates)
                for start in range(0, len(cand_list), 256):
                    batch = cand_list[start:start + 256]
                    read_pipe = self.client.pipeline()
                    for cand_id in batch:
                        read_pipe.get(payload_prefix + cand_id)
                    started = perf_counter() if metrics is not None else 0
                    saved_records = read_pipe.execute()
                    if metrics is not None:
                        metrics['record_fetch_ms'] += (perf_counter() - started) * 1000
                    for cand_id, saved in zip(batch, saved_records):
                        if saved is None:
                            continue  # Expired entry or pre-upgrade signature only.
                        try:
                            started = perf_counter() if metrics is not None else 0
                            decoded = json.loads(saved)
                            if (not isinstance(decoded, dict)
                                    or not isinstance(decoded.get('title'), str)
                                    or not isinstance(decoded.get('canonical_id'), str)
                                    or not decoded['canonical_id']
                                    or not isinstance(decoded.get('shingles'), list)
                                    or not all(isinstance(s, str) for s in decoded['shingles'])):
                                raise ValueError("Expected a complete V2 news record")
                            candidate_shingles = set(decoded['shingles'])
                            if metrics is not None:
                                metrics['decode_ms'] += (perf_counter() - started) * 1000
                        except (ValueError, TypeError, UnicodeError):
                            logger.warning("Skipping invalid LSH shingle payload for %r", cand_id)
                            continue
                        started = perf_counter() if metrics is not None else 0
                        score = self._exact_jaccard(shingles, candidate_shingles)
                        if metrics is not None:
                            metrics['jaccard_ms'] += (perf_counter() - started) * 1000
                        started = perf_counter() if metrics is not None else 0
                        conflict = has_conflict(new_profile, polarity_profile(decoded['title']))
                        if metrics is not None:
                            metrics['polarity_ms'] += (perf_counter() - started) * 1000
                        if conflict:
                            continue
                        if score >= self.jaccard_threshold and score > highest_jaccard:
                            highest_jaccard = score
                            best_match_id = decoded['canonical_id']

            # Store the same shingles used for retrieval; signatures alone are
            # insufficient for exact verification. Payload expiry bounds validity
            # even when shared bucket TTLs are refreshed by newer events.
            write_pipe = self.client.pipeline()
            event_id_bytes = event_id.encode("utf-8")
            record = {'title': title, 'shingles': sorted(shingles),
                      'canonical_id': best_match_id or event_id}
            write_pipe.set(payload_prefix + event_id_bytes, json.dumps(record), ex=self.lsh_ttl)
            for bkey in band_keys:
                write_pipe.sadd(bkey, event_id_bytes)
                write_pipe.expire(bkey, self.lsh_ttl)
            write_pipe.execute()

            if best_match_id is not None:
                logger.info("Near-duplicate %s matches %s (exact Jaccard=%.4f)",
                            event_id, best_match_id, highest_jaccard)
                return True, best_match_id
            return False, None

        except Exception as e:
            logger.error(f"LSH near-duplicate check error for {event_id}: {e}. Fail-open.")
            return False, None

    # ─────────────────────────────────────────────────────────────────────────
    #  UNIFIED ENTRYPOINTS
    # ─────────────────────────────────────────────────────────────────────────

    def check_dedup(
        self,
        event_id: str,
        title: str,
        source: str = "RSS",
        tickers: Optional[List[str]] = None,
    ) -> Tuple[bool, bool, Optional[str]]:
        """
        Executes Three-Tier Deduplication:
        1. Exact Match on ID/URL -> Returns (True, False, None) if duplicate.
        2. MinHash LSH on Title  -> Returns (False, True, canonical_id) if clustered.
        3. Semantic Embedding    -> Returns (False, True, canonical_id) if cosine >=
           SEMANTIC_COSINE_THRESHOLD (calibrated by scripts/calibrate_tier3.py),
           gated by ticker overlap and polarity-class conflict.

        Side effects:
            Legacy embedding/score accessors are thread-local. New workers
            use check_dedup_result() and serialize its public fields directly.
        """
        result = self.check_dedup_result(event_id, title, source, tickers)
        self._legacy_state.result = result
        return result.is_exact, result.is_near, result.canonical_id

    @property
    def _last_embedding(self):
        return getattr(self._legacy_state, 'result', DedupResult()).embedding

    @property
    def _last_semantic_score(self):
        return getattr(self._legacy_state, 'result', DedupResult()).semantic_score

    def check_dedup_result(self, event_id: str, title: str, source: str = 'RSS',
                           tickers: Optional[List[str]] = None,
                           reserve_for_delivery: bool = False) -> DedupResult:
        """Return event-specific data directly; no shared worker handoff state."""

        if self.is_exact_duplicate(event_id, source=source, reserve_for_delivery=reserve_for_delivery):
            return DedupResult(is_exact=True)

        is_near_dup, canonical_id = self.check_near_duplicate(event_id, title, source=source)
        if is_near_dup:
            return DedupResult(is_near=True, canonical_id=canonical_id)

        # Tier 3: Semantic Embedding Dedup (skip for SEC filings)
        if source.upper() != "SEC" and title:
            try:
                embedding, is_sem_dup, sem_canonical, sem_score = self._check_semantic(event_id, title, tickers)
                vector = embedding.tolist() if hasattr(embedding, 'tolist') else list(embedding)
                if is_sem_dup:
                    # Semantic matching may target a lexical duplicate. Resolve
                    # its original cluster before storing this new LSH record.
                    saved = self.client.get(b'lsh:news:v2:event:' + sem_canonical.encode('utf-8'))
                    if saved:
                        sem_canonical = json.loads(saved)['canonical_id']
                    record = {'title': title, 'shingles': sorted(self._create_shingles(title)),
                              'canonical_id': sem_canonical}
                    self.client.set(b'lsh:news:v2:event:' + event_id.encode('utf-8'),
                                    json.dumps(record), ex=self.lsh_ttl)
                    return DedupResult(is_near=True, canonical_id=sem_canonical,
                                       embedding=vector, semantic_score=sem_score)
                return DedupResult(embedding=vector)
            except Exception as e:
                logger.error(f"Tier 3 semantic dedup error for {event_id}: {e}. Fail-open.")

        return DedupResult()

    def _check_semantic(self, event_id: str, title: str, tickers: Optional[List[str]] = None):
        """
        Lazily initializes the embedding model and semantic deduplicator
        (both under locks — main.py runs RSS/GDELT workers in parallel threads),
        then checks for semantic duplicates.

        Returns:
            (embedding, is_sem_dup, canonical_id, cosine_score)
        """
        global _semantic_dedup

        from models.embedder import get_embedder
        embedder = get_embedder()

        if _semantic_dedup is None:
            with _SEMANTIC_INIT_LOCK:
                if _semantic_dedup is None:
                    from utils.semantic_dedup import SemanticDeduplicator
                    _semantic_dedup = SemanticDeduplicator(
                        host=self.client.connection_pool.connection_kwargs.get("host", REDIS_HOST),
                        port=self.client.connection_pool.connection_kwargs.get("port", REDIS_PORT),
                        db=self.client.connection_pool.connection_kwargs.get("db", REDIS_DB),
                    )

        embedding = embedder.embed_text(title)
        is_sem_dup, canonical_id, score = _semantic_dedup.check_semantic_duplicate(
            event_id, embedding, title=title, tickers=tickers
        )
        return embedding, is_sem_dup, canonical_id, score

    def is_duplicate_or_set(self, event_id: str, source: Optional[str] = None) -> bool:
        """Backward-compatibility helper for exact-match deduplication."""
        return self.is_exact_duplicate(event_id, source=source)

    def get_ttl(self, event_id: str, source: Optional[str] = None) -> int:
        """Returns remaining TTL for exact deduplication key in seconds."""
        prefix = f"dedup:exact:{source.lower()}:" if source else "dedup:exact:"
        key = f"{prefix}{event_id}".encode("utf-8")
        try:
            return self.client.ttl(key)
        except Exception:
            return -1

    def release_unstaged(self, event_id: str, source: str):
        """Allow re-fetch after local outbox persistence fails, before delivery."""
        key = f'dedup:exact:{source.lower()}:{event_id}'.encode('utf-8')
        self.client.delete(key)
        if source.upper() != 'SEC':
            self.client.delete(b'lsh:news:v2:event:' + event_id.encode('utf-8'))
            self.client.delete(b'emb:vec:' + event_id.encode('utf-8'),
                               b'emb:pol:' + event_id.encode('utf-8'),
                               b'emb:tick:' + event_id.encode('utf-8'))
            self.client.zrem(b'emb:window:zset', event_id.encode('utf-8'))

    def confirm_staged(self, event_id: str, source: str):
        """Promote a short reservation once its outgoing payload is durable."""
        try:
            self.client.set(f'dedup:exact:{source.lower()}:{event_id}'.encode('utf-8'),
                            b'1', ex=self.exact_ttl)
        except redis.RedisError:
            # Delivery recovery remains independent of Redis availability.
            logger.exception('Could not finalize dedup marker for %s', event_id)
