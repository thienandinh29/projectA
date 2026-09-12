"""
Tier 3: Semantic Deduplication via all-MiniLM-L6-v2 Embeddings

Uses a Redis 120-minute sliding window of L2-normalized 384-dim vectors.
Cosine similarity is computed as a simple dot product (since vectors are unit-length).

Storage Layout in Redis:
  - emb:vec:<event_id>  →  768 raw bytes (384 × float32)          TTL = 7200s
  - emb:pol:<event_id>  →  serialized polarity profile "+beat,-miss"  TTL = 7200s
  - emb:tick:<event_id> →  comma-joined uppercase ticker set      TTL = 7200s
  - emb:window:zset     →  sorted set scored by UTC epoch  TTL = managed via ZREMRANGEBYSCORE

Workflow:
  1. Prune expired entries from the sliding window (score < now - 7200).
  2. Fetch all active vectors, polarity profiles, and ticker sets.
  3. Compute cosine similarities via NumPy matrix dot product.
  4. A candidate above threshold must pass two gates before merging:
       - Ticker gate:    both ticker sets non-empty and disjoint → reject
                         (same company, different event is the dominant
                         real-feed failure mode; empty sets are pass-through
                         because most macro headlines extract no tickers).
       - Polarity guard: one profile positive-only vs other negative-only → reject
                         (MiniLM cannot separate beats/misses — both score ~0.9).
  5. If the best surviving candidate has similarity >= SEMANTIC_COSINE_THRESHOLD
     → flag as semantic duplicate with canonical cluster ID.
  6. Store the new vector, polarity profile, tickers, and register in the window.

Scan cap (SEMANTIC_SCAN_LIMIT, default 1000): the window fetch is newest-first
and bounded, so worst-case latency is O(cap) not O(window) — the full scan
measured 366ms p50 at a 5,000-entry window; capped it is ~78ms (benchmark.py
--tier3). Documented trade-off, parity-tested: a duplicate whose canonical
sits older than the newest N entries within the 120-min window is not found
by Tier 3 (Tier 2's 24h LSH window still covers those).

The threshold is calibrated empirically by scripts/calibrate_tier3.py
(band distribution table + precision/recall sweep), never hand-picked.
"""

import time
import logging
from typing import Optional, Tuple, List, Set

import numpy as np
import redis

from config import (
    REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_SEMANTIC_TTL_SECONDS,
    SEMANTIC_COSINE_THRESHOLD, SEMANTIC_SCAN_LIMIT,
)
from utils.polarity import (
    polarity_profile, has_conflict, serialize_profile, deserialize_profile,
)

logger = logging.getLogger(__name__)

EMBEDDING_DIM = 384
WINDOW_ZSET_KEY = b"emb:window:zset"


class SemanticDeduplicator:
    """
    Redis-backed 120-minute sliding window for semantic near-duplicate detection.

    Uses L2-normalized embeddings from all-MiniLM-L6-v2 and computes cosine
    similarity as dot product via NumPy vectorized matrix multiplication.
    Merge decisions are grounded by a ticker/entity gate and a polarity-class
    guard (see utils/polarity.py), not by cosine similarity alone.
    """

    def __init__(
        self,
        host: str = REDIS_HOST,
        port: int = REDIS_PORT,
        db: int = REDIS_DB,
        semantic_ttl_seconds: int = REDIS_SEMANTIC_TTL_SECONDS,
        cosine_threshold: Optional[float] = None,
        scan_limit: Optional[int] = None,
    ):
        self.semantic_ttl = semantic_ttl_seconds
        self.cosine_threshold = (
            cosine_threshold if cosine_threshold is not None else SEMANTIC_COSINE_THRESHOLD
        )
        # 0/None = unlimited brute-force scan. Otherwise only the newest N
        # window entries are fetched per event (newest-first) — bounded
        # latency; matches to older canonicals inside the 120-min window are
        # a documented, parity-tested limitation.
        self.scan_limit = scan_limit if scan_limit is not None else SEMANTIC_SCAN_LIMIT
        self.client = redis.Redis(
            host=host,
            port=port,
            db=db,
            decode_responses=False,
            socket_timeout=5.0,
        )

    def _vec_key(self, event_id: str) -> bytes:
        return f"emb:vec:{event_id}".encode("utf-8")

    def _pol_key(self, event_id: str) -> bytes:
        return f"emb:pol:{event_id}".encode("utf-8")

    def _tick_key(self, event_id: str) -> bytes:
        return f"emb:tick:{event_id}".encode("utf-8")

    def _prune_window(self, now: float):
        """Remove entries older than the sliding window from the sorted set."""
        cutoff = now - self.semantic_ttl
        try:
            self.client.zremrangebyscore(WINDOW_ZSET_KEY, "-inf", cutoff)
        except Exception as e:
            logger.warning(f"Semantic window prune error: {e}")

    def check_semantic_duplicate(
        self,
        event_id: str,
        embedding: np.ndarray,
        title: str = "",
        tickers: Optional[List[str]] = None,
    ) -> Tuple[bool, Optional[str], float]:
        """
        Checks whether the given embedding is a semantic duplicate of any
        vector currently in the 120-minute sliding window.

        Gates applied to every above-threshold candidate, in order:
          1. Ticker gate — both ticker sets non-empty and disjoint → reject.
          2. Polarity guard — one side positive-only, other negative-only → reject.

        Args:
            event_id:  Unique identifier for the incoming event.
            embedding: L2-normalized np.ndarray of shape (384,), dtype float32.
            title:     Headline text for polarity extraction.
            tickers:   Tickers extracted from the incoming event (empty = no gate).

        Returns:
            (is_semantic_dup, canonical_id, best_cosine_score)
            - is_semantic_dup: True if a candidate passed all gates at cosine >= threshold.
            - canonical_id:    The event_id of the best-matching canonical story.
            - best_cosine_score: Highest cosine similarity found (0.0 if none).
        """
        try:
            now = time.time()
            new_polarity = polarity_profile(title)
            new_tickers = {t.upper() for t in (tickers or []) if t}

            # 1. Prune expired entries
            self._prune_window(now)

            # 2. Get active event IDs from the window (newest-first, capped)
            cutoff = now - self.semantic_ttl
            if self.scan_limit:
                active_members: List[bytes] = self.client.zrevrangebyscore(
                    WINDOW_ZSET_KEY, "+inf", cutoff, start=0, num=self.scan_limit
                )
            else:
                active_members = self.client.zrangebyscore(
                    WINDOW_ZSET_KEY, cutoff, "+inf"
                )

            # Filter out self
            event_id_bytes = event_id.encode("utf-8")
            active_members = [m for m in active_members if m != event_id_bytes]

            best_score = 0.0
            best_match_id = None

            if active_members:
                # 3. Pipeline-fetch all active vectors, polarity profiles, and ticker sets
                pipe = self.client.pipeline()
                for m in active_members:
                    pipe.get(b"emb:vec:" + m)
                    pipe.get(b"emb:pol:" + m)
                    pipe.get(b"emb:tick:" + m)
                raw_results = pipe.execute()

                # 4. Build matrix of active vectors with gate info
                valid_ids: List[str] = []
                valid_vecs: List[np.ndarray] = []
                valid_polarities: List[Tuple[Set[str], Set[str]]] = []
                valid_tickers: List[Set[str]] = []
                expected_bytes = EMBEDDING_DIM * 4  # float32 = 4 bytes

                for i, member in enumerate(active_members):
                    raw_vec = raw_results[i * 3]
                    raw_pol = raw_results[i * 3 + 1]
                    raw_tick = raw_results[i * 3 + 2]

                    if raw_vec and len(raw_vec) == expected_bytes:
                        vec = np.frombuffer(raw_vec, dtype=np.float32).copy()
                        valid_vecs.append(vec)
                        valid_ids.append(member.decode("utf-8"))
                        valid_polarities.append(
                            deserialize_profile(raw_pol.decode("utf-8")) if raw_pol else (set(), set())
                        )
                        valid_tickers.append(
                            {t for t in raw_tick.decode("utf-8").split(",") if t} if raw_tick else set()
                        )

                if valid_vecs:
                    # 5. Vectorized cosine similarity via matrix dot product
                    V_active = np.stack(valid_vecs)  # shape (N, 384)
                    similarities = V_active @ embedding  # shape (N,)

                    # 6. Find best match that passes both gates
                    sorted_indices = np.argsort(similarities)[::-1]  # descending

                    for idx in sorted_indices:
                        score = float(similarities[idx])
                        if score < self.cosine_threshold:
                            break  # No more candidates above threshold

                        cand_tickers = valid_tickers[idx]
                        if new_tickers and cand_tickers and new_tickers.isdisjoint(cand_tickers):
                            logger.debug(
                                f"Semantic ticker gate: rejected match between "
                                f"event={event_id[:16]}... (tickers={sorted(new_tickers)}) "
                                f"and candidate={valid_ids[idx][:16]}... "
                                f"(tickers={sorted(cand_tickers)}, cosine={score:.4f})"
                            )
                            continue

                        if has_conflict(new_polarity, valid_polarities[idx]):
                            logger.info(
                                f"Semantic polarity guard: blocked match between "
                                f"event={event_id[:16]}... (polarity={new_polarity}) "
                                f"and candidate={valid_ids[idx][:16]}... "
                                f"(polarity={valid_polarities[idx]}, cosine={score:.4f})"
                            )
                            continue

                        best_score = score
                        best_match_id = valid_ids[idx]
                        logger.info(
                            f"Semantic duplicate detected: event={event_id[:16]}... "
                            f"matches canonical={best_match_id[:16]}... "
                            f"(cosine={best_score:.4f})"
                        )
                        break  # Found best valid match

            # 7. Store the new vector, polarity, tickers, and register in window
            vec_bytes = embedding.astype(np.float32).tobytes()
            pol_str = serialize_profile(new_polarity)
            tick_str = ",".join(sorted(new_tickers))
            pipe = self.client.pipeline()
            pipe.set(self._vec_key(event_id), vec_bytes, ex=self.semantic_ttl)
            pipe.set(self._pol_key(event_id), pol_str.encode("utf-8"), ex=self.semantic_ttl)
            if tick_str:
                pipe.set(self._tick_key(event_id), tick_str.encode("utf-8"), ex=self.semantic_ttl)
            pipe.zadd(WINDOW_ZSET_KEY, {event_id_bytes: now})
            pipe.expire(WINDOW_ZSET_KEY, self.semantic_ttl + 60)  # safety margin
            pipe.execute()

            if best_match_id:
                return True, best_match_id, best_score
            return False, None, best_score

        except Exception as e:
            logger.error(f"Semantic dedup error for {event_id}: {e}. Fail-open.")
            return False, None, 0.0
