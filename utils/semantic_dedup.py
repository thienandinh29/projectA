"""
Tier 3: Semantic Deduplication via all-MiniLM-L6-v2 Embeddings

Uses a Redis 120-minute sliding window of L2-normalized 384-dim vectors.
Cosine similarity is computed as a simple dot product (since vectors are unit-length).

Storage Layout in Redis:
  - emb:vec:<event_id>  →  768 raw bytes (384 × float32)  TTL = 7200s
  - emb:window:zset     →  sorted set scored by UTC epoch  TTL = managed via ZREMRANGEBYSCORE

Workflow:
  1. Prune expired entries from the sliding window (score < now - 7200).
  2. MGET all active vectors from the window.
  3. Compute cosine similarities via NumPy matrix dot product.
  4. If max(similarity) >= 0.85 → flag as semantic duplicate with canonical cluster ID.
  5. Store the new vector and register in the window.
"""

import time
import re
import logging
from typing import Optional, Tuple, List, Set

import numpy as np
import redis

from config import REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_SEMANTIC_TTL_SECONDS

logger = logging.getLogger(__name__)

# Polarity keywords — must match redis_cache.py for consistent protection
# Includes both base and inflected forms for robust matching
POLARITY_KEYWORDS = {
    "beat", "beats", "miss", "misses",
    "surge", "surges", "plunge", "plunges",
    "soar", "soars", "crash", "crashes",
    "upgrade", "upgrades", "downgrade", "downgrades",
    "rise", "rises", "fall", "falls",
    "jump", "jumps", "sink", "sinks",
    "raise", "raises", "cut", "cuts",
    "hike", "hikes", "lower", "lowers",
    "gain", "gains", "loss", "losses",
    "bullish", "bearish",
    "rally", "rallies", "drop", "drops", "decline", "declines",
}

COSINE_THRESHOLD = 0.65
EMBEDDING_DIM = 384
WINDOW_ZSET_KEY = b"emb:window:zset"


class SemanticDeduplicator:
    """
    Redis-backed 120-minute sliding window for semantic near-duplicate detection.

    Uses L2-normalized embeddings from all-MiniLM-L6-v2 and computes cosine
    similarity as dot product via NumPy vectorized matrix multiplication.
    """

    def __init__(
        self,
        host: str = REDIS_HOST,
        port: int = REDIS_PORT,
        db: int = REDIS_DB,
        semantic_ttl_seconds: int = REDIS_SEMANTIC_TTL_SECONDS,
        cosine_threshold: float = COSINE_THRESHOLD,
    ):
        self.semantic_ttl = semantic_ttl_seconds
        self.cosine_threshold = cosine_threshold
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

    @staticmethod
    def _extract_polarity(title: str) -> Set[str]:
        """Extract polarity keywords from a headline."""
        tokens = set(re.sub(r"[^\w\s]", " ", title.lower()).split())
        return tokens & POLARITY_KEYWORDS

    def _polarity_conflict(self, pol_a: Set[str], pol_b: Set[str]) -> bool:
        """
        Returns True if two polarity sets conflict (i.e., they contain
        different polarity keywords, indicating opposite sentiment).
        If either set is empty, there is no conflict.
        """
        if not pol_a or not pol_b:
            return False
        return pol_a != pol_b

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
    ) -> Tuple[bool, Optional[str], float]:
        """
        Checks whether the given embedding is a semantic duplicate of any
        vector currently in the 120-minute sliding window.

        Includes polarity protection: if the incoming headline and a candidate
        contain different polarity keywords (e.g., "beats" vs "misses"),
        the match is rejected even if cosine similarity is high.

        Args:
            event_id:  Unique identifier for the incoming event.
            embedding: L2-normalized np.ndarray of shape (384,), dtype float32.
            title:     Headline text for polarity extraction.

        Returns:
            (is_semantic_dup, canonical_id, best_cosine_score)
            - is_semantic_dup: True if cosine >= threshold with any active vector.
            - canonical_id:    The event_id of the best-matching canonical story.
            - best_cosine_score: Highest cosine similarity found (0.0 if none).
        """
        try:
            now = time.time()
            new_polarity = self._extract_polarity(title)

            # 1. Prune expired entries
            self._prune_window(now)

            # 2. Get all active event IDs from the window
            active_members: List[bytes] = self.client.zrangebyscore(
                WINDOW_ZSET_KEY, now - self.semantic_ttl, "+inf"
            )

            # Filter out self
            event_id_bytes = event_id.encode("utf-8")
            active_members = [m for m in active_members if m != event_id_bytes]

            best_score = 0.0
            best_match_id = None

            if active_members:
                # 3. Pipeline-fetch all active vectors AND polarity sets
                pipe = self.client.pipeline()
                for m in active_members:
                    pipe.get(b"emb:vec:" + m)
                    pipe.get(b"emb:pol:" + m)
                raw_results = pipe.execute()

                # 4. Build matrix of active vectors with polarity info
                valid_ids: List[str] = []
                valid_vecs: List[np.ndarray] = []
                valid_polarities: List[Set[str]] = []
                expected_bytes = EMBEDDING_DIM * 4  # float32 = 4 bytes

                for i, member in enumerate(active_members):
                    raw_vec = raw_results[i * 2]
                    raw_pol = raw_results[i * 2 + 1]

                    if raw_vec and len(raw_vec) == expected_bytes:
                        vec = np.frombuffer(raw_vec, dtype=np.float32).copy()
                        valid_vecs.append(vec)
                        valid_ids.append(member.decode("utf-8"))
                        # Deserialize polarity set
                        if raw_pol:
                            valid_polarities.append(set(raw_pol.decode("utf-8").split(",")))
                        else:
                            valid_polarities.append(set())

                if valid_vecs:
                    # 5. Vectorized cosine similarity via matrix dot product
                    V_active = np.stack(valid_vecs)  # shape (N, 384)
                    similarities = V_active @ embedding  # shape (N,)

                    # 6. Find best match that passes polarity check
                    sorted_indices = np.argsort(similarities)[::-1]  # descending

                    for idx in sorted_indices:
                        score = float(similarities[idx])
                        if score < self.cosine_threshold:
                            break  # No more candidates above threshold

                        candidate_polarity = valid_polarities[idx]
                        if self._polarity_conflict(new_polarity, candidate_polarity):
                            logger.info(
                                f"Semantic polarity guard: blocked match between "
                                f"event={event_id[:16]}... (polarity={new_polarity}) "
                                f"and candidate={valid_ids[idx][:16]}... "
                                f"(polarity={candidate_polarity}, cosine={score:.4f})"
                            )
                            continue  # Skip this candidate

                        best_score = score
                        best_match_id = valid_ids[idx]
                        logger.info(
                            f"Semantic duplicate detected: event={event_id[:16]}... "
                            f"matches canonical={best_match_id[:16]}... "
                            f"(cosine={best_score:.4f})"
                        )
                        break  # Found best valid match

            # 7. Store the new vector, polarity, and register in window
            vec_bytes = embedding.astype(np.float32).tobytes()
            pol_str = ",".join(sorted(new_polarity)) if new_polarity else ""
            pipe = self.client.pipeline()
            pipe.set(self._vec_key(event_id), vec_bytes, ex=self.semantic_ttl)
            if pol_str:
                pipe.set(self._pol_key(event_id), pol_str.encode("utf-8"), ex=self.semantic_ttl)
            pipe.zadd(WINDOW_ZSET_KEY, {event_id_bytes: now})
            pipe.expire(WINDOW_ZSET_KEY, self.semantic_ttl + 60)  # safety margin
            pipe.execute()

            if best_match_id:
                return True, best_match_id, best_score
            return False, None, best_score

        except Exception as e:
            logger.error(f"Semantic dedup error for {event_id}: {e}. Fail-open.")
            return False, None, 0.0

