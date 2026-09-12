import logging
import re
import threading
from typing import Optional, Tuple, Set, List
import redis
import numpy as np
from datasketch import MinHash
from config import REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_TTL_SECONDS, REDIS_LSH_TTL_SECONDS, REDIS_SEMANTIC_TTL_SECONDS
from utils.polarity import POLARITY_KEYWORDS, base_form

logger = logging.getLogger(__name__)

# Lazy-loaded semantic components (avoid model download at import time)
_semantic_dedup: Optional["SemanticDeduplicator"] = None
_SEMANTIC_INIT_LOCK = threading.Lock()


class RedisDeduplicator:
    """
    Two-Tier Deduplication & Clustering Engine:
      1. Tier 1 (Exact Match): URL/ID hash via atomic SET NX EX (7-day TTL).
      2. Tier 2 (Near-Duplicate Clustering): MinHash LSH on headlines
         with candidate Jaccard verification (24-hour sliding window TTL).

    LSH Math Tuning (High-Recall Filter + Precision Gate):
      - num_perm (m) = 128, num_bands (b) = 32, rows_per_band (r) = 4.
      - Theoretical S-curve inflection point: (1/b)^(1/r) = (1/32)^(1/4) ≈ 0.420.
      - Candidate retrieval probability at s=0.75: 1 - (1 - 0.75^4)^32 ≈ 99.999% (zero recall regression).
      - Step 1 (LSH) acts as a high-recall candidate net.
      - Step 2 (Exact Verification) strictly enforces m.jaccard >= 0.75, discarding sub-threshold noise.
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
        self.rows_per_band = num_perm // num_bands
        self.jaccard_threshold = jaccard_threshold

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

    def is_exact_duplicate(self, event_id: str, source: Optional[str] = None) -> bool:
        """
        Atomically checks if event_id exists via SET NX EX.
        Returns True if duplicate (already seen), False if newly set.
        """
        prefix = f"dedup:exact:{source.lower()}:" if source else "dedup:exact:"
        key = f"{prefix}{event_id}".encode("utf-8")
        try:
            was_set = self.client.set(key, b"1", nx=True, ex=self.exact_ttl)
            return not bool(was_set)
        except Exception as e:
            logger.error(f"Redis exact match error for key {key}: {e}. Fail-open.")
            return False

    # ─────────────────────────────────────────────────────────────────────────
    #  TIER 2: MINHASH LSH NEAR-DUPLICATE CLUSTERING
    # ─────────────────────────────────────────────────────────────────────────

    def _create_minhash(self, text: str) -> MinHash:
        """
        Normalizes text into word unigrams + bigrams and adds weighted polarity anchors
        to guarantee high recall for rewrites while preventing opposite market sentiment collisions.
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

        m = MinHash(num_perm=self.num_perm)
        for s in shingles:
            m.update(s.encode("utf-8"))
        return m

    def check_near_duplicate(self, event_id: str, title: str, source: str = "RSS") -> Tuple[bool, Optional[str]]:
        """
        Checks if the headline is a near-duplicate of any active headline
        within the 120-minute sliding window.

        Returns:
            (is_near_duplicate: bool, canonical_cluster_id: Optional[str])
        """
        # SEC regulatory filings must never use fuzzy deduplication
        if source.upper() == "SEC" or not title:
            return False, None

        try:
            m = self._create_minhash(title)
            sig_bytes = m.hashvalues.tobytes()

            # Query all LSH bands via Redis pipeline
            band_keys = []
            pipeline = self.client.pipeline()
            for b in range(self.num_bands):
                start = b * self.rows_per_band
                end = start + self.rows_per_band
                band_hash = m.hashvalues[start:end].tobytes()
                bkey = f"lsh:{source.lower()}:b:{b}:".encode("utf-8") + band_hash
                band_keys.append(bkey)
                pipeline.smembers(bkey)

            candidate_sets = pipeline.execute()
            event_id_bytes = event_id.encode("utf-8")
            candidates = {cand for c_set in candidate_sets for cand in c_set if cand != event_id_bytes}

            # Verification Step: retrieve saved MinHash signatures for candidates
            if candidates:
                cand_list = list(candidates)[:15]  # Evaluate up to 15 candidates
                sig_pipe = self.client.pipeline()
                for cand_id in cand_list:
                    sig_pipe.get(b"lsh:sig:" + cand_id)
                saved_sigs = sig_pipe.execute()

                best_match_id = None
                highest_jaccard = 0.0

                for cand_id, saved_sig in zip(cand_list, saved_sigs):
                    if saved_sig and len(saved_sig) == len(sig_bytes):
                        cand_hashvalues = np.frombuffer(saved_sig, dtype=m.hashvalues.dtype)
                        cand_m = MinHash(
                            num_perm=self.num_perm,
                            hashvalues=cand_hashvalues,
                            scheme=m.scheme
                        )
                        score = m.jaccard(cand_m)
                        if score >= self.jaccard_threshold and score > highest_jaccard:
                            highest_jaccard = score
                            best_match_id = cand_id.decode("utf-8")

                if best_match_id:
                    logger.info(
                        f"Detected near-duplicate: '{title[:40]}...' matches canonical '{best_match_id}' "
                        f"(Jaccard={highest_jaccard:.2f})"
                    )
                    # Record this near-duplicate into buckets with sliding TTL
                    write_pipe = self.client.pipeline()
                    event_id_bytes = event_id.encode("utf-8")
                    write_pipe.set(b"lsh:sig:" + event_id_bytes, sig_bytes, ex=self.lsh_ttl)
                    for bkey in band_keys:
                        write_pipe.sadd(bkey, event_id_bytes)
                        write_pipe.expire(bkey, self.lsh_ttl)
                    write_pipe.execute()
                    return True, best_match_id

            # Novel story (canonical event): record into buckets with sliding TTL
            write_pipe = self.client.pipeline()
            event_id_bytes = event_id.encode("utf-8")
            write_pipe.set(b"lsh:sig:" + event_id_bytes, sig_bytes, ex=self.lsh_ttl)
            for bkey in band_keys:
                write_pipe.sadd(bkey, event_id_bytes)
                write_pipe.expire(bkey, self.lsh_ttl)
            write_pipe.execute()

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
            Sets self._last_embedding (np.ndarray or None) after each call.
            Sets self._last_semantic_score (float or None) after each call.
            Workers can read these to persist in DuckDB.
        """
        self._last_embedding = None
        self._last_semantic_score = None

        if self.is_exact_duplicate(event_id, source=source):
            return True, False, None

        is_near_dup, canonical_id = self.check_near_duplicate(event_id, title, source=source)
        if is_near_dup:
            return False, True, canonical_id

        # Tier 3: Semantic Embedding Dedup (skip for SEC filings)
        if source.upper() != "SEC" and title:
            try:
                embedding, is_sem_dup, sem_canonical, sem_score = self._check_semantic(event_id, title, tickers)
                self._last_embedding = embedding
                self._last_semantic_score = sem_score if is_sem_dup else None
                if is_sem_dup:
                    return False, True, sem_canonical
            except Exception as e:
                logger.error(f"Tier 3 semantic dedup error for {event_id}: {e}. Fail-open.")

        return False, False, None

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