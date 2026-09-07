import logging
import redis
from typing import Optional
from config import REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_TTL_SECONDS

logger = logging.getLogger(__name__)


class RedisDeduplicator:
    """
    Manages state and deduplication using Redis with atomic SET NX EX.
    Guarantees idempotency and prevents race conditions across concurrent workers.
    """
    def __init__(
        self,
        host: str = REDIS_HOST,
        port: int = REDIS_PORT,
        db: int = REDIS_DB,
        ttl_seconds: int = REDIS_TTL_SECONDS
    ):
        self.ttl_seconds = ttl_seconds
        self.client = redis.Redis(
            host=host,
            port=port,
            db=db,
            decode_responses=True,
            socket_timeout=5.0
        )

    def ping(self) -> bool:
        try:
            return bool(self.client.ping())
        except Exception as e:
            logger.error(f"Failed to ping Redis at {self.client.connection_pool.connection_kwargs.get('host')}: {e}")
            return False

    def is_duplicate_or_set(self, event_id: str, source: Optional[str] = None) -> bool:
        """
        Atomically checks if the event_id has been seen within the TTL window.
        If NOT seen, sets the key with expiration (NX=True, EX=ttl_seconds).

        Returns:
            True if the event is a duplicate (already seen).
            False if the event is new (newly recorded into Redis).
        """
        prefix = f"dedup:{source.lower()}:" if source else "dedup:"
        key = f"{prefix}{event_id}"

        try:
            # redis.set with nx=True returns True if key was set, None/False if key already existed
            was_set = self.client.set(key, "1", nx=True, ex=self.ttl_seconds)
            if was_set:
                # Newly inserted -> Not a duplicate
                return False
            else:
                # Key already exists -> Duplicate
                return True
        except Exception as e:
            logger.error(f"Redis deduplication error for key {key}: {e}. Treating as non-duplicate to prevent data loss.")
            return False

    def get_ttl(self, event_id: str, source: Optional[str] = None) -> int:
        """Helper to inspect remaining TTL for an event."""
        prefix = f"dedup:{source.lower()}:" if source else "dedup:"
        key = f"{prefix}{event_id}"
        try:
            return self.client.ttl(key)
        except Exception:
            return -1
