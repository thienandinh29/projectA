"""Synthetic, isolated Redis fixtures; never flush a database."""
import json
import uuid
from utils.redis_cache import RedisDeduplicator

QUERY = 'Federal Reserve raises interest rates by 25 basis points to curb inflation'
VARIANT = QUERY.replace('curb', 'battle')


class ScopedRedis:
    def __init__(self, client, prefix=None, keys=None):
        self.client = client
        self.prefix = prefix or f'tier2-evidence:{uuid.uuid4()}:'.encode()
        self.keys = keys if keys is not None else set()

    def pipeline(self):
        return ScopedRedis(self.client.pipeline(), self.prefix, self.keys)

    def execute(self):
        return self.client.execute()

    def __getattr__(self, name):
        def command(key, *args, **kwargs):
            key = self.prefix + key
            self.keys.add(key)
            if name == 'delete':
                args = tuple(self.prefix + k for k in args)
                self.keys.update(args)
            return getattr(self.client, name)(key, *args, **kwargs)
        return command

    def cleanup(self):
        keys = list(self.keys)
        for start in range(0, len(keys), 512):
            self.client.delete(*keys[start:start + 512])


def seed_candidates(scoped, count, terminal_only=False):
    """Default uses actual LSH signatures. terminal_only deliberately places
    sub-threshold records in matching buckets to isolate verification batching.
    """
    dedup = RedisDeduplicator()
    bytes_total, signatures = 0, {}
    pipe = scoped.pipeline()
    for i in range(count):
        terminal = i == count - 1
        title = QUERY if terminal else VARIANT
        stored = 'Unrelated geopolitical report' if terminal_only and not terminal else title
        payload = json.dumps({'title': stored, 'shingles': sorted(dedup._create_shingles(stored)),
                              'canonical_id': 'original' if terminal else f'other-{i}'}).encode()
        eid = b'z-terminal' if terminal else f'a{i:06}'.encode()
        pipe.set(b'lsh:news:v2:event:' + eid, payload, ex=3600)
        bytes_total += len(payload)
        if title not in signatures:
            signatures[title] = dedup._create_minhash(title).hashvalues
        for b in range(32):
            key = f'lsh:news:v2:b:{b}:'.encode() + signatures[title][b * 4:b * 4 + 4].tobytes()
            pipe.sadd(key, eid)
            pipe.expire(key, 3600)
        if (i + 1) % 100 == 0:
            pipe.execute()
            pipe = scoped.pipeline()
    pipe.execute()
    return bytes_total
