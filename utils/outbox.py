"""Persist outgoing messages until Kafka acknowledges them (at-least-once)."""
import sqlite3
import json
import uuid
from contextlib import contextmanager
from pathlib import Path


class DeliveryOutbox:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as conn:
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('CREATE TABLE IF NOT EXISTS pending ('
                         'topic TEXT NOT NULL, event_id TEXT NOT NULL, payload BLOB NOT NULL, '
                         'PRIMARY KEY(topic, event_id))')
            conn.execute('''CREATE TABLE IF NOT EXISTS source_cycles (
                cycle_id TEXT PRIMARY KEY, topic TEXT NOT NULL,
                started_at TEXT, finished_at TEXT NOT NULL, statistics TEXT NOT NULL)''')

    def record_cycle(self, topic, started_at, finished_at, statistics):
        with self.connect() as conn:
            conn.execute('INSERT INTO source_cycles VALUES (?,?,?,?,?)',
                         [str(uuid.uuid4()), topic, started_at, finished_at, json.dumps(statistics, sort_keys=True)])

    @contextmanager
    def connect(self):
        conn = sqlite3.connect(str(self.path), timeout=10)
        try:
            with conn:
                yield conn
        finally:
            conn.close()

    def enqueue(self, topic, event_id, payload):
        # Preserve the first wire payload and its original ingestion timestamp.
        with self.connect() as conn:
            conn.execute('INSERT OR IGNORE INTO pending VALUES (?, ?, ?)',
                         (topic, event_id, payload))
            return conn.execute('SELECT payload FROM pending WHERE topic=? AND event_id=?',
                                (topic, event_id)).fetchone()[0]

    def pending(self, topic=None, limit=100):
        with self.connect() as conn:
            if topic is None:
                return conn.execute('SELECT topic, event_id, payload FROM pending ORDER BY rowid LIMIT ?',
                                    (limit,)).fetchall()
            return conn.execute('SELECT topic, event_id, payload FROM pending WHERE topic=? ORDER BY rowid LIMIT ?',
                                (topic, limit)).fetchall()

    def acknowledge(self, topic, event_id):
        with self.connect() as conn:
            conn.execute('DELETE FROM pending WHERE topic=? AND event_id=?', (topic, event_id))

    def count(self, topic):
        with self.connect() as conn:
            return conn.execute('SELECT COUNT(*) FROM pending WHERE topic=?', (topic,)).fetchone()[0]
