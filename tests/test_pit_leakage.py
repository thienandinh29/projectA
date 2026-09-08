import unittest
import tempfile
from pathlib import Path
from datetime import datetime, timezone, timedelta
from models.event import CommonEvent
from lakehouse.db import LakehouseManager


class TestPointInTimeLeakage(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "test_lakehouse.duckdb"
        self.lakehouse = LakehouseManager(db_path=str(self.db_path))

    def tearDown(self):
        self.lakehouse.close()
        self.temp_dir.cleanup()

    def test_zero_lookahead_leakage(self):
        """
        Tests that an event is invisible to as-of queries before its effective_time.
        Scenario:
          - Event Published at 14:00 UTC.
          - Pipeline delays and ingests at 14:05 UTC.
          - effective_time = GREATEST(14:00, 14:05) = 14:05 UTC.
        Query Tests:
          - As-of 14:03 (between publish & ingest): MUST RETURN EMPTY (no lookahead leakage).
          - As-of 14:06 (after ingest): MUST RETURN the event.
        """
        event_time = datetime(2026, 9, 1, 13, 59, 0, tzinfo=timezone.utc)
        published_time = datetime(2026, 9, 1, 14, 0, 0, tzinfo=timezone.utc)
        ingested_time = datetime(2026, 9, 1, 14, 5, 0, tzinfo=timezone.utc)

        event = CommonEvent(
            id="evt_leakage_test_1",
            source="RSS",
            title="NVIDIA Announces Next-Gen AI Architecture",
            content_snippet="NVIDIA revealed revolutionary GPU architecture today.",
            url="https://example.com/nvda-news",
            event_time=event_time,
            published_at=published_time,
            ingested_time=ingested_time,
            tickers_mentioned=["NVDA"],
            is_near_duplicate=False
        )

        self.lakehouse.insert_news_batch([event])

        # Test Point 1: As of 14:03 UTC (Real world had not received it through pipeline yet)
        as_of_1403 = datetime(2026, 9, 1, 14, 3, 0, tzinfo=timezone.utc)
        results_before = self.lakehouse.query_news_as_of(as_of_time=as_of_1403, ticker="NVDA")
        self.assertEqual(
            len(results_before), 0,
            "Look-ahead leakage detected! Event was returned before its effective_time."
        )

        # Test Point 2: As of 14:06 UTC (Available to trading strategy)
        as_of_1406 = datetime(2026, 9, 1, 14, 6, 0, tzinfo=timezone.utc)
        results_after = self.lakehouse.query_news_as_of(as_of_time=as_of_1406, ticker="NVDA")
        self.assertEqual(len(results_after), 1)
        self.assertEqual(results_after[0]["id"], "evt_leakage_test_1")
        # Compare actual UTC epoch seconds to account for local timezone conversion
        self.assertEqual(results_after[0]["effective_time"].timestamp(), ingested_time.timestamp())

    def test_near_duplicate_filtering_in_as_of_query(self):
        """
        Tests that near-duplicate events can be seamlessly excluded from backtest views.
        """
        t = datetime(2026, 9, 1, 15, 0, 0, tzinfo=timezone.utc)

        canonical_event = CommonEvent(
            id="canon_1",
            source="RSS",
            title="Apple Launches New iPhone Model",
            published_at=t,
            ingested_time=t,
            tickers_mentioned=["AAPL"],
            is_near_duplicate=False
        )

        duplicate_event = CommonEvent(
            id="dup_1",
            source="RSS",
            title="Apple Unveils New iPhone Model",
            published_at=t + timedelta(minutes=2),
            ingested_time=t + timedelta(minutes=2),
            tickers_mentioned=["AAPL"],
            is_near_duplicate=True,
            canonical_cluster_id="canon_1"
        )

        self.lakehouse.insert_news_batch([canonical_event, duplicate_event])

        as_of_time = t + timedelta(minutes=5)

        # Query excluding duplicates (default)
        canonical_only = self.lakehouse.query_news_as_of(as_of_time=as_of_time, exclude_duplicates=True)
        self.assertEqual(len(canonical_only), 1)
        self.assertEqual(canonical_only[0]["id"], "canon_1")

        # Query including duplicates
        all_events = self.lakehouse.query_news_as_of(as_of_time=as_of_time, exclude_duplicates=False)
        self.assertEqual(len(all_events), 2)


if __name__ == "__main__":
    unittest.main()
