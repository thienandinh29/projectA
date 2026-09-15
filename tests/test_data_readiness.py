import tempfile
import unittest
import csv
from datetime import datetime, timezone
from pathlib import Path

from lakehouse.db import LakehouseManager
from lakehouse.records import KafkaEnvelope
from scripts.audit_data_readiness import build_report
from scripts.export_nlp_label_sample import export_sample
from tests.test_storage import SOURCES, envelope, event


class TestDataReadiness(unittest.TestCase):
    def test_report_uses_storage_evidence_and_exposes_open_gates(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / 'lake.duckdb')
            lake = LakehouseManager(path)
            lake.write_message_batch([
                envelope(event(), 0),
                envelope(event('SEC', 'filing', metadata={'accession_number':'filing'}), 0),
            ], SOURCES)
            lake.close()
            report = build_report(path, now=datetime(2026, 9, 2, tzinfo=timezone.utc))
            self.assertEqual(report['storage']['schema_version'], 4)
            self.assertEqual(report['collection']['bronze_rows'], 2)
            self.assertEqual(report['collection']['outcome_rows'], 2)
            self.assertTrue(report['gates']['bronze_outcome_audit'])
            self.assertFalse(report['gates']['sustained_collection_window'])
            self.assertFalse(report['automated_continuous_data_ready'])

    def test_unresolved_transport_fault_fails_gate(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / 'lake.duckdb')
            lake = LakehouseManager(path)
            lake.write_message_batch([KafkaEnvelope('rss', -1, 0, b'bad')], SOURCES)
            lake.close()
            report = build_report(path)
            self.assertEqual(report['storage']['unresolved_transport_faults'], 1)
            self.assertFalse(report['gates']['no_unresolved_transport_faults'])

    def test_label_sample_is_balanced_reproducible_and_unlabeled(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / 'lake.duckdb')
            lake = LakehouseManager(path)
            messages = [envelope(event('RSS', f'rss-{i}'), i) for i in range(3)]
            messages += [envelope(event('GDELT', f'gdelt-{i}'), i) for i in range(3)]
            lake.write_message_batch(messages, SOURCES)
            lake.close()
            first, second = Path(tmp) / 'first.csv', Path(tmp) / 'second.csv'
            self.assertEqual(export_sample(path, first, total=4), 4)
            export_sample(path, second, total=4)
            self.assertEqual(first.read_bytes(), second.read_bytes())
            with first.open(encoding='utf-8-sig', newline='') as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual({source: sum(r['source'] == source for r in rows)
                              for source in ('RSS','GDELT')}, {'RSS':2,'GDELT':2})
            self.assertTrue(all(not row['sentiment_label'] for row in rows))


if __name__ == '__main__':
    unittest.main()
