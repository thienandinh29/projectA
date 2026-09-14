import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch
import duckdb
from lakehouse.db import LakehouseManager
from lakehouse.records import KafkaEnvelope, compare_events
from models.event import CommonEvent

SOURCES = {'rss':'RSS', 'gdelt':'GDELT', 'sec':'SEC'}
PUBLISHED = datetime(2026,9,1,14,tzinfo=timezone.utc)


def event(source='RSS', eid='news', **changes):
    obj = CommonEvent(id=eid,source=source,title='Tesla beats earnings estimates',
                      published_at=PUBLISHED,ingested_time=PUBLISHED+timedelta(minutes=5))
    return obj.model_copy(update=changes)


def envelope(e, offset=0, topic=None, partition=0):
    return KafkaEnvelope(topic or e.source.lower(),partition,offset,e.to_json_bytes(),123,b'key',[('header',b'\xff')])


class TestMessageStorage(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name)/'lake.duckdb')
        self.lake = LakehouseManager(self.path)

    def tearDown(self):
        self.lake.close()
        self.tmp.cleanup()

    def write(self, messages):
        return self.lake.write_message_batch(messages, SOURCES)

    def test_replay_and_redelivery_preserve_first_version(self):
        original = envelope(event())
        self.assertEqual(self.write([original])['silver_inserted'],1)
        changed = event(title='Tesla misses earnings estimates', ingested_time=PUBLISHED+timedelta(days=1))
        self.assertEqual(self.write([original])['replayed'],1)
        self.assertEqual(self.write([envelope(changed,1)])['conflicts'],1)
        row = self.lake.conn.execute('SELECT title,ingested_time FROM silver_financial_news').fetchone()
        self.assertEqual(row,('Tesla beats earnings estimates',PUBLISHED+timedelta(minutes=5)))
        self.assertEqual(self.lake.conn.execute('SELECT COUNT(*) FROM bronze_events_raw').fetchone()[0],2)
        self.assertEqual(self.lake.conn.execute('SELECT COUNT(*) FROM lakehouse_message_outcomes').fetchone()[0],2)

    def test_exact_bytes_headers_and_null_invalid_payloads_preserved(self):
        blobs = [b'\xffnot-json',None,b'[]',b'{ "id": "missing fields" }']
        result = self.write([KafkaEnvelope('rss',0,i,raw,key=b'\x00',headers=[('opaque',b'\xff')]) for i,raw in enumerate(blobs)])
        self.assertEqual(result['rejected'],4)
        self.assertEqual(self.lake.conn.execute('SELECT raw_bytes FROM bronze_events_raw ORDER BY kafka_offset').fetchall(),[(b,) for b in blobs])
        self.assertEqual(json.loads(self.lake.conn.execute('SELECT kafka_headers FROM bronze_events_raw LIMIT 1').fetchone()[0]),[['opaque','/w==']])
        self.assertEqual(self.lake.conn.execute('SELECT COUNT(*) FROM lakehouse_rejections WHERE error IS NOT NULL').fetchone()[0],4)

    def test_timestamp_embedding_and_source_validation(self):
        base = event().model_dump(mode='json')
        invalid = []
        for changes in ({'published_at':'2026-09-01T14:00:00'}, {'ingested_time':None},
                        {'event_time':'2026-09-01T14:00:00'}, {'embedding':[1.0]},
                        {'embedding':[float('nan')]*384}, {'metadata':{'bad':float('inf')}},
                        {'source':'GDELT'}):
            invalid.append({**base,**changes})
        invalid.append({k:v for k,v in base.items() if k!='ingested_time'})
        result = self.write([KafkaEnvelope('rss',0,i,json.dumps(obj).encode()) for i,obj in enumerate(invalid)])
        self.assertEqual(result['rejected'],len(invalid))
        self.assertEqual(result['silver_inserted'],0)

    def test_whitespace_and_cross_source_metadata_are_not_conflicts(self):
        self.write([envelope(event(content_snippet='Source A summary'))])
        variant = event('GDELT', title=' Tesla  beats earnings estimates ',content_snippet='Source B summary',metadata={'domain':'other'})
        result = self.write([envelope(variant,1)])
        self.assertEqual(result['conflicts'],0)
        self.assertEqual(result['redelivered'],1)
        self.assertEqual(self.lake.conn.execute('SELECT source FROM silver_financial_news').fetchone()[0],'RSS')

    def test_enrichment_is_audited_without_changing_silver_or_pit(self):
        self.write([envelope(event())])
        enriched = event(content_full='Later full article', embedding=[1.0]+[0.0]*383,tickers_mentioned=['TSLA'])
        result = self.write([envelope(enriched,1)])
        self.assertEqual(result['enrichment'],1)
        self.assertEqual(result['conflicts'],0)
        self.assertEqual(self.lake.conn.execute('SELECT content_full,embedding FROM silver_financial_news').fetchone(),(None,None))
        self.assertEqual(self.lake.query_news_as_of(PUBLISHED+timedelta(minutes=3)),[])
        self.assertEqual(len(self.lake.query_news_as_of(PUBLISHED+timedelta(minutes=6))),1)
        self.lake.close()
        self.lake = LakehouseManager(self.path)
        self.assertEqual(self.write([envelope(event())])['replayed'],1)
        self.assertEqual(self.lake.query_news_as_of(PUBLISHED+timedelta(minutes=3)),[])

    def test_conflict_and_enrichment_both_recorded(self):
        self.write([envelope(event())])
        result=self.write([envelope(event(title='Changed title',content_full='New full text'),1)])
        self.assertEqual((result['conflicts'],result['enrichment']),(1,1))
        row=self.lake.conn.execute('SELECT conflicts,enrichment FROM lakehouse_conflicts').fetchone()
        self.assertIn('title',json.loads(row[0]))
        self.assertIn('content_full',json.loads(row[1]))

    def test_sec_identity_and_existing_content_changes_are_conflicts(self):
        meta={'accession_number':'filing','cik':'320193','form_type':'8-K','company_name':'Apple Inc.'}
        filing=event('SEC','filing',title='[8-K] Apple Inc.',metadata=meta,content_full='Original filing')
        self.write([envelope(filing)])
        normalized=filing.model_copy(update={'metadata':{**meta,'cik':'0000320193','form_type':'8-k','company_name':' Apple  Inc. '}})
        self.assertEqual(self.write([envelope(normalized,1)])['conflicts'],0)
        for i,field in enumerate(('cik','form_type','company_name'),2):
            variant=filing.model_copy(update={'metadata':{**meta,field:'different'}})
            self.assertEqual(self.write([envelope(variant,i)])['conflicts'],1)
            without_identity=filing.model_copy(update={'metadata':{k:v for k,v in meta.items() if k!=field}})
            conflicts,_,_=compare_events(filing.model_dump(mode='json'),without_identity.model_dump(mode='json'))
            self.assertIn(field,conflicts)
        self.assertEqual(self.write([envelope(filing.model_copy(update={'content_full':'Changed filing'}),5)])['conflicts'],1)
        self.assertEqual(self.write([envelope(filing.model_copy(update={'tickers_mentioned':['AAPL']}),6)])['enrichment'],1)
        self.assertEqual(json.loads(self.lake.conn.execute('SELECT first_event FROM silver_sec_filings').fetchone()[0])['content_full'],'Original filing')

    def test_storage_failure_rolls_back_bronze_and_outcomes(self):
        with patch.object(self.lake,'insert_news_batch',side_effect=RuntimeError('Injected DB failure')):
            with self.assertRaises(RuntimeError):
                self.write([KafkaEnvelope('rss',0,0,b'invalid'),envelope(event(),1)])
        for table in ('bronze_events_raw','silver_financial_news','lakehouse_message_outcomes'):
            self.assertEqual(self.lake.conn.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0],0)

    def test_sec_missing_text_is_not_silently_enriched(self):
        filing=event('SEC','filing',metadata={'accession_number':'filing'})
        self.write([envelope(filing)])
        revised=filing.model_copy(update={'content_full':'Newly supplied filing text',
                                          'tickers_mentioned':['AAPL']})
        result=self.write([envelope(revised,1)])
        self.assertEqual((result['conflicts'],result['enrichment']),(1,1))
        audit=self.lake.conn.execute('SELECT conflicts,enrichment FROM lakehouse_conflicts').fetchone()
        self.assertIn('content_full',json.loads(audit[0]))
        self.assertIn('tickers_mentioned',json.loads(audit[1]))
        self.assertIsNone(json.loads(self.lake.conn.execute('SELECT first_event FROM silver_sec_filings').fetchone()[0])['content_full'])

    def test_malformed_sec_identity_is_a_rejection_not_a_batch_failure(self):
        for offset,(field,value) in enumerate((('form_type',1),('company_name',None),('cik',[]))):
            invalid=event('SEC',f'filing-{offset}',metadata={field:value})
            self.assertEqual(self.write([envelope(invalid,offset)])['rejected'],1)
        self.assertEqual(self.lake.conn.execute('SELECT COUNT(*) FROM silver_sec_filings').fetchone()[0],0)

    def test_invalid_transport_is_quarantined_and_good_offsets_remain_committable(self):
        invalid = KafkaEnvelope('rss', -1, 0, b'not-json')
        good = envelope(event(eid='good'), 7)
        result = self.write([invalid, good])
        self.assertEqual((result['transport_rejected'], result['silver_inserted']), (1, 1))
        fault = self.lake.conn.execute(
            'SELECT kafka_partition, kafka_offset, reason, raw_bytes FROM lakehouse_transport_faults').fetchone()
        self.assertEqual(fault[:2], (-1, 0))
        self.assertIn('partition', fault[2])
        self.assertEqual(fault[3], b'not-json')
        self.assertEqual(self.lake.conn.execute('SELECT COUNT(*) FROM bronze_events_raw').fetchone()[0], 1)

    def test_supplementary_and_ordering_changes_are_not_content_conflicts(self):
        a=event(tickers_mentioned=['TSLA','AAPL']).model_dump(mode='json')
        b={**a,'tickers_mentioned':['aapl','TSLA','TSLA'],'canonical_cluster_id':'news'}
        self.assertEqual(compare_events(a,b),({},{},{}))

    def test_silver_api_counts_only_new_identity_rows(self):
        article=event()
        self.assertEqual(self.lake.insert_news_batch([article,article]),1)
        self.assertEqual(self.lake.insert_news_batch([article]),0)
        filing=event('SEC','filing',metadata={'accession_number':'filing'})
        self.assertEqual(self.lake.insert_sec_batch([filing,filing]),1)
        self.assertEqual(self.lake.insert_sec_batch([filing]),0)


class TestMigration(unittest.TestCase):
    def test_legacy_backup_and_transactional_migration(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'legacy.duckdb'
            conn=duckdb.connect(str(path))
            conn.execute(Path('lakehouse/schema.sql').read_text(encoding='utf-8'))
            conn.execute("INSERT INTO bronze_events_raw VALUES ('rss',0,0,'legacy','{}',CURRENT_TIMESTAMP)")
            conn.close()
            lake=LakehouseManager(str(path))
            try:
                self.assertEqual(lake.conn.execute('SELECT raw_bytes,provenance_status FROM bronze_events_raw').fetchone(),(None,'legacy_bytes_unavailable'))
                self.assertEqual(lake.conn.execute('SELECT status FROM lakehouse_message_outcomes').fetchone()[0],'legacy_unprocessed')
                backups=list(Path(tmp).glob('*.bak'))
                self.assertEqual(len(backups),1)
                snapshot=duckdb.connect(str(backups[0]),read_only=True)
                self.assertEqual(snapshot.execute('SELECT COUNT(*) FROM bronze_events_raw').fetchone()[0],1)
                snapshot.close()
                lake._init_schema()
                self.assertEqual(len(list(Path(tmp).glob('*.bak'))),1)
            finally:
                lake.close()

    def test_failed_migration_rolls_back_and_preserves_original(self):
        from lakehouse.migrations import migrate
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'legacy.duckdb'
            conn=duckdb.connect(str(path))
            conn.execute('CREATE TABLE original(i INTEGER); INSERT INTO original VALUES (7)')
            ddl=Path(tmp)/'bad.sql'
            ddl.write_text('CREATE TABLE transient(i INTEGER); SELECT nonexistent;',encoding='utf-8')
            with self.assertRaises(duckdb.Error):
                migrate(conn,path,ddl)
            self.assertEqual(conn.execute('SELECT * FROM original').fetchall(),[(7,)])
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name='transient'").fetchone()[0],0)
            conn.close()


if __name__=='__main__':
    unittest.main()
