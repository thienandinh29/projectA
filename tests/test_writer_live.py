"""Opt-in isolated broker tests, including actual hard process exits.

RUN_LIVE_STORAGE_TESTS=1 python -m unittest tests.test_writer_live -v
No production topics, groups, Redis keys or database files are used.
"""
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid
from datetime import timedelta
from pathlib import Path
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from lakehouse.db import LakehouseManager
from lakehouse.sync import LakehouseWriter
from tests.test_storage import event, PUBLISHED

BROKER=os.environ.get('STORAGE_TEST_BROKER','localhost:9092')


def crash_child(mode, path, group, sources, report):
    def factory(config):
        config['session.timeout.ms']=6000
        consumer=Consumer(config)
        if mode!='after-db':
            return consumer
        class CrashBeforeKafkaCommit:
            def __getattr__(self,name): return getattr(consumer,name)
            def commit(self,**kwargs): os._exit(62)
        return CrashBeforeKafkaCommit()
    writer=LakehouseWriter(db_path=path,group_id=group,topic_sources=json.loads(sources),
        broker=BROKER,consumer_factory=factory,batch_size=8,batch_seconds=5,report_lag=True)
    if mode=='before-db':
        insert=writer.lakehouse.insert_news_batch
        def crash(events):
            insert(events)
            os._exit(61)  # hard exit with an open DuckDB transaction
        writer.lakehouse.insert_news_batch=crash
    counts=writer.run(once=True,idle_timeout=30)
    Path(report).write_text(json.dumps(counts),encoding='utf-8')


@unittest.skipUnless(os.environ.get('RUN_LIVE_STORAGE_TESTS')=='1','Opt-in isolated live broker test')
class TestWriterLive(unittest.TestCase):
    def test_actual_rebalance_discards_buffer_then_restart_recovers(self):
        topic='storage-test-'+uuid.uuid4().hex+'-rebalance'
        group=topic+'-group'
        admin=AdminClient({'bootstrap.servers':BROKER})
        admin.create_topics([NewTopic(topic,2,1)])[topic].result(timeout=15)
        stop=threading.Event()
        thread=None
        observer=None
        try:
            producer=Producer({'bootstrap.servers':BROKER,'acks':'all'})
            for p in range(2):
                producer.produce(topic,partition=p,value=event(eid=f'news-{p}').to_json_bytes())
            self.assertEqual(producer.flush(15),0)
            with tempfile.TemporaryDirectory() as tmp:
                path=str(Path(tmp)/'rebalance.duckdb')
                writer=LakehouseWriter(db_path=path,group_id=group,topic_sources={topic:'RSS'},
                    broker=BROKER,stop_event=stop,batch_size=1000,batch_seconds=120,report_lag=False)
                revoked=threading.Event()
                discard=[]
                original=writer.on_revoke
                def on_revoke(consumer,partitions):
                    discard.append(len(writer.buffer))
                    original(consumer,partitions)
                    revoked.set()
                writer.on_revoke=on_revoke
                failures=[]
                def run():
                    try: writer.run()
                    except BaseException as exc: failures.append(exc)
                thread=threading.Thread(target=run)
                thread.start()
                deadline=time.monotonic()+20
                while len(writer.buffer)<2 and not failures and time.monotonic()<deadline:
                    time.sleep(.05)
                self.assertEqual(len(writer.buffer),2,failures)
                observer=Consumer({'bootstrap.servers':BROKER,'group.id':group,
                    'enable.auto.commit':False,'enable.auto.offset.store':False})
                observer.subscribe([topic])
                deadline=time.monotonic()+20
                while not revoked.is_set() and time.monotonic()<deadline:
                    observer.poll(.2)
                self.assertTrue(revoked.is_set(),'Real broker never revoked partitions')
                self.assertGreater(discard[0],0)
                # Wait for the writer's new single-partition assignment to
                # replay its retained responsibility before graceful shutdown.
                deadline=time.monotonic()+10
                while not writer.buffer and not failures and time.monotonic()<deadline:
                    observer.poll(.2)
                stop.set()
                thread.join(10)
                self.assertFalse(thread.is_alive())
                self.assertEqual(failures,[])
                observer.close()
                observer=None
                # The standalone consumer never persisted/committed its
                # partition. The sole restarted DB writer must recover it.
                restarted=LakehouseWriter(db_path=path,group_id=group,topic_sources={topic:'RSS'},
                    broker=BROKER,batch_size=2,batch_seconds=1,report_lag=False)
                result=restarted.run(once=True,idle_timeout=20)
                lake=LakehouseManager(path)
                try:
                    self.assertEqual(lake.conn.execute('SELECT COUNT(*) FROM bronze_events_raw').fetchone()[0],2)
                    self.assertEqual(lake.conn.execute('SELECT COUNT(*) FROM silver_financial_news').fetchone()[0],2)
                finally: lake.close()
                self.assertGreaterEqual(result.get('bronze_inserted',0),1)
        finally:
            stop.set()
            if thread: thread.join(10)
            if observer: observer.close()
            admin.delete_topics([topic])[topic].result(timeout=15)
            try: admin.delete_consumer_groups([group])[group].result(timeout=15)
            except Exception: pass

    def test_hard_crashes_mixed_topics_offsets_and_pit_restart(self):
        prefix='storage-test-'+uuid.uuid4().hex
        sources={prefix+'-rss':'RSS',prefix+'-gdelt':'GDELT',prefix+'-sec':'SEC'}
        groups=[prefix+'-before',prefix+'-after']
        admin=AdminClient({'bootstrap.servers':BROKER})
        admin.list_topics(timeout=10)
        created=[]
        try:
            for topic,future in admin.create_topics([NewTopic(t,2,1) for t in sources]).items():
                future.result(timeout=15)
                created.append(topic)
            rss,gdelt,sec=list(sources)
            meta={'accession_number':'filing','cik':'320193','form_type':'8-K','company_name':'Apple Inc.'}
            filing=event('SEC','filing',title='[8-K] Apple Inc.',metadata=meta)
            fixtures=[(rss,0,event()),(gdelt,0,event('GDELT')),
                (rss,0,event(title='Tesla misses earnings estimates')),
                (sec,0,filing),(sec,0,filing.model_copy(update={'metadata':{**meta,'company_name':'Different Company'}})),
                (rss,1,event(eid='second')),(rss,1,b'\xffinvalid'),
                (rss,0,event(content_full='Later article text'))]
            producer=Producer({'bootstrap.servers':BROKER,'acks':'all'})
            errors=[]
            for topic,partition,obj in fixtures:
                producer.produce(topic,partition=partition,value=obj if isinstance(obj,bytes) else obj.to_json_bytes(),
                                 callback=lambda err,msg:errors.append(str(err)) if err else None)
            self.assertEqual(producer.flush(15),0)
            self.assertEqual(errors,[])
            evidence={'broker':BROKER,'fixture':'Synthetic mixed RSS/GDELT/SEC; two partitions per topic',
                      'messages':len(fixtures),'phases':[]}
            with tempfile.TemporaryDirectory() as tmp:
                def run(mode,path,group,expected):
                    report=Path(tmp)/f'{mode}-{group}.json'
                    child=subprocess.run([sys.executable,'-m','tests.test_writer_live','--child',mode,str(path),group,
                        json.dumps(sources),str(report)],capture_output=True,text=True,timeout=60)
                    self.assertEqual(child.returncode,expected,child.stderr)
                    result=json.loads(report.read_text()) if report.exists() else {}
                    evidence['phases'].append({'mode':mode,'exit_code':child.returncode,'counts':result})
                    return result
                def counts(path):
                    lake=LakehouseManager(str(path))
                    try:
                        return [lake.conn.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]
                            for t in ('bronze_events_raw','lakehouse_message_outcomes','silver_financial_news','silver_sec_filings')]
                    finally: lake.close()
                def committed(group):
                    observer=Consumer({'bootstrap.servers':BROKER,'group.id':group,'enable.auto.commit':False})
                    try:
                        return observer.committed([TopicPartition(t,p) for t in sources for p in range(2)],timeout=10)
                    finally: observer.close()
                path=Path(tmp)/'before.duckdb'
                run('before-db',path,groups[0],61)
                self.assertEqual(counts(path),[0,0,0,0])
                self.assertTrue(all(p.offset<0 for p in committed(groups[0])))
                recovered=run('normal',path,groups[0],0)
                self.assertEqual((recovered['bronze_inserted'],recovered['silver_inserted'],recovered['rejected']),(8,3,1))
                self.assertEqual(counts(path),[8,8,2,1])
                expected={(rss,0):3,(rss,1):2,(gdelt,0):1,(sec,0):2}
                actual={(p.topic,p.partition):p.offset for p in committed(groups[0]) if p.offset>=0}
                self.assertEqual(actual,expected)
                path2=Path(tmp)/'after.duckdb'
                run('after-db',path2,groups[1],62)
                self.assertEqual(counts(path2),[8,8,2,1])
                self.assertTrue(all(p.offset<0 for p in committed(groups[1])))
                replayed=run('normal',path2,groups[1],0)
                self.assertEqual((replayed['replayed'],replayed['silver_inserted']),(8,0))
                self.assertEqual(counts(path2),[8,8,2,1])
                lake=LakehouseManager(str(path2))
                self.assertEqual(lake.query_news_as_of(PUBLISHED+timedelta(minutes=3)),[])
                visible=lake.query_news_as_of(PUBLISHED+timedelta(minutes=6))
                self.assertEqual({row['id'] for row in visible},{'news','second'})
                self.assertEqual({row['title'] for row in visible},{'Tesla beats earnings estimates'})
                self.assertIsNone(lake.conn.execute("SELECT content_full FROM silver_financial_news WHERE id='news'").fetchone()[0])
                lake.close()
                evidence['final_table_counts']=[8,8,2,1]
                evidence['restart_pit']={'hidden_at':'14:03 UTC','visible_at':'14:06 UTC','first_titles_preserved':True}
            Path('docs/storage-live-validation.json').write_text(json.dumps(evidence,indent=2)+'\n',encoding='utf-8')
        finally:
            for topic,future in admin.delete_topics(created).items() if created else []:
                future.result(timeout=15)
            for group,future in admin.delete_consumer_groups(groups).items():
                try: future.result(timeout=15)
                except Exception: pass  # failed pre-join groups may not exist


if __name__=='__main__':
    if len(sys.argv)>1 and sys.argv[1]=='--child':
        crash_child(*sys.argv[2:])
    else:
        unittest.main()
