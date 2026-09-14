"""Validate an inactive existing lakehouse on a temporary copy, never in place."""
import argparse
import hashlib
import json
import shutil
import tempfile
from pathlib import Path
import duckdb
from lakehouse.db import LakehouseManager
from models.embedder import MODEL_NAME


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db',default='data/lakehouse.duckdb')
    parser.add_argument('--output',default='docs/storage-migration-validation.json')
    args=parser.parse_args()
    source=Path(args.db).resolve()
    # An active writer prevents this read-only connection, enforcing that the
    # source is inactive before it can be copied for migration verification.
    conn=duckdb.connect(str(source),read_only=True)
    tables={'silver_financial_news':'id','silver_sec_filings':'accession_number',
            'bronze_events_raw':'kafka_topic,kafka_partition,kafka_offset'}
    snapshots={}
    for table,key in tables.items():
        cursor=conn.execute(f'SELECT * FROM {table} ORDER BY {key}')
        snapshots[table]=([c[0] for c in cursor.description],cursor.fetchall())
    conn.close()
    original_hash=digest(source)
    evidence={'source':'data/lakehouse.duckdb','source_unchanged':False,'tables':{}}
    with tempfile.TemporaryDirectory() as tmp:
        copy=Path(tmp)/'copy.duckdb'
        shutil.copy2(source,copy)
        lake=LakehouseManager(str(copy))
        try:
            backups=list(Path(tmp).glob('*.bak'))
            assert len(backups)==1,'Expected one pre-migration standalone backup'
            backup=duckdb.connect(str(backups[0]),read_only=True)
            try:
                for table,key in tables.items():
                    columns,rows=snapshots[table]
                    selected=','.join('"'+c.replace('"','""')+'"' for c in columns)
                    sql=f'SELECT {selected} FROM {table} ORDER BY {key}'
                    expected=list(rows)
                    stamped=0
                    if table=='silver_financial_news' and 'embedding_model' in columns:
                        model_index=columns.index('embedding_model')
                        embedding_index=columns.index('embedding')
                        for i,row in enumerate(rows):
                            if row[embedding_index] is not None and row[model_index] is None:
                                replacement=list(row)
                                replacement[model_index]=MODEL_NAME
                                expected[i]=tuple(replacement)
                                stamped+=1
                    assert lake.conn.execute(sql).fetchall()==expected,table+' changed unexpected old fields'
                    assert backup.execute(sql).fetchall()==rows,table+' backup differs'
                    evidence['tables'][table]={'rows':len(rows),'original_columns_preserved_except_model_stamp':True,
                        'legacy_model_stamps_added':stamped,'backup_identical':True}
            finally: backup.close()
            evidence['migration_version']=lake.conn.execute('SELECT MAX(version) FROM lakehouse_migrations').fetchone()[0]
            evidence['legacy_news_without_wire_snapshot']=lake.conn.execute('SELECT COUNT(*) FROM silver_financial_news WHERE first_event IS NULL').fetchone()[0]
            evidence['legacy_sec_without_wire_snapshot']=lake.conn.execute('SELECT COUNT(*) FROM silver_sec_filings WHERE first_event IS NULL').fetchone()[0]
        finally: lake.close()
    assert digest(source)==original_hash,'Source file changed during validation'
    evidence['source_unchanged']=True
    Path(args.output).write_text(json.dumps(evidence,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(evidence,indent=2))


if __name__=='__main__': main()
