"""Repeat controlled local size-scaling measurements under a fixed short protocol.

This is separate from final accuracy tuning. A warm Spark JVM is reused, each size
is run three times in seeded shuffled order, and inputs/settings stay fixed.
"""
import argparse
import gc
import hashlib
import json
import os
from pathlib import Path
import random
import tempfile
import time

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession,functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
import tensorflow as tf

if __package__:
    from . import train_random_forest as rf
    from .prepare_lstm_data import prepare_sequences
    from .train_lstm import calculate_stats,read_records,encode_record,build_model
    from .runtime_evidence import save_runtime
else:
    import train_random_forest as rf
    from prepare_lstm_data import prepare_sequences
    from train_lstm import calculate_stats,read_records,encode_record,build_model
    from runtime_evidence import save_runtime


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,default=Path('outputs/final-study-v1/scaling'))
    parser.add_argument('--models',nargs='+',choices=['rf','lstm'],default=['rf','lstm'])
    args=parser.parse_args()
    if args.output.exists():parser.error('Choose a new benchmark output directory')
    repo=Path(__file__).resolve().parents[1];args.output.mkdir(parents=True)
    source=repo/'data/processed/aligned-hourly-2018-v1'
    all_data=pd.read_parquet(source)
    all_data.building_id=all_data.building_id.astype(str);all_data.timestamp=pd.to_datetime(all_data.timestamp,utc=True)
    ids=sorted(all_data.building_id.unique(),key=int);random.Random(42).shuffle(ids)
    window=all_data[all_data.timestamp.ge(pd.Timestamp('2018-06-24T00:00Z'))&all_data.timestamp.lt(pd.Timestamp('2018-08-15T00:00Z'))]
    input_root=args.output/'inputs';input_root.mkdir()
    hashes=[]
    for n in [10,50,150,300,573]:
        part=window[window.building_id.isin(ids[:n])]
        path=input_root/f'buildings-{n}.parquet';part.to_parquet(path,index=False,coerce_timestamps='us')
        hashes.append(dict(buildings=n,path=str(path),sha256=hashlib.sha256(path.read_bytes()).hexdigest(),rows=len(part),ids=ids[:n]))
    del window,all_data;gc.collect()
    spark=SparkSession.builder.master('local[2]').appName('SolarOpt repeated scaling').config('spark.sql.shuffle.partitions','16').getOrCreate();spark.conf.set('spark.sql.session.timeZone','UTC');spark.conf.set('spark.sql.adaptive.coalescePartitions.enabled','false')
    records=[]
    schedule=[(repeat,n) for repeat in [1,2,3] for n in [50,150,300,573]]
    random.Random(42).shuffle(schedule)
    schedule=[(0,10)]+schedule
    try:
        for repeat,n in schedule:
            for kind in args.models:
                hourly=spark.read.parquet(str(input_root/f'buildings-{n}.parquet'))
                started=time.perf_counter()
                if kind=='rf':
                    rf.feature_cols=rf.BASE_FEATURE_COLS+rf.TEMP_FEATURE_COLS
                    features=rf.build_features(hourly).where("target_timestamp >= timestamp('2018-07-01') AND target_timestamp < timestamp('2018-08-15')")
                    assembler=VectorAssembler(inputCols=rf.feature_cols,outputCol='features')
                    frame=assembler.transform(features).select('features','label','target_timestamp').cache()
                    actual_partitions=frame.rdd.getNumPartitions()
                    train=frame.where("target_timestamp < timestamp('2018-08-01')")
                    val=frame.where("target_timestamp >= timestamp('2018-08-01') AND target_timestamp < timestamp('2018-08-08')")
                    test=frame.where("target_timestamp >= timestamp('2018-08-08')")
                    n_train,n_val,n_test=train.count(),val.count(),test.count()
                    preparation=time.perf_counter()-started
                    began=time.perf_counter()
                    model=RandomForestRegressor(numTrees=50,maxDepth=8,minInstancesPerNode=5,seed=42).fit(train)
                    training=time.perf_counter()-began
                    began=time.perf_counter();pred=model.transform(test).cache()
                    checksum=pred.agg(F.sum('prediction')).first()[0]
                    inference=time.perf_counter()-began
                    pred.unpersist();frame.unpersist()
                else:
                    seq=prepare_sequences(hourly).where("target_date >= date('2018-07-01') AND target_date < date('2018-08-15')")
                    actual_partitions=seq.rdd.getNumPartitions()
                    seq=seq.withColumn('split',F.when(F.col('target_date')<'2018-08-01','train').when(F.col('target_date')<'2018-08-08','validation').otherwise('test'))
                    with tempfile.TemporaryDirectory(prefix='solar-scaling-') as directory:
                        path=Path(directory)/'sequences';seq.write.partitionBy('split').parquet(str(path))
                        stats=calculate_stats(path/'split=train')
                        arrays=[]
                        for name in ['train','validation','test']:
                            encoded=[encode_record(row,stats,True) for row in read_records(path/f'split={name}')]
                            arrays.append((np.stack([row[0] for row in encoded]),np.stack([row[1] for row in encoded])))
                        (x_train,y_train),(x_val,y_val),(x_test,y_test)=arrays
                        n_train,n_val,n_test=len(y_train)*24,len(y_val)*24,len(y_test)*24
                        preparation=time.perf_counter()-started
                        tf.keras.backend.clear_session();tf.keras.utils.set_random_seed(42)
                        model=build_model(2,.001)
                        began=time.perf_counter();model.fit(x_train,y_train,epochs=1,batch_size=128,shuffle=False,verbose=0)
                        training=time.perf_counter()-began
                        began=time.perf_counter()
                        checksum=0.
                        for offset in range(0,len(x_test),128):checksum+=float(np.asarray(model.predict_on_batch(x_test[offset:offset+128])).sum())
                        inference=time.perf_counter()-began
                    del arrays,x_train,y_train,x_val,y_val,x_test,y_test;gc.collect()
                if (n_train,n_val,n_test)!=(n*31*24,n*7*24,n*7*24):raise ValueError('Unexpected benchmark split coverage')
                if not np.isfinite(checksum):raise ValueError('Nonfinite benchmark predictions')
                if repeat:
                    records.append(dict(model=kind,repeat=repeat,buildings=n,training_targets=n_train,test_targets=n_test,partitions=actual_partitions,
                        processing_seconds=preparation,training_seconds=training,prediction_seconds=inference))
                    pd.DataFrame(records).to_csv(args.output/'measurements.csv',index=False)
                print(f'{kind} repeat {repeat} n={n}: preparation={preparation:.3f}s training={training:.3f}s prediction={inference:.3f}s',flush=True)
        frame=pd.DataFrame(records)
        summary=frame.groupby(['model','buildings'])[['processing_seconds','training_seconds','prediction_seconds']].agg(['median','min','max',lambda x:x.quantile(.75)-x.quantile(.25)])
        summary.columns=['_'.join(c).replace('<lambda_0>','iqr') for c in summary.columns]
        summary.reset_index().to_csv(args.output/'summary.csv',index=False)
        (args.output/'input_manifest.json').write_text(json.dumps(hashes,indent=2))
        save_runtime(args.output/'protocol.json',{},dict(repeats=3,schedule=schedule,models=args.models,
            rf_trees=50,rf_depth=8,lstm_epochs=1,batch_size=128,temperature=True,adaptive_partition_coalescing=False,
            threads=dict(tf_intra=os.environ.get('TF_NUM_INTRAOP_THREADS'),tf_inter=os.environ.get('TF_NUM_INTEROP_THREADS')),
            phase_notes='Preparation includes materialized feature/sequence generation and tensor/vector readiness; training uses fixed models; prediction materializes output sums on prepared test inputs. Warm OS/JVM runs, Spark startup excluded. No distributed speedup claim.'),spark)
    finally:spark.stop()


if __name__=='__main__':main()
