"""Load the retained fitted models afresh and reproduce every held-out prediction."""
import json
from pathlib import Path
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession,functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressionModel
import tensorflow as tf

if __package__:
    from .housing_features import join_housing
    from .train_lstm import evaluate_and_save
else:
    from housing_features import join_housing
    from train_lstm import evaluate_and_save


def main():
    repo=Path(__file__).resolve().parents[1];root=repo/'outputs/final-study-v1';checks=[]
    spark=SparkSession.builder.appName('SolarOpt retained model replay').getOrCreate();spark.conf.set('spark.sql.session.timeZone','UTC')
    try:
        base=spark.read.parquet(str(root/'rf-temperature/features')).where("target_timestamp >= timestamp('2018-11-08') AND target_timestamp < timestamp('2019-01-01')")
        for name in ['rf-demand','rf-temperature','rf-housing']:
            record=json.loads((repo/'results/performance'/f'final-{name}-v1.json').read_text())
            features=base
            if name=='rf-housing':features,_,_=join_housing(features,repo.parent/'DATA_for_project/CO_baseline_metadata_and_annual_results.csv')
            features=VectorAssembler(inputCols=record['configuration']['features'],outputCol='features').transform(features)
            model=RandomForestRegressionModel.load(str(root/name/'model'))
            replay=model.transform(features).select('building_id','target_timestamp',F.col('prediction').alias('replay'))
            saved=spark.read.parquet(str(root/name/'predictions')).select('building_id','target_timestamp','predicted_kwh')
            match=replay.join(saved,['building_id','target_timestamp'])
            result=match.agg(F.count('*').alias('rows'),F.max(F.abs(F.col('replay')-F.col('predicted_kwh'))).alias('difference')).first()
            if result.rows!=742608 or result.difference>1e-10:raise ValueError('Retained RF replay differs '+name)
            checks.append(dict(model=name,targets=result.rows,max_absolute_prediction_difference_kwh=result.difference,status='passed'))
            print('Replayed '+name,flush=True)
    finally:spark.stop()
    sequence=repo/'data/processed/final-study-v1/sequences'
    for name,temperature in [('lstm-demand',False),('lstm-temperature',True)]:
        model=tf.keras.models.load_model(root/name/'model.keras',compile=False)
        stats=json.loads((root/name/'normalization.json').read_text())
        output=root/'replayed'/name
        if not (output/'predictions.parquet').is_file():
            output.mkdir(parents=True,exist_ok=True)
            evaluate_and_save(model,sequence/'split=test',stats,temperature,output,128)
        original=pd.read_parquet(root/name/'predictions.parquet')
        replay=pd.read_parquet(output/'predictions.parquet')
        matched=original.merge(replay,on=['building_id','target_timestamp'],validate='one_to_one',suffixes=('_original','_replay'))
        difference=float((matched.predicted_kwh_original-matched.predicted_kwh_replay).abs().max())
        if len(matched)!=742608 or not np.allclose(matched.predicted_kwh_original,matched.predicted_kwh_replay,rtol=1e-5,atol=1e-6):
            raise ValueError('Retained LSTM replay differs '+name)
        checks.append(dict(model=name,targets=len(matched),max_absolute_prediction_difference_kwh=difference,status='passed'))
        print('Replayed '+name,flush=True)
    (root/'model_replay.json').write_text(json.dumps(dict(status='passed',checks=checks),indent=2))


if __name__=='__main__':main()
