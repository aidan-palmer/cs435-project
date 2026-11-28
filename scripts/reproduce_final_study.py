"""Run/resume the frozen study with logged commands and explicit stage boundaries."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--stage',choices=['train','evaluate','scaling','verify','all'],default='all')
    parser.add_argument('--resume',action='store_true')
    args=parser.parse_args();repo=Path(__file__).resolve().parents[1]
    root=repo/'outputs/final-study-v1';logs=root/'logs';logs.mkdir(parents=True,exist_ok=True)
    environment=os.environ.copy();environment.update(SPARK_LOCAL_IP='127.0.0.1',PYTHONUNBUFFERED='1',
        TF_NUM_INTRAOP_THREADS='2',TF_NUM_INTEROP_THREADS='1',
        PYSPARK_SUBMIT_ARGS='--master local[2] --driver-memory 4g --conf spark.sql.shuffle.partitions=16 --conf spark.ui.showConsoleProgress=false pyspark-shell')
    def run(name,command,complete):
        if args.resume and complete.exists():print('Retaining completed '+name,flush=True);return
        print('Running '+name,flush=True)
        with (logs/(name+'.log')).open('w') as stream:
            subprocess.run([sys.executable,*command],cwd=repo,env=environment,stdout=stream,stderr=subprocess.STDOUT,check=True)
    if args.stage in ['all','train']:
        run('sequences',['scripts/prepare_lstm_data.py','data/processed/aligned-hourly-2018-v1','data/processed/final-study-v1/sequences',
            '--val-start','2018-09-15','--test-start','2018-11-08','--runtime-output','results/performance/final-sequence-preparation-v1.json'],
            repo/'results/performance/final-sequence-preparation-v1.json')
        for model,flags in [('rf-temperature',['--use-temperature']),('rf-demand',[]),
                            ('rf-housing',['--use-temperature','--use-housing','--metadata','../DATA_for_project/CO_baseline_metadata_and_annual_results.csv'])]:
            run(model,['scripts/train_random_forest.py','data/processed/aligned-hourly-2018-v1','outputs/final-study-v1/'+model,
                '--val-start','2018-09-15','--test-start','2018-11-08','--runtime-output','results/performance/final-'+model+'-v1.json',*flags,
                *(['--prepared-features','outputs/final-study-v1/rf-temperature/features'] if model!='rf-temperature' else [])],repo/'results/performance'/('final-'+model+'-v1.json'))
        for model,flags in [('lstm-demand',[]),('lstm-temperature',['--use-temperature'])]:
            run(model,['scripts/train_lstm.py','data/processed/final-study-v1/sequences','outputs/final-study-v1/'+model,
                '--runtime-output','results/performance/final-'+model+'-v1.json',*flags],root/model/'metrics.json')
    if args.stage in ['all','evaluate']:
        run('evaluation',['scripts/final_allocation.py'],root/'evaluation/protocol.json')
    if args.stage in ['all','scaling']:
        run('scaling',['scripts/benchmark_scaling.py'],root/'scaling/summary.csv')
    if args.stage in ['all','verify']:
        run('verification',['scripts/verify_final_study.py'],root/'verification.json')
    print('Requested stages completed',flush=True)


if __name__=='__main__':main()
