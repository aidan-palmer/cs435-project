"""Independently recalculate forecast errors and verify final experiment invariants."""
import hashlib
import json
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd


def main():
    repo=Path(__file__).resolve().parents[1];root=repo/'outputs/final-study-v1';evaluation=root/'evaluation'
    if not (root/'model_replay.json').is_file():
        subprocess.run([sys.executable,str(repo/'scripts/replay_final_models.py')],cwd=repo,check=True)
    replay=json.loads((root/'model_replay.json').read_text())
    if replay['status']!='passed' or len(replay['checks'])!=5:raise ValueError('Retained model replay is incomplete')
    recorded=pd.read_csv(evaluation/'forecast_accuracy.csv').set_index('model')
    errors=[]
    for model,folder in [('rf','rf-demand'),('rf-temperature','rf-temperature'),('rf-housing','rf-housing'),
                         ('lstm','lstm-demand'),('lstm-temperature','lstm-temperature')]:
        path=root/folder/('predictions.parquet' if model.startswith('lstm') else 'predictions')
        data=pd.read_parquet(path);error=data.actual_kwh.astype(float)-data.predicted_kwh.astype(float)
        mae=float(error.abs().mean());rmse=float(np.sqrt((error**2).mean()))
        if len(data)!=742608 or not np.isclose(mae,recorded.loc[model,'mae_kwh'],atol=2e-6) or not np.isclose(rmse,recorded.loc[model,'rmse_kwh'],atol=2e-6):
            raise ValueError('Prediction metric mismatch '+model)
        errors.append(dict(model=model,targets=len(data),recalculated_mae_kwh=mae,recalculated_rmse_kwh=rmse))
    cases=pd.read_csv(evaluation/'allocation_comparison.csv')
    if len(cases)!=720:raise ValueError('Expected all model/mode/scenario/budget combinations')
    if (cases.spent_usd>cases.budget_usd+1e-6).any():raise ValueError('Overspent a budget')
    for (mode,scenario,budget),group in cases.groupby(['mode','scenario','budget_usd']):
        if group.random_mean_kg.max()-group.random_mean_kg.min()>1e-6:raise ValueError('Unpaired random controls')
        if group.selected_homes.nunique()!=1:raise ValueError('Unequal installation counts at uniform costs')
    for (mode,model,budget),group in cases.groupby(['mode','model','budget_usd']):
        base=group[group.scenario=='base'].iloc[0]
        for scenario,scale in [('emissions-low',.8),('emissions-high',1.2)]:
            row=group[group.scenario==scenario].iloc[0]
            if not np.isclose(row.realized_kg,base.realized_kg*scale):raise ValueError('Emissions scaling mismatch')
    benchmarks=pd.read_csv(root/'scaling/measurements.csv')
    if len(benchmarks)!=24 or not benchmarks.groupby(['model','buildings']).size().eq(3).all():raise ValueError('Incomplete repeated scaling measurements')
    snapshot=root/'reproducibility';snapshot.mkdir(exist_ok=True)
    paths=[*sorted((repo/'scripts').glob('*.py')),*sorted((repo/'scripts').glob('*.html')),
           *sorted((repo/'tests').glob('*.py')),*sorted((repo/'docs').glob('*.md')),repo/'README.md',
           repo/'configs/final-study-v1.json',repo/'requirements-forecasting.txt',repo/'requirements-forecasting.lock.txt']
    manifest=[]
    for path in paths:
        target=snapshot/path.relative_to(repo);target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(path.read_bytes())
        manifest.append(dict(path=str(path.relative_to(repo)),sha256=hashlib.sha256(path.read_bytes()).hexdigest()))
    (snapshot/'code_manifest.json').write_text(json.dumps(manifest,indent=2))
    artifacts=[]
    dataset_paths=[*sorted((repo/'data/processed/aligned-hourly-2018-v1').glob('*.parquet')),
                   *sorted((repo/'data/processed/final-study-v1/sequences').rglob('*.parquet'))]
    for path in [*dataset_paths,*sorted(root.rglob('*'))]:
        if not path.is_file() or snapshot in path.parents or path in [root/'verification.json',root/'logs/verification.log']:continue
        artifacts.append(dict(path=str(path.relative_to(repo)),bytes=path.stat().st_size,
                              sha256=hashlib.sha256(path.read_bytes()).hexdigest()))
    (snapshot/'artifact_manifest.json').write_text(json.dumps(artifacts,indent=2))
    report=dict(status='passed',retained_model_replay=replay,forecast_metrics=errors,allocation_cases=len(cases),scaling_measurements=len(benchmarks),
        checks=['Recalculated held-out prediction errors','Exact matched keys checked by evaluator',
                'Budget caps and uniform-cost installation counts','Paired random controls','Constant emissions scaling',
                'Three scaling repeats at all four sizes','Frozen source snapshot and hashes retained'])
    (root/'verification.json').write_text(json.dumps(report,indent=2));print(json.dumps(report,indent=2))


if __name__=='__main__':main()
