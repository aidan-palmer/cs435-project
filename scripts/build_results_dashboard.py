"""Publish saved study statistics and pilot forecast curves for the local dashboard."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

if __package__:
    from .summarize_forecasts import summarize
    from .allocation_experiments import predictions, match_predictions
else:
    from summarize_forecasts import summarize
    from allocation_experiments import predictions, match_predictions

NAMES={'rf':'RF demand only','rf-temperature':'RF with temperature','lstm':'LSTM demand only',
       'lstm-temperature':'LSTM with temperature','previous-day':'Previous day','previous-week':'Previous week',
       'frozen-historical-28d':'Frozen historical baseline'}
SCENARIOS={'base':'Base assumptions','cost-low':'Lower installation cost','cost-high':'Higher installation cost',
           'pv-2kw':'2 kW solar','pv-6kw':'6 kW solar','pv-8kw':'8 kW solar',
           'emissions-low':'Emissions factor −20%','emissions-high':'Emissions factor +20%'}


def build(repo,output):
    repo=Path(repo);output=Path(output)
    root=repo/'outputs/priority-experiments';performance=repo/'results/performance'
    pilot=root/'matched-pilot-v1';historical=root/'historical-sensitivity-v1'
    paths=[]
    def read_csv(path):
        paths.append(path);return pd.read_csv(path)
    accuracy=read_csv(pilot/'matched_forecast_accuracy.csv')
    audit_path=performance/'time_alignment_audit.json';paths.append(audit_path)
    audit=json.loads(audit_path.read_text())
    evidence=[dict(id='pilot',name='Matched 10-home pilot',scope='10 simulated detached homes; 12,960 matched hourly targets per model',
                   period='8 November 2018 00:00 UTC to 1 January 2019 00:00 UTC (end exclusive)',
                   note='Same building/hour keys and source actuals verified. Deterministic pilot; not a representative sample.',
                   rows=accuracy.to_dict('records')),
              dict(id='saved',name='Original saved model results',scope='728,856 logged hourly targets per model',
                   period='Original late-2018 experiments; RF logs identify 8 November–31 December',
                   note='Aggregate logs only. Original paired keys and preprocessing provenance remain unverified. These results are separate from the aligned pilot.',
                   rows=summarize(repo))]
    paths.extend(repo/row['source'] for row in evidence[1]['rows'])
    paths.extend([Path(__file__).resolve(),repo/'scripts/summarize_forecasts.py'])
    for group in evidence:
        for row in group['rows']:
            row['model_label']=NAMES.get(row['model'],row['model'])
            row['hourly_targets']=int(row.get('hourly_targets',row.get('test_samples',0)))
    catalogue=[];registry={}
    for cohort,folder in [('pilot',pilot),('historical',historical)]:
        frame=read_csv(folder/'allocation_comparison.csv')
        for row in frame.to_dict('records'):
            model=row['model'];scenario=row['scenario'];budget=f"{row['budget_usd']:.2f}"
            identifier=f'{cohort}--{model}--{scenario}--{budget}'
            directory=folder/model/scenario/('budget-'+budget)
            if not (directory/'summary.json').is_file():raise ValueError('Missing saved allocation '+str(directory))
            paths.extend([directory/'summary.json',directory/'strategy_results.csv',directory/'selected_households.csv'])
            registry[identifier]=str(directory.relative_to(repo))
            catalogue.append(dict(row,id=identifier,cohort=cohort,model_label=NAMES[model],scenario_label=SCENARIOS[scenario]))
    runtime=[]
    for model,filename in [('Full hourly preparation','aligned-hourly-2018-v1.json'),
                          ('RF demand only','rf-pilot-v1.json'),('RF with temperature','rf-pilot-temperature-v1.json'),
                          ('LSTM demand only','lstm-pilot-demand-v1.json'),('LSTM with temperature','lstm-pilot-temperature-v1.json')]:
        path=performance/filename;paths.append(path);record=json.loads(path.read_text());phases=record['phases_seconds']
        runtime.append(dict(model=model,buildings=573 if model.startswith('Full') else 10,
            processing_seconds=phases.get('processing_seconds'),
            training_seconds=phases.get('selected_model_training_seconds',phases.get('training_seconds')),
            prediction_seconds=phases.get('prediction_and_test_metrics_seconds',phases.get('prediction_metrics_and_export_seconds')),
            environment='Spark local[2]' if model.startswith(('RF','Full')) else 'TensorFlow CPU; 4 intra-op / 2 inter-op threads'))
    joined_path=repo/'data/processed/priority-pilot-v1/joined.parquet';paths.append(joined_path)
    source=pd.read_parquet(joined_path,columns=['building_id','hour_start_utc','demand_kwh','solar_generation_kwh'])
    source=source.rename(columns={'hour_start_utc':'target_timestamp'})
    source.building_id=source.building_id.astype(str);source.target_timestamp=pd.to_datetime(source.target_timestamp,utc=True)
    lower,upper=pd.Timestamp('2018-11-08T00:00Z'),pd.Timestamp('2019-01-01T00:00Z')
    target=source[source.target_timestamp.ge(lower)&source.target_timestamp.lt(upper)].copy()
    prediction_paths={'rf':root/'rf-pilot-v1/predictions','rf-temperature':root/'rf-pilot-temperature-v1/predictions',
                      'lstm':root/'lstm-pilot-demand-v1/predictions.parquet',
                      'lstm-temperature':root/'lstm-pilot-temperature-v1/predictions.parquet'}
    curves=target.sort_values(['building_id','target_timestamp']).reset_index(drop=True)
    for model,path in prediction_paths.items():
        paths.extend(sorted(path.glob('*.parquet')) if path.is_dir() else [path])
        pred=predictions(pd.read_parquet(path),'UTC')
        pred=pred[pred.target_timestamp.ge(lower)&pred.target_timestamp.lt(upper)]
        checked=match_predictions(pred,target).sort_values(['building_id','target_timestamp'])
        curves[model]=checked.predicted_kwh.to_numpy()
    for model,hours in [('previous-day',24),('previous-week',168)]:
        prior=source[['building_id','target_timestamp','demand_kwh']].copy()
        prior.target_timestamp+=pd.Timedelta(hours=hours)
        prior=prior.rename(columns={'demand_kwh':model})
        curves=curves.merge(prior,on=['building_id','target_timestamp'],how='left',validate='one_to_one')
        if curves[model].isna().any():raise ValueError('Missing baseline source hours')
    curve_index={}
    curves['day']=curves.target_timestamp.dt.strftime('%Y-%m-%d')
    for (building,day),frame in curves.groupby(['building_id','day'],sort=True):
        curve_index.setdefault(building,{})[day]=dict(
            timestamps=frame.target_timestamp.dt.strftime('%Y-%m-%dT%H:%M:%SZ').tolist(),
            actual=frame.demand_kwh.tolist(),solar=frame.solar_generation_kwh.tolist(),
            models={model:frame[model].tolist() for model in NAMES if model in frame})
    remaining=[
        dict(priority=1,title='Run the full matched model experiment',detail='Extend the aligned ten-home RF/LSTM comparison and allocation to the complete cohort. Freeze common targets, eligible homes, budgets, and configurations.'),
        dict(priority=2,title='Define when installation choices are made',detail='Daily-forecast portfolio rankings are retrospective. A one-time placement claim needs every ranking input available at its decision cutoff.'),
        dict(priority=3,title='Measure scaling with repeated runs',detail='Use nested 50, 150, 300, and 573-building cases with fixed settings and comparable timed phases. Report spread, resources, and local versus cluster execution.'),
        dict(priority=4,title='Finish the promised feature inventory',detail='The proposal includes housing characteristics. Test a documented housing-feature ablation or disclose that the trained models use demand, calendar, and temperature features without those attributes.'),
        dict(priority=5,title='Freeze sources and reproduce the final outputs',detail='Document actual PVWatts and constant-emissions choices, Larimer County geography, and departures from planned data sources. Reproduce the final tables and figures from a clean environment.')]
    annual_path=repo/'data/processed/grid-import-2018'
    annual_frame=pd.read_parquet(annual_path,columns=['grid_import_without_solar_kwh','grid_import_with_solar_kwh','solar_generation_kwh','excess_solar_kwh'])
    if len(annual_frame)!=audit['rows']:raise ValueError('Annual simulation row count differs from alignment audit')
    totals=annual_frame.sum()
    annual=dict(buildings=audit['buildings'],hourly_records=len(annual_frame),
        demand_mwh=float(totals.grid_import_without_solar_kwh/1000),grid_import_mwh=float(totals.grid_import_with_solar_kwh/1000),
        solar_mwh=float(totals.solar_generation_kwh/1000),excess_solar_mwh=float(totals.excess_solar_kwh/1000),
        grid_reduction_pct=float(100*(1-totals.grid_import_with_solar_kwh/totals.grid_import_without_solar_kwh)))
    paths.extend(sorted(annual_path.glob('*.parquet')))
    result=dict(generated_at_utc=datetime.now(timezone.utc).isoformat(),audit=audit,annual=annual,evidence=evidence,
        allocations=catalogue,runtime=runtime,remaining=remaining,
        forecast_browser=dict(buildings=sorted(curve_index,key=int),days=sorted(curves.day.unique()),
                              models=[dict(id=k,label=v) for k,v in NAMES.items() if k in curves]),
        runtime_note='Single observed local runs, not a controlled speed or scaling comparison. RF prediction includes test metrics; LSTM prediction also includes Parquet export. RF training shows only the selected configuration; LSTM includes validation over its executed epochs.')
    output.mkdir(parents=True,exist_ok=True)
    if __package__:
        from .final_dashboard_data import extend_final
    else:
        from final_dashboard_data import extend_final
    extend_final(repo,result,curve_index,registry,paths,output)
    for filename,payload in [('results.json',result),('forecast_series.json',curve_index),('experiments.json',registry)]:
        (output/filename).write_text(json.dumps(payload,indent=2,allow_nan=False))
    pd.DataFrame(result['evidence'][0]['rows']).to_csv(output/'forecast_accuracy.csv',index=False)
    pd.DataFrame(catalogue).to_csv(output/'allocation_comparison.csv',index=False)
    pd.DataFrame(runtime).to_csv(output/'runtime.csv',index=False)
    (output/'source_manifest.json').write_text(json.dumps([dict(path=str(p.relative_to(repo)),sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in sorted(set(paths))],indent=2))
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo',type=Path,default=Path(__file__).resolve().parents[1])
    parser.add_argument('--output',type=Path,default=Path('data/processed/dashboard-results'))
    args=parser.parse_args()
    if (args.repo/'outputs/final-study-v1/verification.json').is_file():
        if __package__:
            from .build_final_dashboard import build_final
        else:
            from build_final_dashboard import build_final
        result=build_final(args.repo.resolve(),args.output)
    else:result=build(args.repo.resolve(),args.output)
    print(f"Built dashboard results: {len(result['allocations'])} saved cases, matched models, and example hourly curves")


if __name__=='__main__':main()
