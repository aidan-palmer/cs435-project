"""Compare model-driven solar allocations or run labeled historical sensitivities.

All models must cover exactly the same building/hour keys and match source actuals.
Retrospective ranking uses later forecast issues and is not a prospective install policy.
"""
import argparse
import csv
import hashlib
import json
import re
from pathlib import Path
import time

import numpy as np
import pandas as pd

if __package__:
    from .allocate_solar import compare_allocations, dollars, experiment_summary, money_cents, read_candidates, save_experiment
    from .runtime_evidence import save_runtime
else:
    from allocate_solar import compare_allocations, dollars, experiment_summary, money_cents, read_candidates, save_experiment
    from runtime_evidence import save_runtime

KEYS=['building_id','target_timestamp']
SCENARIOS=[('base',10800,4,.6997),('cost-low',8640,4,.6997),('cost-high',12960,4,.6997),
           ('pv-2kw',10800,2,.6997),('pv-6kw',10800,6,.6997),('pv-8kw',10800,8,.6997),
           ('emissions-low',10800,4,.55976),('emissions-high',10800,4,.83964)]


def utc(value):
    stamp=pd.Timestamp(value)
    if stamp.tzinfo is None:raise ValueError('Evaluation/decision times require explicit UTC offset')
    return stamp.tz_convert('UTC')


def predictions(frame, clock):
    required=set(KEYS+['forecast_issued_at','horizon','actual_kwh','predicted_kwh'])
    if required-set(frame):raise ValueError('Missing prediction columns: '+str(required-set(frame)))
    frame=frame.copy()
    if frame[list(required)].isna().any().any():raise ValueError('Null prediction values')
    frame['building_id']=frame.building_id.astype(str)
    for col in ['forecast_issued_at','target_timestamp']:
        frame[col]=pd.to_datetime(frame[col],utc=True)
        if clock=='resstock-est-label':
            frame[col]+=pd.Timedelta(hours=5)
            frame.loc[frame[col]<pd.Timestamp('2018-01-01T07:00Z'),col]+=pd.Timedelta(days=365)
        if (frame[col]!=frame[col].dt.floor('h')).any():raise ValueError('Non-hourly prediction times')
    values=frame[['actual_kwh','predicted_kwh','horizon']].to_numpy(dtype=float)
    if not np.isfinite(values).all() or (frame.actual_kwh<0).any():raise ValueError('Invalid prediction energy/horizon')
    if not frame.horizon.isin(range(1,25)).all():raise ValueError('Horizon must be integer 1–24')
    if (frame.target_timestamp != frame.forecast_issued_at+pd.to_timedelta(frame.horizon,unit='h')).any():
        raise ValueError('Target time does not match issue-label plus horizon')
    if frame.duplicated(KEYS).any():raise ValueError('Duplicate building/target predictions')
    counts=frame.groupby(['building_id','forecast_issued_at']).horizon.agg(['count','nunique'])
    if (counts!=24).any().any():raise ValueError('Each exported forecast must contain all 24 horizons')
    # issued_at is the START label of the last observed hourly interval.
    frame['forecast_available_at']=frame.forecast_issued_at+pd.Timedelta(hours=1)
    return frame


def match_predictions(pred, targets, *, decision_time=None):
    if pred.duplicated(KEYS).any() or targets.duplicated(KEYS).any():raise ValueError('Duplicate target keys')
    merged=targets.merge(pred,on=KEYS,how='outer',validate='one_to_one',indicator=True)
    if not merged._merge.eq('both').all():raise ValueError('Prediction/source target keys differ; no silent dropping is allowed')
    if not np.allclose(merged.actual_kwh,merged.demand_kwh,rtol=1e-5,atol=1e-5):
        raise ValueError('Prediction actuals disagree with aligned demand; check source aggregation and clock')
    if decision_time is not None and decision_time>merged.target_timestamp.min():
        raise ValueError('Installation decision must be at or before evaluation targets')
    if decision_time is not None and (merged.forecast_available_at>decision_time).any():
        raise ValueError('Forecast unavailable at installation decision time')
    return merged.drop(columns='_merge')


def score(frame, capacity=4, factor=.6997):
    if not np.isfinite([capacity,factor]).all() or capacity<=0 or factor<0:raise ValueError('Invalid scenario assumption')
    solar=frame.solar_generation_kwh.to_numpy()*capacity/4
    result=frame[KEYS].copy()
    result['predicted_grid_reduction_kwh']=np.minimum(np.maximum(frame.predicted_kwh,0),solar)
    result['realized_grid_reduction_kwh']=np.minimum(frame.demand_kwh,solar)
    result=result.groupby('building_id').agg(predicted_grid_reduction_kwh=('predicted_grid_reduction_kwh','sum'),
        realized_grid_reduction_kwh=('realized_grid_reduction_kwh','sum'),scored_hours=('target_timestamp','size')).reset_index()
    result['predicted_avoided_co2_kg']=result.predicted_grid_reduction_kwh*factor
    result['realized_avoided_co2_kg']=result.realized_grid_reduction_kwh*factor
    return result


def file_manifest(path):
    path=Path(path)
    files=sorted(path.rglob('*.parquet')) if path.is_dir() else [path]
    return [dict(path=str(p),bytes=p.stat().st_size,sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in files]


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('joined',type=Path);parser.add_argument('output',type=Path)
    group=parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--predictions',nargs='+',metavar='RUN=PARQUET')
    group.add_argument('--historical-baseline',action='store_true')
    parser.add_argument('--prediction-clock',choices=['UTC','resstock-est-label'])
    parser.add_argument('--evaluation-start',required=True);parser.add_argument('--evaluation-end',required=True)
    parser.add_argument('--metadata',type=Path,required=True)
    parser.add_argument('--budgets-usd',nargs='+',default=['54000'])
    parser.add_argument('--sensitivity',action='store_true')
    parser.add_argument('--decision-time-utc',help='Require every prediction to be available by this instant')
    parser.add_argument('--retrospective-ranking',action='store_true',help='Explicitly label later-issue ranking as retrospective')
    parser.add_argument('--random-runs',type=int,default=100);parser.add_argument('--seed',type=int,default=42)
    args=parser.parse_args()
    if args.output.exists():parser.error('Choose a new output directory')
    if args.predictions and (not args.prediction_clock or bool(args.decision_time_utc)==args.retrospective_ranking):
        parser.error('Model runs require --prediction-clock and exactly one decision-time or retrospective-ranking option')
    if args.random_runs<1:parser.error('Random runs must be positive')
    start,end=utc(args.evaluation_start),utc(args.evaluation_end)
    if start>=end or start!=start.floor('h') or end!=end.floor('h'):parser.error('Use increasing whole-hour boundaries')
    if args.decision_time_utc and utc(args.decision_time_utc)>start:
        parser.error('Installation decision must be at or before evaluation start')
    budgets=[money_cents(v,'Budget',positive=True) for v in args.budgets_usd]
    if len(set(budgets))!=len(budgets):parser.error('Budgets must be distinct')
    began=time.perf_counter()
    data=pd.read_parquet(args.joined,columns=['building_id','hour_start_utc','demand_kwh','solar_generation_kwh'])
    data=data.rename(columns={'hour_start_utc':'target_timestamp'})
    data['building_id']=data.building_id.astype(str);data.target_timestamp=pd.to_datetime(data.target_timestamp,utc=True)
    if data.isna().any().any() or data.duplicated(KEYS).any():raise ValueError('Null or duplicate source keys')
    if (data.target_timestamp!=data.target_timestamp.dt.floor('h')).any():raise ValueError('Non-hourly source keys')
    energy=data[['demand_kwh','solar_generation_kwh']].to_numpy()
    if not np.isfinite(energy).all() or (energy<0).any():raise ValueError('Invalid source energy')
    targets=data[data.target_timestamp.ge(start)&data.target_timestamp.lt(end)].copy()
    expected=int((end-start).total_seconds()/3600)
    counts=targets.groupby('building_id').size()
    if targets.empty or len(counts)!=data.building_id.nunique() or not counts.eq(expected).all():
        raise ValueError('Every source building must cover the full evaluation window')
    meta=pd.read_csv(args.metadata,dtype=str)
    meta=meta[meta.upgrade.eq('0')&meta.bldg_id.isin(counts.index)]
    if meta.bldg_id.duplicated().any() or set(meta.bldg_id)!=set(counts.index):raise ValueError('Missing/duplicate baseline metadata')
    eligible=set(meta.loc[meta['in.geometry_building_type_acs'].eq('Single-Family Detached'),'bldg_id'])
    if not eligible:raise ValueError('No eligible detached homes')
    models={};sources=file_manifest(args.joined)+file_manifest(args.metadata)
    if args.historical_baseline:
        history=data[data.target_timestamp.ge(start-pd.Timedelta(days=28))&data.target_timestamp.lt(start)].copy()
        if not history.groupby('building_id').size().reindex(counts.index,fill_value=0).eq(672).all():raise ValueError('Need complete 28-day history')
        history['week_hour']=history.target_timestamp.dt.dayofweek*24+history.target_timestamp.dt.hour
        profile=history.groupby(['building_id','week_hour']).demand_kwh.mean().rename('predicted_kwh')
        frame=targets.copy();frame['week_hour']=frame.target_timestamp.dt.dayofweek*24+frame.target_timestamp.dt.hour
        models['frozen-historical-28d']=frame.join(profile,on=['building_id','week_hour'])
    else:
        for entry in args.predictions:
            label,path=entry.split('=',1)
            if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]*',label) or label in models or label in ['previous-day','previous-week']:raise ValueError('Use unique simple run labels')
            pred=predictions(pd.read_parquet(path),args.prediction_clock)
            pred=pred[pred.target_timestamp.ge(start)&pred.target_timestamp.lt(end)]
            models[label]=match_predictions(pred,targets,decision_time=utc(args.decision_time_utc) if args.decision_time_utc else None)
            sources+=file_manifest(path)
        for label,hours in [('previous-day',24),('previous-week',168)]:
            past=data[['building_id','target_timestamp','demand_kwh']].copy()
            past.target_timestamp+=pd.Timedelta(hours=hours)
            past=past.rename(columns={'demand_kwh':'predicted_kwh'})
            frame=targets.merge(past,on=KEYS,how='left',validate='one_to_one')
            if frame.predicted_kwh.isna().any():raise ValueError('Missing seasonal-baseline history')
            models[label]=frame
    args.output.mkdir(parents=True)
    report=[];accuracy=[]
    for model,frame in models.items():
        error=frame.predicted_kwh-frame.demand_kwh
        accuracy.append(dict(model=model,mae_kwh=float(error.abs().mean()),rmse_kwh=float(np.sqrt((error**2).mean())),
                             hourly_targets=len(frame),negative_predictions=int((frame.predicted_kwh<0).sum())))
        for name,cost,capacity,factor in SCENARIOS if args.sensitivity else SCENARIOS[:1]:
            out=args.output/model/name;out.mkdir(parents=True)
            scores=score(frame,capacity,factor)
            scores['installation_cost_usd']=cost;scores['eligible']=scores.building_id.isin(eligible)
            path=out/'allocation-candidates.csv';scores.to_csv(path,index=False)
            assumptions=dict(forecast_run_id=model,pv_profile_id=str(args.joined),pv_capacity_kw=str(capacity),
                emissions_source=f'Illustrative constant-factor sensitivity: {factor} kg CO2/kWh; zero export credit',
                evaluation_start=start.date().isoformat(),evaluation_end=end.date().isoformat(),
                evaluation_start_utc=start.isoformat(),evaluation_end_utc_exclusive=end.isoformat(),
                decision_mode='frozen pre-evaluation profile' if args.historical_baseline else ('retrospective ranking; not prospective placement' if args.retrospective_ranking else 'fixed decision time'),
                decision_time_utc=args.decision_time_utc,installation_cost_usd=cost,
                emissions_factor_kg_per_kwh=factor,pv_scaling='Linear scaling of fixed 4 kW AC shape; clipping/inverter effects not recomputed; cost held fixed in PV scenarios',
                negative_prediction_policy='clip predicted demand to zero for physical solar benefit only; accuracy uses raw predictions')
            (out/'allocation-assumptions.json').write_text(json.dumps(assumptions,indent=2))
            candidates=read_candidates(path);digest=hashlib.sha256(path.read_bytes()).hexdigest()
            for budget in budgets:
                ranked,selections,results=compare_allocations(candidates,budget,seed=args.seed,random_runs=args.random_runs)
                summary=experiment_summary(candidates,results,budget,'benefit_per_dollar',args.seed,assumptions,digest)
                folder=out/('budget-'+dollars(budget));save_experiment(folder,candidates,ranked,selections,results,summary)
                base=summary['random_baseline']['realized_avoided_co2_kg']
                report.append(dict(model=model,scenario=name,budget_usd=dollars(budget),cost_usd=cost,pv_kw=capacity,
                    factor_kg_per_kwh=factor,selected_homes=results[0]['selected_count'],
                    realized_kg=results[0]['realized_avoided_co2_kg'],random_mean_kg=base['mean'],
                    gain_pct=summary['comparison']['realized_avoided_co2_kg']['percent_gain_over_random_mean']))
    pd.DataFrame(report).to_csv(args.output/'allocation_comparison.csv',index=False)
    pd.DataFrame(accuracy).to_csv(args.output/'matched_forecast_accuracy.csv',index=False)
    (args.output/'source_manifest.json').write_text(json.dumps(sources,indent=2))
    save_runtime(args.output/'runtime.json',dict(preparation_scoring_allocation_export_seconds=time.perf_counter()-began),
                 dict(vars(args),joined=str(args.joined),output=str(args.output),metadata=str(args.metadata),
                      rows=len(targets),buildings=len(counts),eligible_buildings=len(eligible)))
    print(f'Saved {len(report)} allocation scenarios to {args.output}')


if __name__=='__main__':main()
