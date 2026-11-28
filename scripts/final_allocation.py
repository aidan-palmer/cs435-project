"""Matched full-cohort evaluation and an auditable one-time installation protocol.

Planning scores freeze UTC weekly profiles derived solely from validation forecasts
available before the decision. Daily test forecasts are evaluated separately.
"""
import argparse
import hashlib
import json
from pathlib import Path
import time

import numpy as np
import pandas as pd

if __package__:
    from .allocation_experiments import SCENARIOS, predictions, match_predictions, score, file_manifest
    from .allocate_solar import compare_allocations, dollars, money_cents, read_candidates, save_experiment, experiment_summary
else:
    from allocation_experiments import SCENARIOS, predictions, match_predictions, score, file_manifest
    from allocate_solar import compare_allocations, dollars, money_cents, read_candidates, save_experiment, experiment_summary

KEYS=['building_id','target_timestamp']
MODEL_FILES={'rf':'rf-demand','rf-temperature':'rf-temperature','rf-housing':'rf-housing',
             'lstm':'lstm-demand','lstm-temperature':'lstm-temperature'}


def freeze_profile(validation,targets,decision,days=28):
    """Return future scores from only the preceding complete weeks of forecasts."""
    if days<7 or days%7:raise ValueError('Use complete weeks')
    if validation.empty or validation.target_timestamp.max()>=decision:
        raise ValueError('Planning inputs contain post-decision target observations')
    if (validation.forecast_available_at>decision).any():raise ValueError('Planning forecast was unavailable at decision')
    lower=decision-pd.Timedelta(days=days)
    old=validation[validation.target_timestamp.ge(lower)].copy()
    if old.duplicated(KEYS).any():raise ValueError('Duplicate planning forecast keys')
    ids=set(targets.building_id)
    counts=old.groupby('building_id').size()
    if set(counts.index)!=ids or not counts.eq(days*24).all():raise ValueError('Need complete pre-decision forecast history for every building')
    old['week_hour']=old.target_timestamp.dt.dayofweek*24+old.target_timestamp.dt.hour
    profile=old.groupby(['building_id','week_hour']).predicted_kwh.mean().rename('predicted_kwh')
    future=targets.copy();future['week_hour']=future.target_timestamp.dt.dayofweek*24+future.target_timestamp.dt.hour
    future=future.join(profile,on=['building_id','week_hour'])
    if future.predicted_kwh.isna().any():raise ValueError('Planning profile missing target keys')
    return future


def write_cases(models,eligible,output,budgets,decision,mode,period):
    report=[]
    for model,frame in models.items():
        for scenario,cost,capacity,factor in SCENARIOS:
            folder=output/mode/model/scenario;folder.mkdir(parents=True)
            scored=score(frame,capacity,factor)
            scored['installation_cost_usd']=cost;scored['eligible']=scored.building_id.isin(eligible)
            path=folder/'allocation-candidates.csv';scored.to_csv(path,index=False)
            candidates=read_candidates(path);digest=hashlib.sha256(path.read_bytes()).hexdigest()
            assumptions=dict(forecast_run_id=f'final-study-v1:{model}:{mode}',pv_profile_id='shared-4kw-PVWatts-2018',
                pv_capacity_kw=str(capacity),emissions_source=f'Constant regional scenario: {factor} kg CO2/kWh; zero export credit',
                evaluation_start=period[0].date().isoformat(),evaluation_end=period[1].date().isoformat(),
                evaluation_start_utc=period[0].isoformat(),evaluation_end_utc_exclusive=period[1].isoformat(),
                decision_mode=mode,decision_time_utc=decision.isoformat() if mode=='prospective' else None,
                planning_reconstruction='Selected models reconstruct prior-period forecasts at the decision using only pre-cutoff training/validation information; historical issue labels do not claim live earlier deployment',
                planning_method='Frozen average predicted UTC hour-of-week from the preceding 28 validation days' if mode=='prospective' else 'Retrospective ranking using daily test forecasts',
                pv_scaling='Linear 4 kW hourly shape rescaling before overlap; PV-case installation cost fixed',
                installation_cost_usd=cost,emissions_factor_kg_per_kwh=factor,
                negative_prediction_policy='Raw values for accuracy; clip demand to zero for solar benefit')
            (folder/'allocation-assumptions.json').write_text(json.dumps(assumptions,indent=2))
            for budget in budgets:
                ranked,selections,results=compare_allocations(candidates,budget,seed=42,random_runs=100)
                summary=experiment_summary(candidates,results,budget,'benefit_per_dollar',42,assumptions,digest)
                save_experiment(folder/('budget-'+dollars(budget)),candidates,ranked,selections,results,summary)
                random=summary['random_baseline']['realized_avoided_co2_kg'];first=results[0]
                eligible_scores=scored[scored.eligible].sort_values('realized_avoided_co2_kg',ascending=False)
                oracle=float(eligible_scores.head(budget//money_cents(str(cost),'cost')).realized_avoided_co2_kg.sum())
                realized=float(first['realized_avoided_co2_kg'])
                report.append(dict(mode=mode,model=model,scenario=scenario,budget_usd=float(dollars(budget)),
                    cost_usd=cost,pv_kw=capacity,factor_kg_per_kwh=factor,
                    selected_homes=first['selected_count'],spent_usd=first['total_cost_usd'],unused_usd=first['unspent_budget_usd'],
                    predicted_kg=float(first['predicted_avoided_co2_kg']),realized_kg=realized,
                    realized_grid_reduction_kwh=realized/factor,random_mean_kg=float(random['mean']),
                    random_sd_kg=float(random['sample_standard_deviation']),
                    gain_pct=float(summary['comparison']['realized_avoided_co2_kg']['percent_gain_over_random_mean']),
                    benefit_kg_per_usd=realized/float(first['total_cost_usd']) if float(first['total_cost_usd']) else None,
                    hindsight_best_kg=oracle,hindsight_capture_pct=100*realized/oracle if oracle else None))
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo',type=Path,default=Path(__file__).resolve().parents[1])
    parser.add_argument('--output',type=Path,default=Path('outputs/final-study-v1/evaluation'))
    args=parser.parse_args();repo=args.repo.resolve();root=repo/'outputs/final-study-v1'
    if args.output.exists():parser.error('Choose a new evaluation output directory')
    config=json.loads((repo/'configs/final-study-v1.json').read_text())
    start,end=pd.Timestamp(config['test_start']),pd.Timestamp(config['test_end_exclusive'])
    data=pd.read_parquet(repo/'data/processed/resstock-pvwatts-2018',columns=['building_id','hour_start_utc','demand_kwh','solar_generation_kwh'])
    data=data.rename(columns={'hour_start_utc':'target_timestamp'})
    data.building_id=data.building_id.astype(str);data.target_timestamp=pd.to_datetime(data.target_timestamp,utc=True)
    if data.duplicated(KEYS).any() or data.isna().any().any():raise ValueError('Invalid source keys')
    test=data[data.target_timestamp.ge(start)&data.target_timestamp.lt(end)].copy()
    history=data[data.target_timestamp.ge(start-pd.Timedelta(days=28))&data.target_timestamp.lt(start)].copy()
    if test.building_id.nunique()!=573 or not test.groupby('building_id').size().eq(1296).all():raise ValueError('Unexpected full test coverage')
    metadata=repo.parent/'DATA_for_project/CO_baseline_metadata_and_annual_results.csv'
    meta=pd.read_csv(metadata,dtype=str);meta=meta[meta.upgrade.eq('0')&meta.bldg_id.isin(test.building_id)]
    if meta.bldg_id.duplicated().any() or set(meta.bldg_id)!=set(test.building_id):raise ValueError('Missing or duplicate baseline metadata')
    eligible=set(meta.loc[meta['in.geometry_building_type_acs'].eq('Single-Family Detached'),'bldg_id'])
    if len(eligible)!=382:raise ValueError('Unexpected eligible cohort')
    daily={};planning={};accuracy=[];breakdown=[]
    sources=file_manifest(repo/'data/processed/resstock-pvwatts-2018')+file_manifest(metadata)
    def evaluate(model,frame):
        error=frame.predicted_kwh-frame.demand_kwh
        accuracy.append(dict(model=model,mae_kwh=float(error.abs().mean()),rmse_kwh=float(np.sqrt((error**2).mean())),
                             hourly_targets=len(frame),buildings=frame.building_id.nunique(),negative_predictions=int((frame.predicted_kwh<0).sum())))
        for label,mask in [('all',pd.Series(True,index=frame.index)),('solar-hours',frame.solar_generation_kwh>0)]:
            subset=error[mask]
            breakdown.append(dict(model=model,segment=label,mae_kwh=float(subset.abs().mean()),rmse_kwh=float(np.sqrt((subset**2).mean())),targets=len(subset)))
        if 'horizon' in frame:
            for horizon,part in frame.groupby('horizon'):
                err=part.predicted_kwh-part.demand_kwh
                breakdown.append(dict(model=model,segment=f'horizon-{horizon}',mae_kwh=float(err.abs().mean()),rmse_kwh=float(np.sqrt((err**2).mean())),targets=len(part)))
    for model,folder in MODEL_FILES.items():
        lstm=model.startswith('lstm')
        for split,target in [('test',test),('validation',history)]:
            path=root/folder/(('predictions.parquet' if split=='test' else 'validation_predictions.parquet') if lstm else ('predictions' if split=='test' else 'validation_predictions'))
            frame=predictions(pd.read_parquet(path),'UTC')
            frame=frame[frame.target_timestamp.ge(target.target_timestamp.min())&frame.target_timestamp.le(target.target_timestamp.max())]
            checked=match_predictions(frame,target)
            sources+=file_manifest(path)
            if split=='test':daily[model]=checked;evaluate(model,checked)
            else:planning[model]=freeze_profile(checked,test,start)
    for model,hours in [('previous-day',24),('previous-week',168)]:
        past=data[['building_id','target_timestamp','demand_kwh']].copy();past.target_timestamp+=pd.Timedelta(hours=hours)
        past=past.rename(columns={'demand_kwh':'predicted_kwh'})
        for target,is_test in [(test,True),(history,False)]:
            frame=target.merge(past,on=KEYS,how='left',validate='one_to_one')
            if frame.predicted_kwh.isna().any():raise ValueError('Missing seasonal history')
            frame['forecast_available_at']=frame.target_timestamp.dt.floor('D')
            frame['horizon']=frame.target_timestamp.dt.hour+1
            if is_test:daily[model]=frame;evaluate(model,frame)
            else:planning[model]=freeze_profile(frame,test,start)
    actual_history=history.assign(predicted_kwh=history.demand_kwh,forecast_available_at=history.target_timestamp+pd.Timedelta(hours=1))
    planning['historical-mean']=freeze_profile(actual_history,test,start)
    args.output.mkdir(parents=True)
    started=time.perf_counter();budgets=[money_cents(str(value),'budget') for value in config['allocation_budgets_usd']]
    report=write_cases(planning,eligible,args.output,budgets,start,'prospective',(start,end))
    report+=write_cases(daily,eligible,args.output,budgets,start,'retrospective',(start,end))
    pd.DataFrame(report).to_csv(args.output/'allocation_comparison.csv',index=False)
    pd.DataFrame(accuracy).to_csv(args.output/'forecast_accuracy.csv',index=False)
    pd.DataFrame(breakdown).to_csv(args.output/'forecast_breakdown.csv',index=False)
    (args.output/'source_manifest.json').write_text(json.dumps(sources,indent=2))
    (args.output/'protocol.json').write_text(json.dumps(dict(config,matched_test_targets=len(test),matched_history_targets=len(history),
        allocation_cases=len(report),allocation_seconds=time.perf_counter()-started,
        verification='Exact common building/hour keys and source demand checked for every trained model and split; complete pre-decision history verified'),indent=2))
    print(pd.DataFrame(accuracy).to_string(index=False));print(f'Saved {len(report)} cases to {args.output}')


if __name__=='__main__':main()
