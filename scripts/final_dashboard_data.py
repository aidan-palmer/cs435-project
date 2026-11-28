"""Attach completed full-study evidence to the dashboard without mixing snapshots."""
import json
from pathlib import Path
import pandas as pd

LABELS={'rf':'RF demand only','rf-temperature':'RF with temperature','rf-housing':'RF with temperature + housing',
        'lstm':'LSTM demand only','lstm-temperature':'LSTM with temperature','previous-day':'Previous day',
        'previous-week':'Previous week','historical-mean':'Frozen historical demand mean'}


def extend_final(repo,result,series,registry,paths,output):
    root=repo/'outputs/final-study-v1';evaluation=root/'evaluation'
    if not (root/'verification.json').is_file():return
    accuracy=pd.read_csv(evaluation/'forecast_accuracy.csv')
    records=accuracy.assign(model_label=accuracy.model.map(LABELS)).to_dict('records')
    result['evidence'].insert(0,dict(id='full',name='Full aligned cohort',scope='573 simulated homes; 742,608 matched test targets per model',
        period='8 November 2018 00:00 UTC to 1 January 2019 00:00 UTC (end exclusive)',
        note='Full-cohort daily forecasts. Housing variant adds static floor area and heating fuel to temperature RF. One-time installation portfolios below use pre-decision profiles instead of later daily forecasts.',rows=records))
    cases=pd.read_csv(evaluation/'allocation_comparison.csv')
    for row in cases.to_dict('records'):
        mode,model,scenario=row['mode'],row['model'],row['scenario'];budget=f"{row['budget_usd']:.2f}"
        cohort='full-'+mode;identifier=f'{cohort}--{model}--{scenario}--{budget}'
        path=evaluation/mode/model/scenario/('budget-'+budget)
        registry[identifier]=str(path.relative_to(repo))
        result['allocations'].append(dict(row,id=identifier,cohort=cohort,model_label=LABELS[model],
            scenario_label={'base':'Base assumptions','cost-low':'Lower installation cost','cost-high':'Higher installation cost',
                'pv-2kw':'2 kW solar','pv-6kw':'6 kW solar','pv-8kw':'8 kW solar','emissions-low':'Emissions factor −20%','emissions-high':'Emissions factor +20%'}[scenario]))
    result['cohorts']=[dict(id='full-prospective',label='Full cohort · one-time decision',
        scope='382 eligible detached profiles · 8 November–31 December 2018 UTC · placement fixed at 8 November 00:00 UTC using only preceding validation forecasts.',
        note='Selected models reconstruct prior validation forecasts at the cutoff using pre-cutoff information, then freeze weekly profiles. This is a planning proxy; historical labels do not claim live earlier deployment. Processing and construction delays are idealized away.'),
        dict(id='full-retrospective',label='Full cohort · retrospective comparison',
        scope='382 eligible detached profiles · 8 November–31 December 2018 UTC · retrospective rankings from daily test forecasts.',
        note='Later daily forecasts are unavailable at an earlier installation cutoff; this comparison is a retrospective diagnostic, not a prospective installation policy.'),
        dict(id='pilot',label='Earlier ten-home pilot',scope='10 detached profiles · 8 November–31 December 2018 UTC · retrospective daily-forecast ranking.',note='Earlier integration pilot; separate cohort and runs.'),
        dict(id='historical',label='Earlier Q4 historical baseline',scope='382 eligible profiles · October–December 2018 · frozen historical demand mean.',note='Earlier Q4 sensitivity experiment; period differs from the full matched study.')]
    result['cohorts']=[item for item in result['cohorts'] if any(row['cohort']==item['id'] for row in result['allocations'])]
    for label,folder in [('RF demand only','rf-demand'),('RF with temperature','rf-temperature'),('RF with housing','rf-housing'),
                         ('LSTM demand only','lstm-demand'),('LSTM with temperature','lstm-temperature')]:
        path=repo/'results/performance'/f'final-{folder}-v1.json';record=json.loads(path.read_text());phases=record['phases_seconds'];paths.append(path)
        result['runtime'].append(dict(model=label+' · full cohort',buildings=573,processing_seconds=phases.get('processing_seconds'),
            training_seconds=phases.get('selected_model_training_seconds',phases.get('training_seconds')),
            prediction_seconds=phases.get('prediction_and_test_metrics_seconds',phases.get('prediction_metrics_and_export_seconds')),
            environment='Spark local[2]' if folder.startswith('rf') else 'TensorFlow CPU; 2 intra-op / 1 inter-op threads'))
    summary=pd.read_csv(root/'scaling/summary.csv')
    result['scaling']=summary.to_dict('records')
    result['scaling_note']='Three repeats at each nested cohort size. Fixed 31-day training, seven validation and seven test days. RF: 50 trees, depth 8; LSTM: one epoch. Medians and IQR describe local warm-JVM/OS measurements, not cluster speedup or tuned-model accuracy.'
    sample_ids=list(series)
    data=pd.read_parquet(repo/'data/processed/resstock-pvwatts-2018',columns=['building_id','hour_start_utc','demand_kwh','solar_generation_kwh'])
    data.building_id=data.building_id.astype(str);data=data[data.building_id.isin(sample_ids)].copy();data.hour_start_utc=pd.to_datetime(data.hour_start_utc,utc=True)
    target=data[data.hour_start_utc.ge(pd.Timestamp('2018-11-08T00:00Z'))&data.hour_start_utc.lt(pd.Timestamp('2019-01-01T00:00Z'))].copy()
    target=target.sort_values(['building_id','hour_start_utc']).reset_index(drop=True)
    for model,folder in [('rf','rf-demand'),('rf-temperature','rf-temperature'),('rf-housing','rf-housing'),('lstm','lstm-demand'),('lstm-temperature','lstm-temperature')]:
        path=root/folder/('predictions.parquet' if model.startswith('lstm') else 'predictions')
        pred=pd.read_parquet(path);pred.building_id=pred.building_id.astype(str);pred.target_timestamp=pd.to_datetime(pred.target_timestamp,utc=True)
        pred=pred[pred.building_id.isin(sample_ids)].sort_values(['building_id','target_timestamp'])
        if len(pred)!=len(target) or not (pred.building_id.to_numpy()==target.building_id.to_numpy()).all() or not (pred.target_timestamp.to_numpy()==target.hour_start_utc.to_numpy()).all():raise ValueError('Full study example curve keys differ')
        target[model]=pred.predicted_kwh.to_numpy()
    for model,hours in [('previous-day',24),('previous-week',168)]:
        previous=data[['building_id','hour_start_utc','demand_kwh']].copy();previous.hour_start_utc+=pd.Timedelta(hours=hours)
        target=target.merge(previous.rename(columns={'demand_kwh':model}),on=['building_id','hour_start_utc'],how='left',validate='one_to_one')
    series.clear();target['day']=target.hour_start_utc.dt.strftime('%Y-%m-%d')
    for (building,day),frame in target.groupby(['building_id','day'],sort=True):
        series.setdefault(building,{})[day]=dict(timestamps=frame.hour_start_utc.dt.strftime('%Y-%m-%dT%H:%M:%SZ').tolist(),
            actual=frame.demand_kwh.tolist(),solar=frame.solar_generation_kwh.tolist(),
            models={model:frame[model].tolist() for model in LABELS if model in frame})
    result['forecast_browser']['models']=[dict(id=k,label=v) for k,v in LABELS.items() if k in target]
    result['forecast_browser']['scope']='Ten example households from full-cohort test forecasts · 4 kW solar · UTC hour starts'
    result['remaining']=[dict(priority=1,title='Final study completed',detail='Full matched model evaluation, housing-feature comparison, one-time installation protocol, 720 allocation cases, and repeated local scaling are documented.'),
        dict(priority=2,title='Interpret the limits',detail='Simulated Larimer profiles are not actual property recommendations. Weekly planning profiles, fixed-shape PV scaling, constant emissions, and local CPU execution remain explicit assumptions.'),
        dict(priority=3,title='Optional future extensions',detail='Cluster benchmarking, repeated model seeds, physical PV capacity reruns, hourly emissions, and additional housing context in LSTM are extensions beyond the completed study.')]
    accuracy.to_csv(output/'forecast_accuracy.csv',index=False)
    summary.to_csv(output/'scaling.csv',index=False)
    result['full_study']=json.loads((evaluation/'protocol.json').read_text())
    paths.extend([evaluation/'forecast_accuracy.csv',evaluation/'allocation_comparison.csv',root/'scaling/summary.csv',root/'verification.json'])
