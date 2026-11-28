"""Build the verified full-study app snapshot without requiring earlier pilot runs."""
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import shutil
import pandas as pd

if __package__:
    from .final_dashboard_data import extend_final,LABELS
    from .summarize_forecasts import summarize
else:
    from final_dashboard_data import extend_final,LABELS
    from summarize_forecasts import summarize


def build_final(repo,output):
    repo=Path(repo);output=Path(output);root=repo/'outputs/final-study-v1'
    if not (root/'verification.json').is_file():raise ValueError('Verify the full study before publishing its dashboard')
    if output.exists() and not (output/'results.json').is_file():raise ValueError('Dashboard folder has no known snapshot')
    if output.exists():
        old=json.loads((output/'results.json').read_text())
        archive=output.with_name(output.name+'-pilot-snapshot')
        if not old.get('full_study') and not archive.exists():shutil.copytree(output,archive)
    output.mkdir(parents=True,exist_ok=True)
    audit_path=repo/'results/performance/time_alignment_audit.json'
    audit=json.loads(audit_path.read_text());paths=[audit_path]
    annual_path=repo/'data/processed/grid-import-2018'
    annual_frame=pd.read_parquet(annual_path,columns=['grid_import_without_solar_kwh','grid_import_with_solar_kwh','solar_generation_kwh','excess_solar_kwh'])
    totals=annual_frame.sum()
    annual=dict(buildings=573,hourly_records=len(annual_frame),demand_mwh=float(totals.grid_import_without_solar_kwh/1000),
        grid_import_mwh=float(totals.grid_import_with_solar_kwh/1000),solar_mwh=float(totals.solar_generation_kwh/1000),
        excess_solar_mwh=float(totals.excess_solar_kwh/1000),grid_reduction_pct=float(100*(1-totals.grid_import_with_solar_kwh/totals.grid_import_without_solar_kwh)))
    del annual_frame;paths+=sorted(annual_path.glob('*.parquet'))
    preparation=repo/'results/performance/aligned-hourly-2018-v1.json';prep=json.loads(preparation.read_text());paths.append(preparation)
    result=dict(generated_at_utc=datetime.now(timezone.utc).isoformat(),audit=audit,annual=annual,evidence=[],allocations=[],
        runtime=[dict(model='Full hourly preparation',buildings=573,processing_seconds=prep['phases_seconds']['processing_seconds'],
            training_seconds=None,prediction_seconds=None,environment='Spark local[2]')],
        runtime_note='Full observed run phases are separate from controlled repeated scaling. Shared LSTM sequence generation took 35.26 s before per-model normalization/data readiness. RF prediction includes test metrics; LSTM prediction also includes Parquet export. RF selected-fit time excludes its full validation grid. Do not treat these unlike totals as a controlled model-speed ranking.')
    sequence_record=repo/'results/performance/final-sequence-preparation-v1.json'
    sequence_preparation=json.loads(sequence_record.read_text());paths.append(sequence_record)
    result['runtime'].append(dict(model='Shared LSTM sequence generation',buildings=573,
        processing_seconds=sequence_preparation['phases_seconds']['processing_seconds'],
        training_seconds=None,prediction_seconds=None,environment='Spark local[2]'))
    try:
        saved=summarize(repo)
        for row in saved:row.update(model_label=LABELS.get(row['model'],row['model']),hourly_targets=row['test_samples'])
        result['evidence'].append(dict(id='saved',name='Original saved model results',scope='728,856 logged hourly targets per model',
            period='Original late-2018 experiments',note='Earlier aggregate logs; original keys and preprocessing provenance unverified. Not pooled with the new aligned full study.',rows=saved))
        paths.extend(repo/row['source'] for row in saved)
    except FileNotFoundError:pass
    ids=['128','1414','3575','4827','5957','10163','10812','13402','13950','14022']
    series={building:{} for building in ids};registry={}
    result['forecast_browser']=dict(buildings=ids,days=pd.date_range('2018-11-08','2018-12-31',freq='D').strftime('%Y-%m-%d').tolist(),models=[])
    extend_final(repo,result,series,registry,paths,output)
    for name,value in [('results.json',result),('forecast_series.json',series),('experiments.json',registry)]:
        (output/name).write_text(json.dumps(value,indent=2,allow_nan=False))
    pd.DataFrame(result['allocations']).to_csv(output/'allocation_comparison.csv',index=False)
    pd.DataFrame(result['runtime']).to_csv(output/'runtime.csv',index=False)
    (output/'source_manifest.json').write_text(json.dumps([dict(path=str(path.relative_to(repo)),sha256=hashlib.sha256(path.read_bytes()).hexdigest()) for path in sorted(set(paths))],indent=2))
    return result
