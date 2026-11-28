from pathlib import Path
import json
import argparse
import numpy as np
import pandas as pd
parser=argparse.ArgumentParser(description='Verify shared UTC preparation against saved simulation demand')
parser.add_argument('--repo',type=Path,default=Path(__file__).resolve().parents[1])
parser.add_argument('--aligned',type=Path,required=True)
parser.add_argument('--joined',type=Path,required=True)
parser.add_argument('--raw-example',type=Path,required=True)
parser.add_argument('--output',type=Path,required=True)
parser.add_argument('--pilot-output',type=Path)
args=parser.parse_args();repo=args.repo
a=pd.read_parquet(args.aligned,columns=['building_id','timestamp','demand_kwh'])
b=pd.read_parquet(args.joined,columns=['building_id','hour_start_utc','demand_kwh'])
a.building_id=a.building_id.astype(str);b.building_id=b.building_id.astype(str)
a.timestamp=pd.to_datetime(a.timestamp,utc=True);b.hour_start_utc=pd.to_datetime(b.hour_start_utc,utc=True)
a=a.sort_values(['building_id','timestamp']).reset_index(drop=True)
b=b.sort_values(['building_id','hour_start_utc']).reset_index(drop=True)
if not a.building_id.equals(b.building_id):raise ValueError('Building keys differ')
if not np.array_equal(a.timestamp.to_numpy(),b.hour_start_utc.to_numpy()):raise ValueError('Hourly keys differ')
error=np.max(np.abs(a.demand_kwh.to_numpy()-b.demand_kwh.to_numpy()))
if error>=1e-9:raise ValueError('Hourly demand differs')
raw=pd.read_parquet(args.raw_example,columns=['timestamp','out.electricity.total.energy_consumption'])
old=raw.groupby(raw.timestamp.dt.floor('h')).size()
new=raw.groupby((raw.timestamp-pd.Timedelta(minutes=15)).dt.floor('h')).size()
evidence=dict(rows=len(a),buildings=a.building_id.nunique(),exact_keys_match_existing_simulation=True,max_demand_difference_kwh=float(error),
              raw_example_building=args.raw_example.stem.split('-')[0],raw_intervals=len(raw),old_direct_floor_hours=len(old),old_first_bin_intervals=int(old.iloc[0]),old_last_bin_intervals=int(old.iloc[-1]),
              corrected_hours=len(new),corrected_min_intervals=int(new.min()),corrected_max_intervals=int(new.max()),
              status=f'Compared all {a.building_id.nunique()} profiles against existing simulation demand',
              old_forecast_result_status='Saved legacy RF/LSTM metrics preserved; cannot establish preprocessing provenance without original run inputs')
args.output.parent.mkdir(parents=True,exist_ok=True)
args.output.write_text(json.dumps(evidence,indent=2))
print(json.dumps(evidence,indent=2))
if args.pilot_output:
    # Small deterministic pilot cohort, explicitly distinct from full-model evidence.
    meta=pd.read_csv(repo.parent/'DATA_for_project/CO_baseline_metadata_and_annual_results.csv',dtype=str)
    ids=sorted(set(meta.loc[meta.upgrade.eq('0')&meta['in.geometry_building_type_acs'].eq('Single-Family Detached'),'bldg_id'])&set(a.building_id),key=int)[:10]
    full=pd.read_parquet(args.aligned);full.building_id=full.building_id.astype(str)
    out=args.pilot_output;out.mkdir(exist_ok=False)
    full[full.building_id.isin(ids)].to_parquet(out/'hourly.parquet',index=False,coerce_timestamps='us')
    joined=pd.read_parquet(args.joined);joined.building_id=joined.building_id.astype(str)
    joined[joined.building_id.isin(ids)].to_parquet(out/'joined.parquet',index=False,coerce_timestamps='us')
    print(json.dumps(evidence,indent=2));print('Saved 10-building pilot:',ids)
