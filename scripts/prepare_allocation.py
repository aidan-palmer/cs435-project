"""Create scored allocation inputs from aligned household demand and solar Parquet.

Fit a frozen UTC hour-of-week demand profile using only pre-evaluation history.
Use explicit cost and constant-emissions assumptions to score the held-out period.
"""

import argparse
import csv
from datetime import date, datetime, time, timedelta, timezone
import hashlib
import json
import math
from pathlib import Path
from zoneinfo import ZoneInfo

from pyspark.errors import PySparkException
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import NumericType, TimestampType

if __package__:
    from .allocate_solar import (compare_allocations, dollars, experiment_summary, money_cents,
                                 nonnegative_number, read_candidates, save_experiment)
else:
    from allocate_solar import (compare_allocations, dollars, experiment_summary, money_cents,
                               nonnegative_number, read_candidates, save_experiment)


DEFAULT_METADATA = Path(__file__).resolve().parents[2] / 'DATA_for_project/CO_baseline_metadata_and_annual_results.csv'


def windows(start, end, training_days=28):
    if training_days < 7 or training_days % 7:
        raise ValueError('Training days must be a positive multiple of seven')
    start_day, end_day = date.fromisoformat(start), date.fromisoformat(end)
    if start_day >= end_day:
        raise ValueError('Evaluation end must be later than start; end is exclusive')
    mountain = ZoneInfo('America/Denver')
    start_utc = datetime.combine(start_day, time.min, mountain).astimezone(timezone.utc)
    end_utc = datetime.combine(end_day, time.min, mountain).astimezone(timezone.utc)
    return start_utc - timedelta(days=training_days), start_utc, end_utc


def eligible_ids(metadata, building_ids, building_type='Single-Family Detached'):
    selected, seen = set(), set()
    with Path(metadata).open(encoding='utf-8-sig', newline='') as source:
        rows = csv.DictReader(source)
        required = {'bldg_id', 'upgrade', 'in.geometry_building_type_acs'}
        if required - set(rows.fieldnames or []):
            raise ValueError('Metadata requires bldg_id, upgrade, and in.geometry_building_type_acs')
        for row in rows:
            building = row['bldg_id']
            if building not in building_ids or row['upgrade'] != '0':
                continue
            if building in seen:
                raise ValueError('Duplicate baseline metadata for building ' + building)
            seen.add(building)
            if row['in.geometry_building_type_acs'] == building_type:
                selected.add(building)
    if building_ids - seen:
        raise ValueError('Some input buildings have no baseline metadata; provide matching metadata or --all-buildings')
    return selected


def score_households(hourly, start, end, *, training_days=28, factor=1.0):
    if hourly.sparkSession.conf.get('spark.sql.session.timeZone') != 'UTC':
        raise ValueError('Allocation preparation requires the Spark session timezone UTC')
    required = {'building_id', 'hour_start_utc', 'demand_kwh', 'solar_generation_kwh'}
    if missing := required - set(hourly.columns):
        raise ValueError('Missing joined columns: ' + ', '.join(sorted(missing)))
    if not isinstance(hourly.schema['hour_start_utc'].dataType, TimestampType):
        raise ValueError('hour_start_utc must be a UTC timestamp')
    for name in ('demand_kwh', 'solar_generation_kwh'):
        if not isinstance(hourly.schema[name].dataType, NumericType):
            raise ValueError(name + ' must be numeric')
    if not math.isfinite(factor) or factor < 0:
        raise ValueError('Emissions factor must be finite and nonnegative')
    train_start, evaluation_start, evaluation_end = windows(start, end, training_days)
    # Spark session timezone is UTC. Keep timestamps inside Spark throughout grouping.
    hour = F.col('hour_start_utc')
    lower, boundary, upper = [F.to_timestamp(F.lit(value.isoformat())) for value in
                              (train_start, evaluation_start, evaluation_end)]
    data = hourly.select(F.col('building_id').cast('string').alias('building_id'),
                         'hour_start_utc', 'demand_kwh', 'solar_generation_kwh')
    data = data.where(hour.isNull() | ((hour >= lower) & (hour < upper))).cache()
    try:
        invalid = (F.col('building_id').isNull() | (F.trim('building_id') == '') | hour.isNull()
                   | (hour != F.date_trunc('hour', hour)))
        for name in ('demand_kwh', 'solar_generation_kwh'):
            value = F.col(name)
            invalid = invalid | value.isNull() | F.isnan(value) | (value < 0) | (value == float('inf'))
        coverage = data.groupBy('building_id').agg(
            F.count('*').alias('rows'), F.count_distinct('hour_start_utc').alias('hours'),
            F.count_if(invalid).alias('invalid'), F.count_if(hour < boundary).alias('training_hours'),
            F.count_if(hour >= boundary).alias('evaluation_hours'),
        ).collect()
        expected = int((evaluation_end - evaluation_start).total_seconds() / 3600)
        if not coverage:
            raise ValueError('No data for the requested training/evaluation windows')
        if any(row.invalid or row.rows != row.hours or row.training_hours != training_days * 24
               or row.evaluation_hours != expected for row in coverage):
            raise ValueError('Each building needs complete, unique, aligned hours and finite nonnegative energy in both windows')
        keyed = data.withColumn('week_hour', (F.dayofweek(hour) - 1) * 24 + F.hour(hour))
        profile = keyed.where(hour < boundary).groupBy('building_id', 'week_hour').agg(
            F.avg('demand_kwh').alias('predicted_kwh')
        )
        targets = keyed.where(hour >= boundary).join(profile, ['building_id', 'week_hour'], 'inner')
        scored = targets.groupBy('building_id').agg(
            F.sum(F.least('predicted_kwh', 'solar_generation_kwh')).alias('predicted_grid_reduction_kwh'),
            F.sum(F.least('demand_kwh', 'solar_generation_kwh')).alias('realized_grid_reduction_kwh'),
            F.count('*').alias('scored_hours'),
        ).orderBy('building_id').collect()
        if any(row.scored_hours != expected for row in scored):
            raise ValueError('The frozen profile did not cover every evaluation hour')
        result = []
        for row in scored:
            values = row.asDict()
            for key in ('predicted_grid_reduction_kwh', 'realized_grid_reduction_kwh'):
                values[key] = float(values[key])
            values['predicted_avoided_co2_kg'] = values['predicted_grid_reduction_kwh'] * factor
            values['realized_avoided_co2_kg'] = values['realized_grid_reduction_kwh'] * factor
            if not all(math.isfinite(values[key]) and values[key] >= 0 for key in (
                    'predicted_avoided_co2_kg', 'realized_avoided_co2_kg')):
                raise ValueError('Aggregated CO2 benefits are outside the finite numeric range')
            result.append(values)
        return result
    finally:
        data.unpersist()


def save_inputs(output, scores, cost_cents, eligible, assumptions):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    candidates = output / 'allocation-candidates.csv'
    fields = ['building_id', 'installation_cost_usd', 'predicted_avoided_co2_kg',
              'realized_avoided_co2_kg', 'eligible', 'predicted_grid_reduction_kwh',
              'realized_grid_reduction_kwh', 'scored_hours']
    with candidates.open('w', encoding='utf-8', newline='') as destination:
        writer = csv.DictWriter(destination, fieldnames=fields)
        writer.writeheader()
        for score in scores:
            writer.writerow({**score, 'installation_cost_usd': dollars(cost_cents),
                             'eligible': str(score['building_id'] in eligible).lower()})
    with (output / 'allocation-assumptions.json').open('w', encoding='utf-8') as destination:
        json.dump(assumptions, destination, indent=2, allow_nan=False)
        destination.write('\n')
    return candidates


def ask(value, prompt):
    if value is not None:
        return value
    try:
        return input(prompt).strip()
    except EOFError:
        raise ValueError('Interactive input unavailable; supply the cost, emissions factor, and emissions-source flags') from None


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('joined', help='Joined demand and solar Parquet, e.g. data/processed/resstock-pvwatts-2018')
    parser.add_argument('output', type=Path, help='New directory for input files and optional budget runs')
    parser.add_argument('--evaluation-start', required=True, help='First Mountain Time date, inclusive')
    parser.add_argument('--evaluation-end', required=True, help='Last Mountain Time date, exclusive')
    parser.add_argument('--training-days', type=int, default=28)
    parser.add_argument('--installation-cost-usd', help='Uniform per-home cost; prompted if omitted')
    parser.add_argument('--emissions-factor-kg-per-kwh', help='Constant factor; prompted if omitted')
    parser.add_argument('--emissions-source', help='Chosen source/method, or explicit sensitivity-scenario label')
    parser.add_argument('--pv-capacity-kw', default='4', help='Capacity represented by the input solar profile (default: project 4 kW)')
    parser.add_argument('--metadata', type=Path, default=DEFAULT_METADATA)
    parser.add_argument('--all-buildings', action='store_true', help='Include all modeled building types instead of detached homes only')
    parser.add_argument('--budgets-usd', nargs='+', help='Optionally run allocations after creating the files')
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--random-runs', type=int, default=100)
    args = parser.parse_args(argv)
    try:
        if args.output.exists():
            raise ValueError('Output already exists; choose a new directory')
        train_start, start, end = windows(args.evaluation_start, args.evaluation_end, args.training_days)
        if args.random_runs < 1:
            raise ValueError('Random runs must be at least one')
        budgets = [money_cents(value, 'Budget') for value in (args.budgets_usd or [])]
        if len(budgets) != len(set(budgets)):
            raise ValueError('Budgets must be distinct')
        cost = money_cents(ask(args.installation_cost_usd, f'Installation cost per household system (USD; {args.pv_capacity_kw} kW profile): '), 'Installation cost', positive=True)
        factor = nonnegative_number(ask(args.emissions_factor_kg_per_kwh, 'Chosen constant CO2 factor (kg CO2/kWh): '), 'Emissions factor')
        if not math.isfinite(float(factor)):
            raise ValueError('Emissions factor exceeds the numeric calculation range')
        source = ask(args.emissions_source, 'Emissions source/method, or label this as a sensitivity assumption: ').strip()
        if not source:
            raise ValueError('Emissions source or scenario label is required')
        capacity = nonnegative_number(args.pv_capacity_kw, 'PV capacity')
        if not capacity:
            raise ValueError('PV capacity must be positive')
        if not args.all_buildings and not args.metadata.is_file():
            raise ValueError('Detached-home eligibility requires --metadata; use --all-buildings for an explicitly broader scenario')
    except ValueError as error:
        parser.error(str(error))
    spark = SparkSession.builder.appName('SolarOpt allocation input preparation').getOrCreate()
    spark.conf.set('spark.sql.session.timeZone', 'UTC')
    try:
        scores = score_households(spark.read.parquet(args.joined), args.evaluation_start, args.evaluation_end,
                                  training_days=args.training_days, factor=float(factor))
        ids = {row['building_id'] for row in scores}
        eligible = ids if args.all_buildings else eligible_ids(args.metadata, ids)
        if not eligible:
            raise ValueError('No households satisfy the eligibility filter')
        assumptions = {
            'forecast_run_id': f'frozen-UTC-weekly-profile-{args.training_days}d:{train_start.isoformat()}/{start.isoformat()}',
            'pv_profile_id': 'solar_generation_kwh from ' + args.joined,
            'pv_capacity_kw': str(capacity),
            'emissions_source': f'{source}; constant {factor} kg CO2/kWh; zero export credit',
            'evaluation_start': args.evaluation_start, 'evaluation_end': args.evaluation_end,
            'preparation': {'input': args.joined, 'training_start_utc': train_start.isoformat(),
                            'training_end_utc_exclusive': start.isoformat(), 'evaluation_end_utc_exclusive': end.isoformat(),
                            'timezone': 'America/Denver', 'forecast_clock': 'UTC hour of week',
                            'building_count': len(scores), 'eligible_count': len(eligible),
                            'uniform_installation_cost_usd': dollars(cost),
                            'emissions_factor_kg_per_kwh': str(factor),
                            'eligibility': 'all buildings' if args.all_buildings else 'Single-Family Detached',
                            'method': 'Frozen pre-evaluation weekday/hour demand mean; no evaluation demand used in prediction'},
        }
        path = save_inputs(args.output, scores, cost, eligible, assumptions)
        print(f'Created {path} and {args.output / "allocation-assumptions.json"}', flush=True)
        print(f'{len(scores)} buildings scored; {len(eligible)} eligible; {scores[0]["scored_hours"]} hours per building', flush=True)
        candidates = read_candidates(path)
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        for budget in budgets:
            ranked, selections, results = compare_allocations(candidates, budget, seed=args.seed, random_runs=args.random_runs)
            summary = experiment_summary(candidates, results, budget, 'benefit_per_dollar', args.seed, assumptions, digest)
            folder = args.output / ('budget-' + dollars(budget).replace('.00', ''))
            save_experiment(folder, candidates, ranked, selections, results, summary)
            print(f'Budget ${dollars(budget)}: {results[0]["selected_count"]} prioritized homes; saved {folder}', flush=True)
    except (OSError, ValueError, PySparkException) as error:
        parser.error(str(error))
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
