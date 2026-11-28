"""Build validated UTC hourly demand/temperature from 2018 ResStock intervals."""
import argparse
import time

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import LongType

if __package__:
    from .hourly_alignment import aggregate_intervals, est_2018_to_utc
    from .runtime_evidence import save_runtime
else:
    from hourly_alignment import aggregate_intervals, est_2018_to_utc
    from runtime_evidence import save_runtime


def load_parquets(spark, data_dir):
    loads = spark.read.parquet(data_dir)
    stamp = (F.expr('timestamp_micros(timestamp DIV 1000)')
             if isinstance(loads.schema['timestamp'].dataType, LongType) else F.col('timestamp'))
    return loads.select(F.col('bldg_id').cast('string').alias('building_id'),
                        stamp.alias('interval_end'),
                        F.col('`out.electricity.total.energy_consumption`').alias('interval_kwh'),
                        F.col('`out.outdoor_air_dryblub_temp.c`').alias('temperature_c'))


def build_hourly(df):
    hourly = aggregate_intervals(df)
    label = F.col('hour_start')
    coverage = hourly.groupBy('building_id').agg(F.count('*').alias('hours'),
        F.date_format(F.min(label),'yyyy-MM-dd HH:mm:ss').alias('first'), F.date_format(F.max(label),'yyyy-MM-dd HH:mm:ss').alias('last')).collect()
    if not coverage or any(r.hours!=8760 or str(r.first)!='2018-01-01 00:00:00'
                           or str(r.last)!='2018-12-31 23:00:00' for r in coverage):
        raise ValueError('Expected complete 2018 fixed-EST calendar per building')
    return hourly.select('building_id', label.alias('resstock_hour_label_est'),
                         est_2018_to_utc(label).alias('timestamp'),
                         'demand_kwh','temperature_c')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('input');parser.add_argument('output')
    parser.add_argument('--runtime-output', required=True)
    args=parser.parse_args()
    spark=SparkSession.builder.appName('SolarOpt aligned hourly features').getOrCreate()
    spark.conf.set('spark.sql.session.timeZone','UTC')
    spark.conf.set('spark.sql.legacy.parquet.nanosAsLong','true')
    started=time.perf_counter()
    try:
        hourly=build_hourly(load_parquets(spark,args.input))
        hourly.write.mode('errorifexists').parquet(args.output)
        elapsed=time.perf_counter()-started
        saved=spark.read.parquet(args.output)
        counts=saved.agg(F.count('*').alias('rows'),F.count_distinct('building_id').alias('buildings')).first()
        save_runtime(args.runtime_output, dict(processing_seconds=elapsed),
                     dict(input=args.input, output=args.output, timestamp_clock='UTC',
                          source_clock='fixed EST interval ends',rows=counts.rows,buildings=counts.buildings),spark)
        print(f'Saved {counts.rows:,} UTC hourly rows for {counts.buildings} buildings')
    finally:spark.stop()


if __name__=='__main__':main()
