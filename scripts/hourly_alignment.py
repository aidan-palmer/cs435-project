"""Shared ResStock interval-end aggregation and 2018 fixed-EST conversion."""
from pyspark.sql import functions as F


def aggregate_intervals(data):
    """Input: building_id, interval_end, interval_kwh, optional temperature_c.

    Source timestamps encode fixed EST wall labels, not UTC instants.
    A quarter-hour stamped 01:00 belongs to the 00:00 hourly bin.
    """
    if data.sparkSession.conf.get('spark.sql.session.timeZone') != 'UTC':
        raise ValueError('Use UTC Spark session to preserve fixed source wall labels')
    invalid = F.col('building_id').isNull() | F.col('interval_end').isNull()
    for col in ['interval_kwh'] + (['temperature_c'] if 'temperature_c' in data.columns else []):
        value = F.col(col)
        invalid = invalid | value.isNull() | F.isnan(value) | (F.abs(value)==float('inf'))
    invalid = invalid | (F.col('interval_kwh') < 0)
    invalid = invalid | (F.unix_micros('interval_end') % 900000000 != 0)
    if data.where(invalid).limit(1).count():
        raise ValueError('Invalid energy, temperature, or quarter-hour timestamp')
    aggregations = [F.sum('interval_kwh').alias('demand_kwh'), F.count('*').alias('intervals'),
                    F.count_distinct('interval_end').alias('unique_intervals')]
    if 'temperature_c' in data.columns:
        aggregations.append(F.avg('temperature_c').alias('temperature_c'))
    hourly = data.withColumn('hour_start', F.date_trunc('hour', F.expr('interval_end - INTERVAL 15 MINUTES'))).groupBy('building_id','hour_start').agg(*aggregations)
    if hourly.where((F.col('intervals')!=4)|(F.col('unique_intervals')!=4)).limit(1).count():
        raise ValueError('Incomplete or duplicate quarter-hour bins')
    return hourly.drop('intervals','unique_intervals')


def est_2018_to_utc(label):
    """Restore ResStock's wrapped first two EST hours to Colorado year end."""
    utc = label + F.expr('INTERVAL 5 HOURS')
    return F.when(utc < F.lit('2018-01-01 07:00:00').cast('timestamp'),
                  utc + F.expr('INTERVAL 365 DAYS')).otherwise(utc)
