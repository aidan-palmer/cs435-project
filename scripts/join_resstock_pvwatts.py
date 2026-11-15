"""Align 2018 Larimer ResStock hourly demand with the Fort Collins rooftop profile."""

import argparse

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import TimestampType


HOURS_PER_YEAR = 8760
EST_LABEL_START = "2018-01-01 00:00:00"
EST_LABEL_END = "2019-01-01 00:00:00"
COLORADO_UTC_START = "2018-01-01 07:00:00"
COLORADO_UTC_END = "2019-01-01 07:00:00"


def read_solar(spark, path):
    solar = spark.read.option("header", True).csv(path).select(
        F.to_timestamp("hour_start_utc").alias("hour_start_utc"),
        "hour_start_mountain",
        F.col("solar_generation_kwh").cast("double"),
    )
    utc_hour = F.col("hour_start_utc")
    generation = F.col("solar_generation_kwh")
    expected_local = F.date_format(
        F.from_utc_timestamp(utc_hour, "America/Denver"), "yyyy-MM-dd'T'HH:mm:ss"
    )
    local = F.col("hour_start_mountain")
    bad_row = (
        utc_hour.isNull()
        | (utc_hour < F.lit(COLORADO_UTC_START).cast("timestamp"))
        | (utc_hour >= F.lit(COLORADO_UTC_END).cast("timestamp"))
        | (utc_hour != F.date_trunc("hour", utc_hour))
        | ~utc_hour.eqNullSafe(F.to_timestamp(local))
        | ~F.substring(local, 1, 19).eqNullSafe(expected_local)
        | generation.isNull()
        | F.isnan(generation)
        | (generation < 0)
        | (generation == float("inf"))
    )
    summary = solar.agg(
        F.count("*").alias("rows"),
        F.count_distinct(utc_hour).alias("hours"),
        F.count_if(bad_row).alias("invalid"),
    ).first()
    if summary.rows != HOURS_PER_YEAR or summary.hours != HOURS_PER_YEAR or summary.invalid:
        raise ValueError("PVWatts must have one valid record per hour of the 2018 Colorado year")
    return solar


def read_demand(spark, path):
    demand = spark.read.parquet(path).select("building_id", "hour_start", "demand_kwh")
    if not isinstance(demand.schema["hour_start"].dataType, TimestampType):
        raise ValueError("Expected hour_start from scripts/resstock_pilot.py")

    label = F.col("hour_start")
    energy = F.col("demand_kwh")
    bad_row = (
        F.col("building_id").isNull()
        | label.isNull()
        | (label < F.lit(EST_LABEL_START).cast("timestamp"))
        | (label >= F.lit(EST_LABEL_END).cast("timestamp"))
        | (label != F.date_trunc("hour", label))
        | energy.isNull()
        | F.isnan(energy)
        | (energy < 0)
        | (energy == float("inf"))
    )
    coverage = demand.groupBy("building_id").agg(
        F.count("*").alias("rows"),
        F.count_distinct(label).alias("hours"),
        F.count_if(bad_row).alias("invalid"),
    ).agg(
        F.count("*").alias("buildings"),
        F.count_if(
            (F.col("rows") != HOURS_PER_YEAR)
            | (F.col("hours") != HOURS_PER_YEAR)
            | (F.col("invalid") > 0)
        ).alias("invalid_buildings"),
    ).first()
    if not coverage.buildings or coverage.invalid_buildings:
        raise ValueError("ResStock must have one valid record per published EST hour per building")

    # The pilot stores EST wall-clock labels in Spark's UTC session. Add five
    # hours to obtain actual UTC, then restore the first two wrapped hours
    # to December 31 in Colorado. Keep the original label for traceability.
    utc_hour = label + F.expr("INTERVAL 5 HOURS")
    return demand.select(
        "building_id",
        F.date_format(label, "yyyy-MM-dd'T'HH:mm:ss").alias("resstock_hour_label_est"),
        F.when(
            utc_hour < F.lit(COLORADO_UTC_START).cast("timestamp"),
            utc_hour + F.expr("INTERVAL 365 DAYS"),
        ).otherwise(utc_hour).alias("hour_start_utc"),
        "demand_kwh",
    ), coverage.buildings


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("resstock", help="Hourly Parquet from scripts/resstock_pilot.py")
    parser.add_argument("pvwatts", help="Fort Collins rooftop hourly CSV")
    parser.add_argument("output", help="New directory for the joined Parquet")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("SolarOpt demand and rooftop solar").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    try:
        solar = read_solar(spark, args.pvwatts)
        demand, buildings = read_demand(spark, args.resstock)
        joined = demand.join(F.broadcast(solar), "hour_start_utc").select(
            "building_id", "hour_start_utc", "hour_start_mountain", "resstock_hour_label_est",
            "demand_kwh", "solar_generation_kwh",
        )
        joined.write.mode("errorifexists").parquet(args.output)
        print(f"Joined {buildings:,} buildings × {HOURS_PER_YEAR:,} hours to {args.output}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
