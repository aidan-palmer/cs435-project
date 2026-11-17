"""Simulate hourly grid imports for each building with and without solar."""

import argparse

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import NumericType, TimestampType


def simulate_grid_import(hourly):
    """Add per-building, per-hour energy flows to the joined demand and solar data."""
    required = {"building_id", "hour_start_utc", "demand_kwh", "solar_generation_kwh"}
    missing = required - set(hourly.columns)
    if missing:
        raise ValueError(f"Missing hourly columns: {', '.join(sorted(missing))}")
    if not isinstance(hourly.schema["hour_start_utc"].dataType, TimestampType):
        raise ValueError("hour_start_utc must be a UTC timestamp")
    for name in ("demand_kwh", "solar_generation_kwh"):
        if not isinstance(hourly.schema[name].dataType, NumericType):
            raise ValueError(f"{name} must be numeric")

    hour = F.col("hour_start_utc")
    demand = F.col("demand_kwh")
    solar = F.col("solar_generation_kwh")
    invalid = (
        F.col("building_id").isNull()
        | hour.isNull()
        | (F.unix_micros(hour) % 3_600_000_000 != 0)
        | demand.isNull()
        | F.isnan(demand)
        | (demand < 0)
        | (demand == float("inf"))
        | solar.isNull()
        | F.isnan(solar)
        | (solar < 0)
        | (solar == float("inf"))
    )
    summary = hourly.agg(
        F.count("*").alias("rows"),
        F.count_distinct("building_id", "hour_start_utc").alias("building_hours"),
        F.count_if(invalid).alias("invalid"),
    ).first()
    if not summary.rows:
        raise ValueError("No hourly demand and solar rows were provided")
    if summary.invalid:
        raise ValueError("Input requires building IDs, aligned UTC hours, and finite nonnegative kWh")
    if summary.rows != summary.building_hours:
        raise ValueError("Duplicate building and UTC hour in the input")

    self_consumed = F.least(demand, solar)
    return hourly.select(
        "*",
        demand.alias("grid_import_without_solar_kwh"),
        (demand - self_consumed).alias("grid_import_with_solar_kwh"),
        self_consumed.alias("self_consumed_solar_kwh"),
        (solar - self_consumed).alias("excess_solar_kwh"),
        self_consumed.alias("grid_reduction_kwh"),
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("hourly", help="Joined ResStock and PVWatts Parquet directory")
    parser.add_argument("output", help="New directory for hourly simulation Parquet")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("SolarOpt grid import").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    try:
        hourly = spark.read.parquet(args.hourly)
        try:
            results = simulate_grid_import(hourly)
        except ValueError as error:
            parser.error(str(error))
        results.write.mode("errorifexists").parquet(args.output)
        print(f"Wrote hourly grid-import simulation to {args.output}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
