"""Validate ResStock baseline profiles, aggregate hourly demand and score a weekly baseline."""

import argparse
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession, Window, functions as F


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profiles", type=Path)
    parser.add_argument("output", type=Path)
    cohort = parser.add_mutually_exclusive_group()
    cohort.add_argument("--ids", nargs="+", type=int, default=[128, 1414, 3575])
    cohort.add_argument("--all-larimer", action="store_true", help="Use every Larimer County baseline profile")
    parser.add_argument("--test-start", required=True, help="First test date, YYYY-MM-DD")
    parser.add_argument("--electricity-column", default="out.electricity.total.energy_consumption")
    parser.add_argument("--metadata", type=Path, required=True)
    args = parser.parse_args()
    cutoff = datetime.strptime(args.test_start, "%Y-%m-%d")
    spark = SparkSession.builder.appName("SolarOpt ResStock pilot").getOrCreate()
    # Use a fixed clock for the source's wall times. UTC here prevents DST
    # reinterpretation and does not assert that the source reports UTC.
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.legacy.parquet.nanosAsLong", "true")
    try:
        metadata = spark.read.option("header", True).csv(str(args.metadata))
        eligible = metadata.where(
            (F.col("upgrade") == "0")
            & (F.col("`in.state`") == "CO")
            & (F.col("`in.county`") == "G0800690")
        )
        if not args.all_larimer:
            eligible = eligible.where(
                F.col("`in.geometry_building_type_acs`") == "Single-Family Detached"
            )
        eligible_ids = {
            int(row.bldg_id) for row in eligible.select("bldg_id").distinct().collect()
        }
        ids = sorted(eligible_ids if args.all_larimer else set(args.ids))
        if not ids or not set(ids).issubset(eligible_ids):
            raise ValueError("No matching profiles or some IDs are outside the selected metadata cohort")
        print(f"Processing {len(ids)} Larimer County baseline profiles")
        raw = spark.read.parquet(*(str(args.profiles / f"{i}-0.parquet") for i in ids))
        if args.electricity_column not in raw.columns:
            raise ValueError(f"Electricity column not found: {args.electricity_column}")
        data = raw.select(
            F.regexp_extract(F.input_file_name(), r"/([0-9]+)-0\.parquet$", 1)
            .cast("long").alias("building_id"),
            F.expr("timestamp_micros(timestamp DIV 1000)").alias("interval_end"),
            F.col(f"`{args.electricity_column}`").cast("double").alias("interval_kwh"),
        ).cache()
        if data.where(
            F.col("building_id").isNull() | F.col("interval_end").isNull()
            | F.col("interval_kwh").isNull() | F.isnan("interval_kwh")
            | (F.col("interval_kwh") < 0) | (F.col("interval_kwh") == float("inf"))
        ).limit(1).count():
            raise ValueError("Invalid building ID, timestamp or demand")
        window = Window.partitionBy("building_id").orderBy("interval_end")
        gaps = data.withColumn("seconds", F.col("interval_end").cast("long")).withColumn(
            "delta", F.col("seconds") - F.lag("seconds").over(window)
        ).where((F.col("delta") != 900) | (F.col("seconds") % 900 != 0))
        if gaps.limit(1).count():
            raise ValueError("Duplicate, missing or misaligned 15-minute intervals")
        hourly = data.withColumn(
            "hour_start", F.date_trunc("hour", F.expr("interval_end - INTERVAL 15 MINUTES"))
        ).groupBy("building_id", "hour_start").agg(
            F.sum("interval_kwh").alias("demand_kwh"), F.count("*").alias("intervals")
        ).cache()
        if hourly.where(F.col("intervals") != 4).limit(1).count():
            raise ValueError("Incomplete hourly bins")
        data.groupBy("building_id").agg(
            F.count("*").alias("intervals"), F.min("interval_end"), F.max("interval_end"),
            F.sum("interval_kwh").alias("source_kwh")
        ).join(hourly.groupBy("building_id").agg(
            F.count("*").alias("hours"), F.sum("demand_kwh").alias("hourly_kwh")
        ), "building_id").orderBy("building_id").show(len(ids) if len(ids) <= 10 else 10, truncate=False)
        past = hourly.select(
            "building_id", F.expr("hour_start + INTERVAL 7 DAYS").alias("hour_start"),
            F.col("demand_kwh").alias("prediction_kwh")
        )
        scored = hourly.where(F.col("hour_start") >= F.lit(cutoff.isoformat()).cast("timestamp")).join(
            past, ["building_id", "hour_start"]
        ).withColumn("error", F.col("prediction_kwh") - F.col("demand_kwh"))
        metrics = scored.agg(
            F.count("*").alias("scored_hours"),
            F.avg(F.abs("error")).alias("mae_kwh"),
            F.sqrt(F.avg(F.pow("error", 2))).alias("rmse_kwh")
        )
        metrics.show()
        if metrics.first().scored_hours == 0:
            raise ValueError("No test hours matched a prior-week observation")
        # Synthetic end-to-end smoke check. These inputs are not research estimates.
        fixture = scored.orderBy("hour_start", "building_id").limit(3).withColumn(
            "test_solar_kwh", F.lit(0.5)
        ).withColumn("grid_import_kwh", F.greatest(F.col("demand_kwh") - F.col("test_solar_kwh"), F.lit(0.0))
        ).withColumn("test_avoided_co2_kg", (F.col("demand_kwh") - F.col("grid_import_kwh")) * F.lit(0.4))
        fixture.show()
        hourly.drop("intervals").write.mode("errorifexists").parquet(str(args.output))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
