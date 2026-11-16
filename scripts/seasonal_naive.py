
import argparse

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window


def build_forecasts(hourly):
    """
    Generate next-day hourly forecasts for each building.

    Previous-day:  prediction(t) = demand(t - 24 hours)
    Previous-week: prediction(t) = demand(t - 168 hours)

    Returns one row per target hour with predictions from
    both baselines, for complete 24-hour forecast windows.
    """

    hourly = (
        hourly.select(
            F.col("building_id").cast("string").alias("building_id"),
            F.col("timestamp").alias("timestamp"),
            F.col("demand_kwh").cast("double").alias("demand_kwh")
        )
        .filter(
            F.col("building_id").isNotNull()
            & F.col("timestamp").isNotNull()
            & F.col("demand_kwh").isNotNull()
            & (~F.isnan("demand_kwh"))
            & (F.col("demand_kwh") >= 0)
        )
    )

    # Each building must have at most one observation per hour.
    duplicates = (
        hourly.groupBy("building_id", "timestamp")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )

    if duplicates:
        raise ValueError(
            "Duplicate building/timestamp records found. "
            "Resolve duplicates before forecasting."
        )

    # Alias three copies for timestamp-based joins.
    target = hourly.alias("t")
    previous_day = hourly.alias("d")
    previous_week = hourly.alias("w")

    # Join using exact elapsed-hour offsets.
    # This is safer than row-based lag() when hours are missing.
    forecasts = (
        target
        .join(
            previous_day,
            (F.col("t.building_id") == F.col("d.building_id"))
            & (
                F.col("d.timestamp")
                == F.expr("t.timestamp - INTERVAL 24 HOURS")
            ),
            "inner"
        )
        .join(
            previous_week,
            (F.col("t.building_id") == F.col("w.building_id"))
            & (
                F.col("w.timestamp")
                == F.expr("t.timestamp - INTERVAL 168 HOURS")
            ),
            "inner"
        )
        .select(
            F.col("t.building_id").alias("building_id"),
            F.col("t.timestamp").alias("target_timestamp"),
            F.col("t.demand_kwh").alias("actual_kwh"),
            F.col("d.demand_kwh").alias("previous_day_kwh"),
            F.col("w.demand_kwh").alias("previous_week_kwh")
        )
    )

    # A forecast issued at 23:00 the previous day predicts
    # 00:00 through 23:00 on the target day.
    forecasts = (
        forecasts
        .withColumn(
            "forecast_issued_at",
            F.expr(
                "date_trunc('DAY', target_timestamp) "
                "- INTERVAL 1 HOURS"
            )
        )
        .withColumn(
            "horizon",
            F.hour("target_timestamp") + 1
        )
    )

    # Require all 24 target hours and corresponding historical
    # observations to exist for both baseline methods.
    forecast_window = Window.partitionBy(
        "building_id", "forecast_issued_at"
    )

    forecasts = (
        forecasts
        .withColumn(
            "hours_available",
            F.count("*").over(forecast_window)
        )
        .filter(F.col("hours_available") == 24)
        .drop("hours_available")
    )

    # Convert into a common prediction format.
    day = (
        forecasts
        .select(
            "building_id",
            "forecast_issued_at",
            "target_timestamp",
            "horizon",
            "actual_kwh",
            F.col("previous_day_kwh").alias("predicted_kwh")
        )
        .withColumn("model", F.lit("seasonal_naive_day"))
    )

    week = (
        forecasts
        .select(
            "building_id",
            "forecast_issued_at",
            "target_timestamp",
            "horizon",
            "actual_kwh",
            F.col("previous_week_kwh").alias("predicted_kwh")
        )
        .withColumn("model", F.lit("seasonal_naive_week"))
    )

    return day.unionByName(week)


def evaluate(predictions):
    """Calculate MAE and RMSE separately for each model."""

    errors = (
        predictions
        .withColumn(
            "absolute_error",
            F.abs(F.col("actual_kwh") - F.col("predicted_kwh"))
        )
        .withColumn(
            "squared_error",
            F.pow(F.col("actual_kwh") - F.col("predicted_kwh"), 2)
        )
    )

    metrics = (
        errors.groupBy("model")
        .agg(
            F.avg("absolute_error").alias("mae"),
            F.sqrt(F.avg("squared_error")).alias("rmse"),
            F.count("*").alias("num_predictions"),
            F.countDistinct(
                F.struct("building_id", "forecast_issued_at")
            ).alias("num_forecasts")
        )
        .orderBy("model")
    )

    return metrics


def main():
    parser = argparse.ArgumentParser(
        description="SolarOpt seasonal-naive forecasting"
    )

    parser.add_argument(
        "input",
        help="Path to hourly ResStock Parquet dataset"
    )
    parser.add_argument(
        "output",
        help="Output directory"
    )
    parser.add_argument(
        "--test-start",
        required=True,
        help="First test target date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--test-end",
        default=None,
        help="Exclusive end of test period (YYYY-MM-DD)"
    )

    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("SolarOpt Seasonal Naive")
        .getOrCreate()
    )

    # Read the complete historical dataset.
    # Keep earlier history available for test-period forecasts.
    hourly = spark.read.parquet(args.input)

    predictions = build_forecasts(hourly)

    # Filter by target date AFTER generating forecasts.
    predictions = predictions.filter(
        F.to_date("target_timestamp") >= F.lit(args.test_start)
    )

    if args.test_end is not None:
        predictions = predictions.filter(
            F.to_date("target_timestamp") < F.lit(args.test_end)
        )

    predictions = predictions.cache()

    if predictions.limit(1).count() == 0:
        raise ValueError(
            "No complete forecasts for the selected test period. "
            "Check timestamps, data coverage, and test dates."
        )

    metrics = evaluate(predictions)

    print("\n=== Seasonal Naive Baseline Results ===")
    metrics.show(truncate=False)

    print("\n=== Example Predictions ===")
    (
        predictions
        .orderBy("building_id", "target_timestamp", "model")
        .show(20, truncate=False)
    )

    # Save forecasts in Parquet format.
    (
        predictions
        .write
        .mode("overwrite")
        .partitionBy("model")
        .parquet(args.output + "/predictions")
    )

    # Save evaluation metrics as CSV.
    (
        metrics
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(args.output + "/metrics")
    )

    predictions.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
