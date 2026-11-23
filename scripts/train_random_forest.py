import argparse
import time

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor


BASE_FEATURE_COLS = [
    "lag_0",
    "lag_1",
    "lag_24",
    "same_hour_yesterday",
    "same_hour_last_week",
    "mean_24",
    "mean_168",
    "std_24",
    "target_hour",
    "target_day_of_week",
    "target_month",
    "horizon"
]

TEMP_FEATURE_COLS = [
    "temp_lag_0",
    "temp_lag_24",
    "temp_mean_24",
    "temp_mean_168",
    "temp_min_24",
    "temp_max_24"
]

feature_cols = BASE_FEATURE_COLS.copy()


def build_features(hourly):
    """
    Build next-day forecasting examples.

    Input columns:
        building_id, timestamp, demand_kwh

    Each output row predicts one future hour.
    There are 24 rows per complete daily forecast.
    """

    hourly = (
        hourly
        .select(
            F.col("building_id").cast("string").alias("building_id"),
            F.col("timestamp"),
            F.col("demand_kwh").cast("double").alias("demand_kwh"),
            F.col("temperature_c").cast("double").alias("temperature_c")
        )
        .filter(
            F.col("building_id").isNotNull()
            & F.col("timestamp").isNotNull()
            & F.col("demand_kwh").isNotNull()
            & (~F.isnan("demand_kwh"))
            & (F.col("demand_kwh") >= 0)
        )
    )

    # Validate one unique record per building and hour.
    duplicates = (
        hourly.groupBy("building_id", "timestamp")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )

    if duplicates:
        raise ValueError(
            "Duplicate hourly timestamps found for a building"
        )

    # Each building's observations are ordered independently.
    w = (
        Window.partitionBy("building_id")
        .orderBy("timestamp")
    )

    w24 = w.rowsBetween(-23, 0)
    w168 = w.rowsBetween(-167, 0)

    # Calculate features before filtering to 23:00.
    history = (
        hourly
        # Electricity demand features
        .withColumn("lag_0", F.col("demand_kwh"))
        .withColumn("lag_1", F.lag("demand_kwh", 1).over(w))
        .withColumn("lag_24", F.lag("demand_kwh", 24).over(w))
        .withColumn("mean_24", F.avg("demand_kwh").over(w24))
        .withColumn("mean_168", F.avg("demand_kwh").over(w168))
        .withColumn("std_24", F.stddev("demand_kwh").over(w24))
        
        # Temperature features
        .withColumn("temp_lag_0", F.col("temperature_c"))
        .withColumn(
            "temp_lag_24",
            F.lag("temperature_c", 24).over(w)
        )
        .withColumn(
            "temp_mean_24",
            F.avg("temperature_c").over(w24)
        )
        .withColumn(
            "temp_mean_168",
            F.avg("temperature_c").over(w168)
        )
        .withColumn(
            "temp_min_24",
            F.min("temperature_c").over(w24)
        )
        .withColumn(
            "temp_max_24",
            F.max("temperature_c").over(w24)
        )
        
        .withColumn(
            "history_start",
            F.lag("timestamp", 167).over(w)
        )
    )

    # Issue one forecast per building per day at 23:00.
    issues = (
        history
        .filter(F.hour("timestamp") == 23)
        .withColumnRenamed("timestamp", "forecast_issued_at")
    )

    # Require a complete 168-hour historical period.
    # With unique hourly timestamps, the endpoint check
    # rules out missing intermediate hours.
    issues = issues.filter(
        F.col("history_start").isNotNull()
        & (
            F.col("history_start")
            == F.expr("forecast_issued_at - INTERVAL 167 HOURS")
        )
    )

    # Expand each forecast into 24 hourly targets.
    forecasts = (
        issues
        .withColumn(
            "horizon",
            F.explode(F.sequence(F.lit(1), F.lit(24)))
        )
        .withColumn(
            "target_timestamp",
            F.expr(
                "forecast_issued_at + horizon * INTERVAL 1 HOUR"
            )
        )
        .withColumn(
            "target_hour",
            F.hour("target_timestamp")
        )
        .withColumn(
            "target_day_of_week",
            F.dayofweek("target_timestamp")
        )
        .withColumn(
            "target_month",
            F.month("target_timestamp")
        )
        .withColumn(
            "yesterday_timestamp",
            F.expr("target_timestamp - INTERVAL 24 HOURS")
        )
        .withColumn(
            "last_week_timestamp",
            F.expr("target_timestamp - INTERVAL 168 HOURS")
        )
    )

    # Join actual demand at the target hour.
    targets = hourly.select(
        F.col("building_id").alias("target_id"),
        F.col("timestamp").alias("target_time"),
        F.col("demand_kwh").alias("label")
    )

    forecasts = (
        forecasts.join(
            targets,
            (F.col("building_id") == F.col("target_id"))
            & (F.col("target_timestamp") == F.col("target_time")),
            "inner"
        )
        .drop("target_id", "target_time")
    )

    # Join demand at the same hour on the previous day.
    day = hourly.select(
        F.col("building_id").alias("day_id"),
        F.col("timestamp").alias("day_time"),
        F.col("demand_kwh").alias("same_hour_yesterday")
    )

    forecasts = (
        forecasts.join(
            day,
            (F.col("building_id") == F.col("day_id"))
            & (F.col("yesterday_timestamp") == F.col("day_time")),
            "inner"
        )
        .drop("day_id", "day_time")
    )

    # Join demand at the same hour one week earlier.
    week = hourly.select(
        F.col("building_id").alias("week_id"),
        F.col("timestamp").alias("week_time"),
        F.col("demand_kwh").alias("same_hour_last_week")
    )

    forecasts = (
        forecasts.join(
            week,
            (F.col("building_id") == F.col("week_id"))
            & (F.col("last_week_timestamp") == F.col("week_time")),
            "inner"
        )
        .drop("week_id", "week_time")
    )

    # Retain only complete 24-hour forecasts.
    forecast_window = Window.partitionBy(
        "building_id", "forecast_issued_at"
    )

    forecasts = (
        forecasts
        .withColumn(
            "num_hours",
            F.count("*").over(forecast_window)
        )
        .filter(F.col("num_hours") == 24)
    )

    # Keep only columns needed by training and evaluation.
    return (
        forecasts
        .select(
            "building_id",
            "forecast_issued_at",
            "target_timestamp",
            *feature_cols,
            "label"
        )
        .dropna(subset=feature_cols + ["label"])
    )


def evaluate(predictions):
    """
    Calculate MAE, RMSE, and prediction count.
    """
    errors = (
        predictions
        .withColumn(
            "absolute_error",
            F.abs(F.col("label") - F.col("prediction"))
        )
        .withColumn(
            "squared_error",
            F.pow(F.col("label") - F.col("prediction"), 2)
        )
    )

    result = errors.agg(
        F.avg("absolute_error").alias("mae"),
        F.sqrt(F.avg("squared_error")).alias("rmse"),
        F.count("*").alias("count")
    ).first()

    return {
        "mae": result["mae"],
        "rmse": result["rmse"],
        "count": result["count"]
    }


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("input", help="Hourly Parquet path")
    parser.add_argument("output", help="Output directory")
    parser.add_argument(
        "--use-temperature",
        action="store_true",
        help="Include historical temperature features"
    )
    args = parser.parse_args()
    
    if args.use_temperature:
        global feature_cols
        feature_cols += TEMP_FEATURE_COLS

    spark = (
        SparkSession.builder
        .appName("SolarOpt Random Forest")
        .getOrCreate()
    )

    # Read hourly data.
    hourly = spark.read.parquet(args.input)

    # Generate features and labels.
    features = build_features(hourly)

    # Save features so we can reuse them later.
    features.write.mode("overwrite").parquet(
        args.output + "/features"
    )

    # Read back materialized features to avoid recomputing
    # the window functions and joins during training.
    features = spark.read.parquet(args.output + "/features")

    # Get all distinct forecast target dates
    dates = (
        features
        .select(F.to_date("target_timestamp").alias("date"))
        .distinct()
        .orderBy("date")
        .collect()
    )

    dates = [row["date"] for row in dates]

    if len(dates) < 3:
        raise ValueError("Not enough dates for train/val/test split")

    n = len(dates)

    train_end = int(n * 0.70)
    val_end = int(n * 0.85)

    val_start = dates[train_end]
    test_start = dates[val_end]

    print(f"Validation starts: {val_start}")
    print(f"Testing starts: {test_start}")

    train = features.filter(
        F.to_date("target_timestamp") < F.lit(val_start)
    )

    validation = features.filter(
        (F.to_date("target_timestamp") >= F.lit(val_start))
        & (F.to_date("target_timestamp") < F.lit(test_start))
    )

    test = features.filter(
        F.to_date("target_timestamp") >= F.lit(test_start)
    )
    
    for name, df in [
        ("Train", train),
        ("Validation", validation),
        ("Test", test)
    ]:
        print(f"\n{name}:")

        df.agg(
            F.count("*").alias("samples"),
            F.min("target_timestamp").alias("start"),
            F.max("target_timestamp").alias("end")
        ).show(truncate=False)
    
    # Assemble numeric features into Spark vectors.
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features",
        handleInvalid="error"
    )

    train = assembler.transform(train).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    validation = assembler.transform(validation).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    test = assembler.transform(test).persist(
        StorageLevel.MEMORY_AND_DISK
    )

    n_train = train.count()
    n_val = validation.count()
    n_test = test.count()

    print(f"Training samples: {n_train}")
    print(f"Validation samples: {n_val}")
    print(f"Testing samples: {n_test}")

    if min(n_train, n_val, n_test) == 0:
        raise ValueError("One or more data splits are empty")

    # Compare a few hyperparameter configurations.
    configurations = [
        {"numTrees": 50, "maxDepth": 8},
        {"numTrees": 100, "maxDepth": 10},
        {"numTrees": 100, "maxDepth": 12}
    ]

    best_model = None
    best_config = None
    best_rmse = float("inf")

    for config in configurations:
        rf = RandomForestRegressor(
            featuresCol="features",
            labelCol="label",
            numTrees=config["numTrees"],
            maxDepth=config["maxDepth"],
            minInstancesPerNode=5,
            seed=42
        )

        start = time.perf_counter()
        model = rf.fit(train)
        training_seconds = time.perf_counter() - start

        val_predictions = model.transform(validation)
        metrics = evaluate(val_predictions)

        print(
            f"Config: {config} | "
            f"MAE: {metrics['mae']:.4f} | "
            f"RMSE: {metrics['rmse']:.4f} | "
            f"Training: {training_seconds:.2f}s"
        )

        if metrics["rmse"] < best_rmse:
            best_rmse = metrics["rmse"]
            best_model = model
            best_config = config

    print(f"\nBest parameters: {best_config}")

    # Evaluate only the selected model on test data.
    predictions = best_model.transform(test).persist(
        StorageLevel.MEMORY_AND_DISK
    )

    test_metrics = evaluate(predictions)

    print("\n=== RANDOM FOREST TEST RESULTS ===")
    print(f"MAE:  {test_metrics['mae']:.4f} kWh")
    print(f"RMSE: {test_metrics['rmse']:.4f} kWh")
    print(f"Samples: {test_metrics['count']}")

    # Evaluate seasonal naive on identical test rows.
    naive_day = evaluate(
        test.withColumn(
            "prediction",
            F.col("same_hour_yesterday")
        )
    )
    naive_week = evaluate(
        test.withColumn(
            "prediction",
            F.col("same_hour_last_week")
        )
    )

    print("\n=== SEASONAL NAIVE COMPARISON ===")
    print("Previous day:", naive_day)
    print("Previous week:", naive_week)

    # Save the trained model.
    best_model.write().overwrite().save(
        args.output + "/model"
    )

    # Save predictions for the solar simulation.
    output_predictions = (
        predictions
        .select(
            "building_id",
            "forecast_issued_at",
            "target_timestamp",
            "horizon",
            F.col("label").alias("actual_kwh"),
            F.col("prediction").alias("predicted_kwh")
        )
        .withColumn("model", F.lit("random_forest"))
    )

    output_predictions.write.mode("overwrite").parquet(
        args.output + "/predictions"
    )

    # Print feature importances.
    print("\n=== FEATURE IMPORTANCE ===")

    importance = sorted(
        zip(feature_cols, best_model.featureImportances),
        key=lambda x: x[1],
        reverse=True
    )

    for feature, score in importance:
        print(f"{feature:25s}: {score:.4f}")

    predictions.unpersist()
    train.unpersist()
    validation.unpersist()
    test.unpersist()

    spark.stop()


if __name__ == "__main__":
    main()
