import argparse
import time
import json
from datetime import date

if __package__:
    from .runtime_evidence import save_runtime
    from .housing_features import join_housing
else:
    from runtime_evidence import save_runtime
    from housing_features import join_housing

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


def write_json(spark,path,value):
    target=spark._jvm.org.apache.hadoop.fs.Path(path)
    fs=target.getFileSystem(spark._jsc.hadoopConfiguration())
    stream=fs.create(target,False)
    stream.write(bytearray(json.dumps(value,indent=2).encode('utf-8')))
    stream.close()


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("input", help="Hourly Parquet path")
    parser.add_argument("output", help="Output directory")
    parser.add_argument(
        "--use-temperature",
        action="store_true",
        help="Include historical temperature features"
    )
    parser.add_argument('--val-start', help='Explicit first validation UTC date')
    parser.add_argument('--test-start', help='Explicit first test UTC date')
    parser.add_argument('--runtime-output', help='New local runtime/configuration JSON')
    parser.add_argument('--prepared-features',help='Reuse the same materialized full-cohort feature keys')
    parser.add_argument('--use-housing',action='store_true')
    parser.add_argument('--metadata',help='Baseline housing metadata CSV')
    parser.add_argument('--num-trees',type=int,help='Fixed configuration for scaling runs')
    parser.add_argument('--max-depth',type=int,help='Fixed configuration for scaling runs')
    args = parser.parse_args()
    if args.use_housing and not args.metadata:parser.error('--use-housing requires --metadata')
    if bool(args.num_trees)!=bool(args.max_depth):parser.error('Supply both fixed RF settings')
    if bool(args.val_start) != bool(args.test_start):
        parser.error('Supply both --val-start and --test-start')
    if args.val_start and date.fromisoformat(args.val_start) >= date.fromisoformat(args.test_start):
        parser.error('--val-start must precede --test-start')
    phases = {}
    processing_start = time.perf_counter()
    
    if args.use_temperature:
        global feature_cols
        feature_cols = BASE_FEATURE_COLS + TEMP_FEATURE_COLS

    spark = (
        SparkSession.builder
        .appName("SolarOpt Random Forest")
        .getOrCreate()
    )

    spark.conf.set('spark.sql.session.timeZone','UTC')

    # Read hourly data.
    hourly = spark.read.parquet(args.input)

    processing_start=time.perf_counter()
    # Generate features and labels.
    features = spark.read.parquet(args.prepared_features) if args.prepared_features else build_features(hourly)
    housing_inventory=None
    if args.use_housing:
        features,housing_columns,housing_inventory=join_housing(features,args.metadata)
        feature_cols.extend(housing_columns)

    # Save features so we can reuse them later.
    features.write.mode("errorifexists").parquet(
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

    val_start = date.fromisoformat(args.val_start) if args.val_start else dates[train_end]
    test_start = date.fromisoformat(args.test_start) if args.test_start else dates[val_end]

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

    train = assembler.transform(train).select('features','label').persist(
        StorageLevel.MEMORY_AND_DISK
    )
    retained=['building_id','forecast_issued_at','target_timestamp','horizon','features','label','same_hour_yesterday','same_hour_last_week']
    validation = assembler.transform(validation).select(*retained).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    test = assembler.transform(test).select(*retained).persist(
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

    phases['processing_seconds'] = time.perf_counter() - processing_start
    tuning_start = time.perf_counter()
    trial_timings = []

    # Compare a few hyperparameter configurations.
    configurations = [
        {"numTrees": 50, "maxDepth": 8},
        {"numTrees": 100, "maxDepth": 10},
        {"numTrees": 100, "maxDepth": 12}
    ]

    if args.num_trees:
        configurations=[dict(numTrees=args.num_trees,maxDepth=args.max_depth)]

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

        trial_timings.append(dict(config=config, training_seconds=training_seconds))
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

    phases['tuning_training_and_validation_seconds'] = time.perf_counter() - tuning_start
    phases['selected_model_training_seconds'] = next(t['training_seconds'] for t in trial_timings if t['config']==best_config)
    inference_start = time.perf_counter()

    # Evaluate only the selected model on test data.
    predictions = best_model.transform(test).persist(
        StorageLevel.MEMORY_AND_DISK
    )

    # The aggregate forces Spark to execute predictions inside this timer.
    predictions.agg(F.sum('prediction').alias('checksum')).first()
    phases['prediction_materialization_seconds']=time.perf_counter()-inference_start
    test_metrics = evaluate(predictions)
    phases['prediction_and_test_metrics_seconds'] = time.perf_counter() - inference_start

    print("\n=== RANDOM FOREST TEST RESULTS ===")
    print(f"MAE:  {test_metrics['mae']:.4f} kWh")
    print(f"RMSE: {test_metrics['rmse']:.4f} kWh")
    print(f"Samples: {test_metrics['count']}")

    # Preserve exact selected-model validation forecasts for pre-decision planning.
    validation_start=time.perf_counter()
    validation_output=best_model.transform(validation).select(
        'building_id','forecast_issued_at','target_timestamp','horizon',
        F.col('label').alias('actual_kwh'),F.col('prediction').alias('predicted_kwh'))
    validation_output.write.mode('errorifexists').parquet(args.output+'/validation_predictions')
    phases['validation_prediction_export_seconds']=time.perf_counter()-validation_start
    write_json(spark,args.output+'/metrics.json',dict(test_metrics,model='rf_housing' if args.use_housing else ('rf_temperature' if args.use_temperature else 'rf'),best_config=best_config,val_start=str(val_start),test_start=str(test_start)))
    if housing_inventory:write_json(spark,args.output+'/housing_features.json',housing_inventory)

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
    best_model.write().save(
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
        .withColumn("model", F.lit("random_forest_housing" if args.use_housing else ("random_forest_temperature" if args.use_temperature else "random_forest")))
        .withColumn("forecast_available_at", F.expr("forecast_issued_at + INTERVAL 1 HOUR"))
    )

    export_start = time.perf_counter()
    output_predictions.write.mode("errorifexists").parquet(
        args.output + "/predictions"
    )

    phases['prediction_export_seconds'] = time.perf_counter() - export_start
    if args.runtime_output:
        save_runtime(args.runtime_output, phases,
            dict(input=args.input,output=args.output,use_temperature=args.use_temperature,
                 use_housing=args.use_housing,housing_inventory=housing_inventory,
                 val_start=str(val_start),test_start=str(test_start),timestamp_clock='UTC',
                 seed=42,features=feature_cols,best_config=best_config,trials=trial_timings,
                 train_rows=n_train,validation_rows=n_val,test_rows=n_test),spark)

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
