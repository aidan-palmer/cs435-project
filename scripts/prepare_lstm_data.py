
import argparse
import json
import time

if __package__:
    from .runtime_evidence import save_runtime
else:
    from runtime_evidence import save_runtime

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window


INPUT_HOURS = 168
OUTPUT_HOURS = 24


def prepare_sequences(hourly):
    # Use complete, valid hourly records.
    df = (
        hourly.select(
            F.col("building_id").cast("string").alias("building_id"),
            F.col("timestamp"),
            F.col("demand_kwh").cast("double").alias("demand"),
            F.col("temperature_c").cast("double").alias("temperature")
        )
        .filter(
            F.col("building_id").isNotNull()
            & F.col("timestamp").isNotNull()
            & F.col("demand").isNotNull()
            & (~F.isnan("demand"))
            & (F.col("demand") >= 0)
            & F.col("temperature").isNotNull()
            & (~F.isnan("temperature"))
        )
    )

    # Require unique building-hour observations.
    duplicate = (
        df.groupBy("building_id", "timestamp")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )

    if duplicate:
        raise ValueError(
            "Duplicate hourly timestamps found. "
            "Fix hourly aggregation before generating sequences."
        )

    # Each building is processed independently.
    w = (
        Window.partitionBy("building_id")
        .orderBy("timestamp")
    )

    history_window = w.rowsBetween(-167, 0)
    future_window = w.rowsBetween(1, 24)

    # Create arrays of chronological observations.
    df = (
        df
        .withColumn(
            "history",
            F.sort_array(
                F.collect_list(
                    F.struct(
                        "timestamp",
                        "demand",
                        "temperature"
                    )
                ).over(history_window)
            )
        )
        .withColumn(
            "future",
            F.sort_array(
                F.collect_list(
                    F.struct("timestamp", "demand")
                ).over(future_window)
            )
        )
    )

    # Issue exactly one 24-hour forecast per day.
    df = df.filter(F.hour("timestamp") == 23)

    # Require complete historical and future windows.
    df = df.filter(
        (F.size("history") == INPUT_HOURS)
        & (F.size("future") == OUTPUT_HOURS)
        & (
            F.element_at("history", 1).getField("timestamp")
            == F.expr("timestamp - INTERVAL 167 HOURS")
        )
        & (
            F.element_at("future", -1).getField("timestamp")
            == F.expr("timestamp + INTERVAL 24 HOURS")
        )
    )

    # Extract numeric arrays.
    df = (
        df
        .withColumn(
            "demand_history",
            F.expr("transform(history, x -> x.demand)")
        )
        .withColumn(
            "temperature_history",
            F.expr("transform(history, x -> x.temperature)")
        )
        .withColumn(
            "targets",
            F.expr("transform(future, x -> x.demand)")
        )
        .withColumnRenamed("timestamp", "forecast_issued_at")
        .withColumn(
            "target_date",
            F.to_date(
                F.expr("forecast_issued_at + INTERVAL 1 HOUR")
            )
        )
    )

    return df.select(
        "building_id",
        "forecast_issued_at",
        "target_date",
        "demand_history",
        "temperature_history",
        "targets"
    )


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("input", help="Hourly HDFS Parquet path")
    parser.add_argument("output", help="Output HDFS Parquet path")
    parser.add_argument("--val-start", required=True)
    parser.add_argument("--test-start", required=True)
    parser.add_argument("--runtime-output")
    '''
    Use the same start dates as the random forest! 
    '''
    
    args = parser.parse_args()

    if args.val_start >= args.test_start:
        raise ValueError(
            "--val-start must be earlier than --test-start"
        )

    spark = (
        SparkSession.builder
        .appName("SolarOpt LSTM Sequence Generation")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    started=time.perf_counter()
    hourly = spark.read.parquet(args.input)
    sequences = prepare_sequences(hourly)

    # Chronological split by target day.
    sequences = sequences.withColumn(
        "split",
        F.when(
            F.col("target_date") < F.lit(args.val_start),
            "train"
        )
        .when(
            F.col("target_date") < F.lit(args.test_start),
            "validation"
        )
        .otherwise("test")
    )

    sequences=sequences.cache()
    counts={row["split"]:row["count"] for row in sequences.groupBy("split").count().collect()}
    # One row per building/day, not one row per target hour.
    sequences.groupBy("split").count().show()

    (
        sequences
        .write
        .mode("errorifexists")
        .partitionBy("split")
        .parquet(args.output)
    )

    metadata=dict(input=args.input,output=args.output,val_start=args.val_start,
                  test_start=args.test_start,timestamp_clock='UTC',input_hours=168,target_hours=24,
                  split_counts=counts,issue_label='Start of last observed hour; available one hour later')
    target=spark._jvm.org.apache.hadoop.fs.Path(args.output.rstrip('/')+'/_sequence_metadata.json')
    fs=target.getFileSystem(spark._jsc.hadoopConfiguration())
    stream=fs.create(target,False)
    stream.write(bytearray(json.dumps(metadata,indent=2).encode('utf-8')))
    stream.close()
    print("Saved LSTM sequences to:", args.output)

    if args.runtime_output:
        save_runtime(args.runtime_output,dict(processing_seconds=time.perf_counter()-started),metadata,spark)
    sequences.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
