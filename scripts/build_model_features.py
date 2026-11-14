from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("SolarOpt Feature Engineering")
    .getOrCreate()
)

HDFS_DIR = "/larimer_baseline"


def load_parquets(data_dir=HDFS_DIR):
    # Read all ResStock Parquet files
    loads = spark.read.parquet(f"{data_dir}/*.parquet")

    # Select and rename columns
    df = loads.select(
        F.col("bldg_id").alias("building_id"),
        F.col("timestamp"),
        F.col(
            "`out.electricity.total.energy_consumption`"
        ).alias("demand_kwh"),
        F.col(
            "`out.outdoor_air_dryblub_temp.c`"
        ).alias("temperature_c")
    )
    return df


def build_hourly(df):
    hourly = (
        df.withColumn(
            "hour",
            F.date_trunc("hour", F.col("timestamp"))
        )
        .groupBy("building_id", "hour")
        .agg(
            F.sum("demand_kwh").alias("demand_kwh"),
            F.avg("temperature_c").alias("temperature_c")
        )
        .withColumnRenamed("hour", "timestamp")
    )
    return hourly


def main():
    df = load_parquets()
    hourly = build_hourly(df)
    hourly.printSchema()
    hourly.show(10, truncate=False)
    

if __name__ == "__main__":
    main()
