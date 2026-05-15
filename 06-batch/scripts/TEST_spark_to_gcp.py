#!/usr/bin/env python
# coding: utf-8

import argparse
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Get arguments
# ---------------------------------------------------------------------------
def get_argurments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', required=True)
    parser.add_argument('--debug', action='store_true')

    args = parser.parse_args()
    return args

# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------
def get_spark_session(debug: bool) -> SparkSession:
    # Get GCP configuration from environment variables
    gcp_project_id = os.getenv('GCP_PROJECT_ID')
    gcp_location = os.getenv('GCP_LOCATION', 'europe-west1')
    gcp_bucket = os.getenv('GCP_BUCKET')

    spark = (
        SparkSession.builder
            .master("local") \
            .appName("GCP-Spark-Session") \
            .config("spark.datasource.bigquery.project", gcp_project_id) \
            .getOrCreate()
    )

    spark.conf.set("location", gcp_location)
    spark.sparkContext.setLogLevel("DEBUG" if debug else "ERROR")

    spark.conf.set('spark.datasource.bigquery.temporaryGcsBucket', gcp_bucket)

    print(f"Spark master: {spark.sparkContext.master}")
    return spark

# ---------------------------------------------------------------------------
# Read input
# ---------------------------------------------------------------------------
def process_data(spark: SparkSession, source: str, year: str):
    try:
        df_green = spark.read.option("recursiveFileLookup", "true").parquet(os.path.join(source, "green", year))
        df_green = df_green \
            .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
            .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

        df_yellow = spark.read.option("recursiveFileLookup", "true").parquet(os.path.join(source, "yellow", year))
        df_yellow = df_yellow \
            .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
            .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')
    except Exception as e:
        print(f"Error reading parquet files: {e}")
        exit(1)

    print(f"Green taxi records: {df_green.count()}")
    print(f"Yellow taxi records: {df_yellow.count()}")

    common_colums = [
        'VendorID',
        'pickup_datetime',
        'dropoff_datetime',
        'store_and_fwd_flag',
        'RatecodeID',
        'PULocationID',
        'DOLocationID',
        'passenger_count',
        'trip_distance',
        'fare_amount',
        'extra',
        'mta_tax',
        'tip_amount',
        'tolls_amount',
        'improvement_surcharge',
        'total_amount',
        'payment_type',
        'congestion_surcharge'
    ]

    df_green_sel = df_green \
        .select(common_colums) \
        .withColumn('service_type', F.lit('green'))

    df_yellow_sel = df_yellow \
        .select(common_colums) \
        .withColumn('service_type', F.lit('yellow'))

    df_trips_data = df_green_sel.unionAll(df_yellow_sel)
    df_trips_data.createOrReplaceTempView("trips_data")

# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------
def create_df_revenue(spark: SparkSession) -> pyspark.sql.DataFrame:

    sql_classic_df = spark.sql("""
        SELECT
            PULocationID                        AS revenue_zone,
            date_trunc('month', pickup_datetime) AS revenue_month,
            service_type,
            SUM(fare_amount)                    AS revenue_monthly_fare,
            SUM(extra)                          AS revenue_monthly_extra,
            SUM(mta_tax)                        AS revenue_monthly_mta_tax,
            SUM(tip_amount)                     AS revenue_monthly_tip_amount,
            SUM(tolls_amount)                   AS revenue_monthly_tolls_amount,
            SUM(improvement_surcharge)          AS revenue_monthly_improvement_surcharge,
            SUM(total_amount)                   AS revenue_monthly_total_amount,
            SUM(congestion_surcharge)           AS revenue_monthly_congestion_surcharge,
            AVG(passenger_count)                AS avg_montly_passenger_count,
            AVG(trip_distance)                  AS avg_montly_trip_distance
        FROM trips_data
        GROUP BY 1, 2, 3
    """)
    
    return sql_classic_df

# ---------------------------------------------------------------------------
# Write output
# ---------------------------------------------------------------------------
def write_output(df: pyspark.sql.DataFrame):
    project  = os.environ["GCP_PROJECT_ID"]
    dataset  = os.environ["GCP_DATASET"]
    table    = os.environ["DB_TABLE"]
    bq_table = f"{project}:{dataset}.{table}"
    print(f"[prod] Writing BigQuery table: {bq_table}")
    df.write.format("bigquery") \
        .option("table", f"{project}.{dataset}.{table}") \
        .option("writeMethod", "direct") \ # Use direct write method for better performance - no temporary bucket needed
        .mode("overwrite") \
        .save()


    print(f"[dev] Wrote {df.count()} rows to {table}")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    try:
        args = get_argurments()
        source = os.environ["DATALAKE_SOURCES"]
        year = args.year
        debug = args.debug

        spark = get_spark_session(debug)
        process_data(spark, source, year)
        df_rev  = create_df_revenue(spark)
        write_output(df_rev)


    except Exception as e:
        print(f"Error processing data: {e}")
        exit(1)
    
    print("Done.")


if __name__ == "__main__":
    main()
