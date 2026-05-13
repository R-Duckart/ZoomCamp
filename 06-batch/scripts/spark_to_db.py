#!/usr/bin/env python
# coding: utf-8

import argparse
import os
import duckdb
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
def get_spark_session(env: str, debug: bool) -> SparkSession:

    builder = SparkSession.builder.appName(f"taxi-revenue-{env}")

    if env == "prod":
        gcp_key     = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
        project     = os.environ["GCP_PROJECT_ID"]
        temp_bucket = "[GCP_TEMP_BUCKET]" 
        builder = builder \
            .config("spark.hadoop.fs.gs.impl",
                    "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
            .config("spark.hadoop.fs.AbstractFileSystem.gs.impl",
                    "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
            .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
            .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", gcp_key) \
            .config("spark.datasource.bigquery.project", project) \
            .config("spark.datasource.bigquery.temporaryGcsBucket", temp_bucket)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("DEBUG" if debug else "ERROR")
    print(f"[ENV={env}] Spark master: {spark.sparkContext.master}")
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
def write_output(df: pyspark.sql.DataFrame, env: str):
    if env == "prod":
        project  = os.environ["GCP_PROJECT_ID"]
        dataset  = os.environ["GCP_DATASET"]
        table    = os.environ["DB_TABLE"]
        bq_table = f"{project}:{dataset}.{table}"
        print(f"[prod] Writing BigQuery table: {bq_table}")
        df.write \
            .format("bigquery") \
            .option("table", bq_table) \
            .mode("ignore") \
            .save()

    else:
        db_path = os.environ["DB_PATH"]
        table   = os.environ["DB_TABLE"]
        schema  = os.environ.get("DB_SCHEMA", "reporting")
        print(f"[dev] Writing DuckDB table '{table}' to {db_path}")
        print("[dev] Using single-partition JDBC write to avoid DuckDB file lock conflicts")
        
        '''
        spark.write does not support creating tables with complex schemas in DuckDB via JDBC, so we need to prepare the table schema manually before writing.
        '''
        # Extract schema from Spark DataFrame
        schema_str = ", ".join([f"{field.name} {field.dataType.simpleString()}" for field in df.schema.fields])       
        # Prepare DuckDB: create schema and table
        conn = duckdb.connect(db_path)
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        # Create empty table with correct schema from DataFrame
        conn.execute(f"CREATE OR REPLACE TABLE {schema}.{table} ({schema_str})")
        conn.close()

        print(f"[dev] Prepared {schema}.{table}")

        try:
            # Direct Spark write to DuckDB via JDBC
            df.coalesce(1) \
                .write \
                .format("jdbc") \
                .option("url", f"jdbc:duckdb:{db_path}") \
                .option("dbtable", f"{schema}.{table}") \
                .option("driver", "org.duckdb.DuckDBDriver") \
                .option("numPartitions", "1") \
                .mode("append") \
                .save()
        except Exception as e:
            print(f"Error writing to DuckDB: {e}")
            exit(1) 

        print(f"[dev] Wrote {df.count()} rows to {schema}.{table}")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    try:
        env = os.environ.get("ENV", "dev").lower()
        source = os.environ["DATALAKE_SOURCES"]  # → /opt/workspace/datalake/raw/taxi
        args = get_argurments()
        year = args.year
        debug = args.debug

        print(f"Starting taxi revenue pipeline [env={env}, year={year}]")

        spark    = get_spark_session(env, debug)
        process_data(spark, source, year)
        df_rev   = create_df_revenue(spark)
        print(type(df_rev))
        write_output(df_rev, env)

    except Exception as e:
        print(f"Error processing data: {e}")
        exit(1)
    
    print("Done.")


if __name__ == "__main__":
    main()
