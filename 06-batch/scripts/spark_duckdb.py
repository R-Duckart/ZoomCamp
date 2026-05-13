#!/usr/bin/env python
# coding: utf-8

import argparse

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

def get_argurments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', required=True)
    parser.add_argument('--debug', default=False, required=False)

    args = parser.parse_args()
    return args

def get_spark_session(debug: bool) -> SparkSession:
    try:
        spark = SparkSession.builder \
                            .master('local') \
                            .appName('test') \
                            .getOrCreate()  
    except Exception as e:
        print(f"Error creating Spark session: {e}")
        exit(1)

    if debug:
        spark.sparkContext.setLogLevel("DEBUG")
    else:
        spark.sparkContext.setLogLevel("ERROR")

    return spark

def get_df_from_duckdb(spark: SparkSession, duckdb_path: str, table_name: str) -> pyspark.sql.DataFrame:
    df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:duckdb:{duckdb_path}") \
        .option("dbtable", table_name) \
        .option("driver", "org.duckdb.jdbc.DuckDBDriver") \
        .load()
    return df

def create_df_revenue(spark: SparkSession, df_trip: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    df_trip.createOrReplaceTempView("trips_data")

    df_result = spark.sql("""
        SELECT 
            -- Reveneue grouping 
            PULocationID AS revenue_zone,
            date_trunc('month', pickup_datetime) AS revenue_month, 
            service_type, 

            -- Revenue calculation 
            SUM(fare_amount) AS revenue_monthly_fare,
            SUM(extra) AS revenue_monthly_extra,
            SUM(mta_tax) AS revenue_monthly_mta_tax,
            SUM(tip_amount) AS revenue_monthly_tip_amount,
            SUM(tolls_amount) AS revenue_monthly_tolls_amount,
            SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
            SUM(total_amount) AS revenue_monthly_total_amount,
            SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

            -- Additional calculations
            AVG(passenger_count) AS avg_montly_passenger_count,
            AVG(trip_distance) AS avg_montly_trip_distance
        FROM
            trips_data
        GROUP BY
            1, 2, 3
    """)

    return df_result

def save_in_db(df_revenue: pyspark.sql.DataFrame, duckdb_path: str, output_table: str):
    try:
        df_revenue.write \
                .format("jdbc") \
                .mode("overwrite") \
                .option("url", f"jdbc:duckdb:{duckdb_path}") \
                .option("dbtable", output_table) \
                .option("driver", "org.duckdb.jdbc.DuckDBDriver") \
                .save()
    except Exception as e:
        print(f"Error saving data to DuckDB: {e}")
        exit(1) 

def __main__():
    try:
        duckdb_path = os.getenv("DB_PATH")
        input_table = os.getenv("INPUT_TABLE")
        output_table = os.getenv("OUTPUT_TABLE")

        args = get_argurments()
        year = args.year

        spark = get_spark_session(args.debug)

        df_trips_data = get_df_from_duckdb(spark, duckdb_path, input_table)
        df_revenue = create_df_revenue(spark, df_trips_data)

        save_in_db(df_revenue, duckdb_path, output_table)

    except Exception as e:
        print(f"Error processing data: {e}")

if __name__ == '__main__':
    __main__()