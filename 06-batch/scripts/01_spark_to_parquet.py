#!/usr/bin/env python
# coding: utf-8

'''
    Before executing this script you have to get the parquet files from the previous step. 
    You can do this by running the following command from the root of the project: uv run 00_get_parquets_files.py
'''

import argparse
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

def get_argurments() -> argparse.Namespace:
    """
    Parse command-line arguments.
    
    Args:
        --raw_sources: Path to raw parquet files (green/yellow)
        --output: Path where to write output parquet files
        --year: Year to process (e.g., 2020)
        --debug: Enable debug logging (default: False)
    
    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--raw_sources', required=True)
    parser.add_argument('--output', required=True)
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

def process_data(spark: SparkSession, input_green: str, input_yellow: str) -> pyspark.sql.DataFrame:
    """
    Read and process green and yellow taxi data.
    
    - Reads parquet files recursively from both sources
    - Standardizes column names (pickup/dropoff datetime)
    - Selects common columns from both datasets
    - Combines both datasets with service_type indicator
    
    Args:
        spark: Spark session
        input_green: Path to green taxi parquet files
        input_yellow: Path to yellow taxi parquet files
    
    Returns:
        pyspark.sql.DataFrame: Combined and processed taxi data
    
    Raises:
        SystemExit: If parquet files cannot be read
    """
    try:
        df_green = spark.read.option("recursiveFileLookup", "true").parquet(input_green)
        df_green = df_green \
            .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
            .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

        df_yellow = spark.read.option("recursiveFileLookup", "true").parquet(input_yellow)
        df_yellow = df_yellow \
            .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
            .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')
    except Exception as e:
        print(f"Error reading parquet files: {e}")
        exit(1)

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
    df_trips_data.registerTempTable('trips_data')

    return df_trips_data

def calculate_revenue(df_trips_data: pyspark.sql.DataFrame, spark: SparkSession) -> pyspark.sql.DataFrame:
    """
    Calculate monthly revenue metrics grouped by location and service type.
    
    - Groups by pickup location (zone), month, and service type
    - Calculates total revenue and fare breakdown
    - Computes average passenger count and trip distance
    
    Args:
        df_trips_data: DataFrame with processed taxi trip data
        spark: Spark session
    
    Returns:
        pyspark.sql.DataFrame: Revenue metrics by zone, month, and service type
    """
    df_trips_data.createOrReplaceTempView('trips_data')

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

def __main__():
    """
    Main orchestration function.
    
    Workflow:
    1. Parse command-line arguments
    2. Initialize Spark session
    3. Load and process green/yellow taxi data
    4. Calculate revenue metrics
    5. Write results to parquet
    """
    try:
        args = get_argurments()

        input_green = os.path.join(args.raw_sources, 'green', args.year)
        input_yellow = os.path.join(args.raw_sources, 'yellow', args.year)
        output = os.path.join(args.output, args.year)

        spark = get_spark_session(args.debug)
        df_trips_data = process_data(spark, input_green, input_yellow)

        df_result = calculate_revenue(df_trips_data, spark)

        df_result.coalesce(1).write.parquet(output, mode='overwrite')

    except Exception as e:
        print(f"Error processing data: {e}")

if __name__ == '__main__':
    __main__()