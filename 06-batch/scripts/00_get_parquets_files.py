#!/usr/bin/env python
# coding: utf-8

"""
Taxi Data Downloader and Parquet Converter

Downloads NYC taxi trip data (green and yellow) from the DataTalksClub GitHub repository,
converts CSV.GZ files to Parquet format, and organizes them in a data lake structure.

Features:
    - Parallel downloads using ThreadPoolExecutor for efficiency
    - Automatic CSV to Parquet conversion with proper type enforcement
    - Organized data lake structure: taxi/{color}/{year}/{month}/
    - Skips already downloaded files to avoid redundant work
    - Handles both green and yellow taxi data with different schemas

Usage:
    # Download and convert all configured years and months
    uv run scripts/00_get_parquets_files.py

Configuration:
    Edit the constants in this file to customize:
    - YEARS: List of years to download (e.g., [2019, 2020])
    - MONTHS: List of months to download (default: all 12 months)
    - MAX_WORKERS: Number of parallel download threads

Prerequisites:
    - Internet connection to download from GitHub
    - Sufficient disk space for CSV and Parquet files
    - Required packages: pandas, pyarrow, python-dotenv

Output Structure:
    datalake/raw/taxi/
        green/
            2019/
                01/
                    green_tripdata_2019-01.parquet
                02/
                    green_tripdata_2019-02.parquet
        yellow/
            2019/
                01/
                    yellow_tripdata_2019-01.parquet
"""

from itertools import product
import pandas as pd
import os
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
load_dotenv()


# Constants
BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi}/{taxi}_tripdata_{year}-{month}.csv.gz"
TAXI = ["green", "yellow"]
# set here all years you want to download and process, e.g. [2019, 2020]
YEARS = [2019, 2020] 
# set here all months you want to download and process, e.g. range(1, 13) for all months
MONTHS = [f"{i:02d}" for i in range(1, 13)]
CHUNK_SIZE = 8 * 1024 * 1024
GCS_ROOT_DIR = "taxi-ny"
MAX_WORKERS = 4
DOWNLOAD_CSV = os.path.join("..", "DOWNLOAD", "CSV")
DATALAKE_PATH = os.path.join("..", "datalake", "raw", "taxi")


def download_file(taxi: str, year: int, month: str, format="csv.gz") -> str:
    os.makedirs(DOWNLOAD_CSV, exist_ok=True)
    url = f"{BASE_URL.format(taxi=taxi, year=year, month=month)}"
    file_path = os.path.join(DOWNLOAD_CSV, f"{taxi}_tripdata_{year}-{month}.{format}")

    if os.path.exists(file_path):
        print(f"File already exists: {file_path}. Skipping download.")
        return file_path

    try:
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, file_path)
        return file_path
    
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None

def convert_csv_to_parquet(csv_file_path: str, taxi: str, year: int, month: str) -> str:

    # Enforce types so parquet columns will directly have good types
    # (as we did in module 1 in ingest.py script)
    dtypes = {
        "VendorID": "Int64",
        "RatecodeID": "Int64",
        "PULocationID": "Int64",
        "DOLocationID": "Int64",
        "passenger_count": "Int64",
        "payment_type": "Int64",
        "trip_type": "Int64",  # only in green but ignored if missing column
        "store_and_fwd_flag": "string",
        "trip_distance": "float64",
        "fare_amount": "float64",
        "extra": "float64",
        "mta_tax": "float64",
        "tip_amount": "float64",
        "tolls_amount": "float64",
        "ehailfee": "float64",  # only in green but ignored if missing column
        "improvement_surcharge": "float64",
        "total_amount": "float64",
        "congestion_surcharge": "float64",
    }
    try:
        if taxi == "yellow":
            parse_dates = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
        else:
            parse_dates = ["lpep_pickup_datetime", "lpep_dropoff_datetime"]

        file_name = os.path.basename(csv_file_path).replace(".csv.gz", ".parquet")
        parquet_path = os.path.join(DATALAKE_PATH, taxi, str(year), month, file_name)
        os.makedirs(os.path.dirname(parquet_path), exist_ok=True)

        # Apply dtypes and parse_dates only for columns that exist in this file.
        source_columns = pd.read_csv(csv_file_path, nrows=0, compression="gzip").columns
        filtered_dtypes = {k: v for k, v in dtypes.items() if k in source_columns}
        filtered_parse_dates = [col for col in parse_dates if col in source_columns]

        df = pd.read_csv(
            csv_file_path,
            dtype=filtered_dtypes,
            parse_dates=filtered_parse_dates,
            compression="gzip",
        )
        df.to_parquet(parquet_path, engine="pyarrow")

        print(f"Converted {csv_file_path} to {parquet_path}")
        return parquet_path
    
    except Exception as e:
        print(f"Failed to convert {csv_file_path} to Parquet: {e}")
        return None

def process_taxi_data(taxi: str, year: int, month: str) -> str:
    """
    Download and convert a single taxi trip file (end-to-end processing).
    
    Orchestrates the complete workflow:
    1. Downloads CSV.GZ file from GitHub
    2. Converts to Parquet with proper types
    3. Organizes in data lake structure
    
    Args:
        taxi: Taxi type - 'green' or 'yellow'
        year: Year of the data (e.g., 2019)
        month: Month as zero-padded string (e.g., '01')
    
    Returns:
        str: Path to the final Parquet file, or None if processing failed at any step
    """
    csv_file_path = download_file(taxi, year, month)
    if csv_file_path:
        return convert_csv_to_parquet(csv_file_path, taxi, year, month)
    return None

if __name__ == "__main__":
    # Download csv files in parallel
    jobs = list(product(TAXI, YEARS, MONTHS))

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_taxi_data, taxi, year, month) 
                for taxi, year, month in jobs]
        
        # Attendre que tous les tasks se terminent
        result = [f.result() for f in futures]

    print("All files processed and verified.")