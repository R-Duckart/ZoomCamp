#!/usr/bin/env python
# coding: utf-8

"""
Parquet Upload Tool - DuckDB & Google Cloud Storage

This script uploads parquet files from the local data lake to either DuckDB (local)
or Google Cloud Storage (GCS) in parallel, with progress tracking.

Features:
    - Parallel uploads using ThreadPoolExecutor (configurable worker threads)
    - Support for multiple targets: DuckDB and Google Cloud Storage (GCS)
    - Recursive directory traversal to find all parquet files
    - Chunked uploads for efficient memory usage
    - Automatic environment variable loading from .env file

Usage:
    # Upload to local DuckDB
    uv run scripts/02_upload_to_db.py --target duckdb
    
    # Upload to Google Cloud Storage
    uv run scripts/02_upload_to_db.py --target gcs

Prerequisites:
    - For DuckDB: Ensure taxi.duckdb database exists
    - For GCS: Set GCP_BUCKET and GOOGLE_APPLICATION_CREDENTIALS in .env file
    
Environment Variables Required (.env):
    - GCP_BUCKET: Your GCS bucket name (for GCS target)
    - GOOGLE_APPLICATION_CREDENTIALS: Path to GCP service account JSON key (for GCS target)

Execution:
    Run this script from the root of the project directory.
"""

import argparse
from ast import pattern
import glob
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
load_dotenv()


# Constants
TAXI = ["green", "yellow"]
PARQUET_DIR = os.path.join("datalake", "raw", "taxi")
CHUNK_SIZE = 8 * 1024 * 1024
MAX_WORKERS = 4


def connect_gcp_bucket():
    """
    Initialize Google Cloud Storage client and connect to the configured bucket.
    
    Validates environment variables and credentials before establishing connection.
    Uses service account authentication from JSON key file.
    
    Environment Variables Required:
        GCP_BUCKET: Name of the target GCS bucket
        GOOGLE_APPLICATION_CREDENTIALS: Path to service account JSON key file
    
    Returns:
        tuple: (bucket_name: str, bucket: google.cloud.storage.Bucket)
    
    Raises:
        SystemExit: If environment variables are missing or credentials are invalid
    
    Note:
        Uses client.bucket() instead of client.get_bucket() to avoid requiring
        storage.buckets.get permission (not included in Storage Object Admin role).
    """
    from google.cloud import storage

    bucket_name = os.environ.get("GCP_BUCKET")
    # Warning: The GOOGLE_APPLICATION_CREDENTIALS environment variable should point to a valid service account JSON key file. 
    # Make sure to set this environment variable before running the script.
    credentials_file = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

    if not bucket_name:
        print("Missing GCP_BUCKET in environment variables.")
        sys.exit(1)
    if not credentials_file or not os.path.isfile(credentials_file):
        print(f"Missing or invalid GOOGLE_APPLICATION_CREDENTIALS: {credentials_file}")
        sys.exit(1)

    try:
        client = storage.Client.from_service_account_json(credentials_file)
        # Use client.bucket() instead of client.get_bucket() to avoid
        # requiring storage.buckets.get permission (not in Storage Object Admin).
        return (bucket_name, client.bucket(bucket_name))

    except Exception as e:
        print(f"Failed to initialize GCS client: {e}")
        sys.exit(1)

def upload_to_gcs(file_path, bucket_name, bucket):
    """
    Upload a single parquet file to Google Cloud Storage with chunked transfer.
    
    Preserves the local directory structure in GCS (e.g., datalake/raw/taxi/green/2019/01/...).
    Uses chunked upload for efficient memory usage with large files.
    
    Args:
        file_path: Local path to the parquet file to upload
        bucket_name: Name of the target GCS bucket (for logging)
        bucket: google.cloud.storage.Bucket instance
    
    Returns:
        bool: True if upload succeeded, False otherwise
    
    Example:
        >>> upload_to_gcs('datalake/raw/taxi/green/2019/01/file.parquet', 'my-bucket', bucket_obj)
        Uploading datalake/raw/taxi/green/2019/01/file.parquet to gs://my-bucket/...
        True
    """

    gcs_path = f"{file_path}".replace("\\", "/")

    blob_name = f"{gcs_path}"
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    try:
        print(f"Uploading {gcs_path} to gs://{bucket_name}/{blob_name} ...")
        blob.upload_from_filename(gcs_path)
        print(f"Uploaded: gs://{bucket_name}/{blob_name}")
        return True
    except Exception as e:
        print(f"Failed to upload {gcs_path} to GCS: {e}")
        return False

def upload_to_duckdb(taxi, con):
    """
    Load all parquet files for a specific taxi type into a DuckDB table.
    
    Creates a table if it doesn't exist and inserts all parquet files matching
    the taxi type using DuckDB's efficient read_parquet() function.
    
    Args:
        taxi: Taxi type - 'green' or 'yellow'
        con: DuckDB connection instance
    
    Returns:
        bool: True if insertion succeeded, False otherwise
    
    Table Structure:
        Creates/appends to table named '{taxi}_tripdata' (e.g., 'green_tripdata')
    
    Example:
        >>> import duckdb
        >>> con = duckdb.connect('taxi.duckdb')
        >>> upload_to_duckdb('green', con)
        Inserting green_tripdata_*.parquet into DuckDB table 'green_tripdata' ...
        Inserted into 'green_tripdata' — total rows: 1234567
        True
    """
    parquet_file = os.path.join(PARQUET_DIR, taxi, "*", "*", "*.parquet")
    table_name = f"{taxi}_tripdata"
    try:
        print(f"Inserting {os.path.basename(parquet_file)} into DuckDB table '{table_name}' ...")

        con.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name}
            AS SELECT * FROM read_parquet('{parquet_file.replace(os.sep, '/')}') WHERE 1=0
        """)

        con.sql(f"INSERT INTO {table_name} SELECT * FROM read_parquet('{parquet_file.replace(os.sep, '/')}')")

        count = con.sql(f"SELECT count(*) FROM {table_name}").fetchone()[0]
        print(f"Inserted into '{table_name}' — total rows: {count}")

        return True
        
    except Exception as e:
        print(f"Failed to insert {os.path.basename(parquet_file)} into DuckDB: {e}")
        return False


if __name__ == "__main__":
    # Arguments
    parser = argparse.ArgumentParser(description="Download taxi CSV, convert to Parquet, upload to GCS or DuckDB.")
    parser.add_argument("--target", choices=["gcs", "duckdb"], default="duckdb",
                        help="Upload target: 'gcs' for Google Cloud Storage, 'duckdb' for local DuckDB (default: duckdb)")
    args = parser.parse_args()

    if args.target == "gcs":
        print("Uploading to Google Cloud Storage...")

        files_to_upload = []
        for taxi in TAXI:
            pattern = f"{PARQUET_DIR}/{taxi}/*/*/*.parquet"
            files_to_upload.extend([f for f in glob.glob(pattern, recursive=True) if os.path.isfile(f)])

        bucket_name, bucket = connect_gcp_bucket()
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            results = [executor.submit(upload_to_gcs, file_path, bucket_name, bucket) for file_path in files_to_upload]
    else:
        print("Uploading to DuckDB...")
        import duckdb
        duckdb_file = os.environ.get("DUCKDB_FILE", "taxi.duckdb")
        duckdb_path = os.path.join("datawarehouse", "duckdb", duckdb_file)
        os.makedirs(os.path.dirname(duckdb_path), exist_ok=True)
        con = duckdb.connect(duckdb_path)

        results = [upload_to_duckdb(taxi, con) for taxi in TAXI]
        con.close()

    if not all(results):
        print("Some files failed during conversion/upload.")
        sys.exit(1)

    print("All files processed and verified.")