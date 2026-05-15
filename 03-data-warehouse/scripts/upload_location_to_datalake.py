import argparse
import os
import sys

import pandas as pd
import urllib

from dotenv import load_dotenv
load_dotenv()


BASE_URL = "https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/04-analytics-engineering/taxi_rides_ny/seeds/taxi_zone_lookup.csv?raw=true"
GCS_ROOT_DIR = "taxi-ny"
GCS_BLOB_PREFIX = f"{GCS_ROOT_DIR}/zones"
CHUNK_SIZE = 5 * 1024 * 1024  # 5 MB
DOWNLOAD_CSV = os.path.join("..", "DOWNLOAD", "CSV")


def download_file():
    os.makedirs(DOWNLOAD_CSV, exist_ok=True)
    file_path = os.path.join(DOWNLOAD_CSV, "taxi_zone_lookup.csv")

    if os.path.exists(file_path):
        print(f"File already exists: {file_path}. Skipping download.")
        return file_path

    try:
        print(f"Downloading {BASE_URL}...")
        urllib.request.urlretrieve(BASE_URL, file_path)
        return file_path
    except Exception as e:
        print(f"Failed to download {BASE_URL}: {e}")
        return None

def convert_csv_to_parquet(csv_file_path):
    dtypes = {
        "locationid": "int",
        "borough": "string",
        "zone": "string",
        "service_zone": "string"
    }
    try:
        file_name = os.path.basename(csv_file_path).replace(".csv", ".parquet")
        parquet_path = os.path.join('..', 'parquet', file_name)
        os.makedirs(os.path.dirname(parquet_path), exist_ok=True)

        # Apply dtypes only for columns that exist in this file.
        df_sample = pd.read_csv(csv_file_path, nrows=0)
        filtered_dtypes = {k: v for k, v in dtypes.items() if k in df_sample.columns}

        df = pd.read_csv(
            csv_file_path,
            dtype=filtered_dtypes,
            keep_default_na=False,
        )
        df.to_parquet(parquet_path, engine="pyarrow")

        return parquet_path
    
    except Exception as e:
        print(f"Failed to convert {csv_file_path} to Parquet: {e}")
        sys.exit(1)

def connect_gcp_bucket():
    from google.cloud import storage

    bucket_name = os.environ.get("GCP_GCS_BUCKET")
    credentials_file = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

    if not bucket_name:
        print("Missing GCP_GCS_BUCKET in environment variables.")
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

    parquet_file_path = convert_csv_to_parquet(file_path)
    if not parquet_file_path:
        return False

    parquet_file = os.path.basename(parquet_file_path)
    blob_name = f"{GCS_BLOB_PREFIX}/{parquet_file}"
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    try:
        print(f"Uploading {parquet_file} to gs://{bucket_name}/{blob_name} ...")
        blob.upload_from_filename(parquet_file_path)
        print(f"Uploaded: gs://{bucket_name}/{blob_name}")
        return True
    except Exception as e:
        print(f"Failed to upload {parquet_file} to GCS: {e}")
        return False

def upload_to_duckdb(parquet_file_path, con):

    table_name = "taxi_zones"

    try:
        print(f"Inserting {os.path.basename(parquet_file_path)} into DuckDB table '{table_name}' ...")
        con.sql(f"DROP TABLE IF EXISTS {table_name}")
        con.sql(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{parquet_file_path.replace(os.sep, '/')}')")
        count = con.sql(f"SELECT count(*) FROM {table_name}").fetchone()[0]
        print(f"Inserted into '{table_name}' — total rows: {count}")
        return True
    except Exception as e:
        print(f"Failed to insert {os.path.basename(parquet_file_path)} into DuckDB: {e}")
        return False


if __name__ == "__main__":
    # Arguments
    parser = argparse.ArgumentParser(description="Download taxi CSV, convert to Parquet, upload to GCS or DuckDB.")
    parser.add_argument("--target", choices=["gcs", "duckdb"], default="duckdb",
                        help="Upload target: 'gcs' for Google Cloud Storage, 'duckdb' for local DuckDB (default: duckdb)")
    args = parser.parse_args()

    # Download csv files in parallel
    file_path = download_file()
    parquet_file_path = convert_csv_to_parquet(file_path)


    if args.target == "gcs":
        print("Uploading to Google Cloud Storage...")
        bucket_name, bucket = connect_gcp_bucket()
        results = upload_to_gcs(file_path, bucket_name, bucket)
    else:
        print("Uploading to DuckDB...")
        import duckdb
        DUCKDB_FILE = os.environ.get("DUCKDB_FILE", "taxi.duckdb")
        con = duckdb.connect(DUCKDB_FILE)
        results = upload_to_duckdb(parquet_file_path, con)
        con.close()

    if not results:
        print("Some files failed during conversion/upload.")
        sys.exit(1)

    print("All files processed and verified.")