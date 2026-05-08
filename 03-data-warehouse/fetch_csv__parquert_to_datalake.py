from itertools import product
import argparse
import pandas as pd
import os
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
load_dotenv()


# Constants
BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi}/{taxi}_tripdata_{year}-{month}.csv.gz"
TAXI = ["green", "yellow"]
YEARS = [2019]
MONTHS = [f"{i:02d}" for i in range(1, 3)]
CHUNK_SIZE = 8 * 1024 * 1024
GCS_ROOT_DIR = "taxi-ny"
MAX_WORKERS = 4
DOWNLOAD_CSV = os.path.join("DOWNLOAD", "CSV")


def download_file(taxi, year, month, format="csv.gz"):
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

def convert_csv_to_parquet(csv_file_path, taxi):
    # read it back into a parquet file
    # enforce types so parquet columns will directly have good types
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
        parquet_path = os.path.join(os.getcwd(), 'parquet', file_name)
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

        return parquet_path
    
    except Exception as e:
        print(f"Failed to convert {csv_file_path} to Parquet: {e}")
        return None

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
    local_file_name = os.path.basename(file_path)
    taxi = local_file_name.split("_tripdata_")[0]

    parquet_file_path = convert_csv_to_parquet(file_path, taxi)
    if not parquet_file_path:
        return False

    parquet_file = os.path.basename(parquet_file_path)
    blob_name = f"{GCS_ROOT_DIR}/{taxi}/{parquet_file}"
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

def upload_to_duckdb(file_path, con):
    local_file_name = os.path.basename(file_path)
    taxi = local_file_name.split("_tripdata_")[0]

    parquet_file_path = convert_csv_to_parquet(file_path, taxi)
    if not parquet_file_path:
        return False

    table_name = f"{taxi}_tripdata"
    try:
        print(f"Inserting {os.path.basename(parquet_file_path)} into DuckDB table '{table_name}' ...")
        con.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name}
            AS SELECT * FROM read_parquet('{parquet_file_path.replace(os.sep, '/')}') WHERE 1=0
        """)
        con.sql(f"INSERT INTO {table_name} SELECT * FROM read_parquet('{parquet_file_path.replace(os.sep, '/')}')")
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
    jobs = list(product(TAXI, YEARS, MONTHS))
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(download_file, taxi, year, month) for taxi, year, month in jobs]
        file_paths = [f.result() for f in futures]

    # Upload to GCS or DuckDB in parallel (GCS) or sequentially (DuckDB)
    file_paths = [f for f in file_paths if f is not None]

    if args.target == "gcs":
        print("Uploading to Google Cloud Storage...")
        bucket_name, bucket = connect_gcp_bucket()
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            results = list(executor.map(lambda fp: upload_to_gcs(fp, bucket_name, bucket), file_paths))
    else:
        print("Uploading to DuckDB...")
        import duckdb
        DUCKDB_FILE = os.environ.get("DUCKDB_FILE", "taxi.duckdb")
        con = duckdb.connect(DUCKDB_FILE)
        results = [upload_to_duckdb(fp, con) for fp in file_paths]
        con.close()

    if not all(results):
        print("Some files failed during conversion/upload.")
        sys.exit(1)

    print("All files processed and verified.")