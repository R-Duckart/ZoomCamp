# 03 - Data Warehouse

Data ingestion pipeline for NYC Taxi data to Google Cloud Storage and BigQuery.

## 📋 Description

This module contains Python scripts to:
- Download NYC taxi data (Green & Yellow trips) from GitHub releases
- Download taxi zone data (taxi_zone_lookup)
- Convert CSV files to Parquet format
- Upload Parquet files to Google Cloud Storage (GCS)
- Create External Tables in BigQuery to query the data

## 🏗️ Architecture

```
Web (NYC TLC Data) → Download CSV → Convert to Parquet → Upload to GCS → BigQuery External Tables
```

## 📁 Available Scripts

### 1. `fetch_csv__parquert_to_datalake.py`
Optimized script to download and upload taxi data (Green/Yellow trips).
- Concurrent downloads with ThreadPoolExecutor
- CSV → Parquet conversion with strong column typing
- Batch upload to GCS (taxi-ny/green/, taxi-ny/yellow/)

**Usage:**
```bash
python fetch_csv__parquert_to_datalake.py
```

### 2. `upload_location_to_datalake.py`
Upload taxi zone data (taxi_zone_lookup.csv).
- Downloads from ZoomCamp repository
- Converts to Parquet
- Uploads to GCS (taxi-ny/zones/)

**Usage:**
```bash
python upload_location_to_datalake.py
```

### 3. `web_to_gcs.py` & `web_to_gcs_with_progress_bar.py`
Alternative scripts for uploading taxi data with or without progress bar.

## ⚙️ Configuration

### Prerequisites
- Python 3.14+
- Google Cloud Platform account with a GCS bucket
- Service Account with Storage Object Admin permissions

### Installation

1. **Install dependencies:**
```bash
uv sync
```

2. **Configure environment variables:**

Create a `.env` file at the root of the folder:
```env
GCP_GCS_BUCKET=your-bucket-name
GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account-key.json
```

### Main Dependencies
- `pandas` - data manipulation
- `pyarrow` - Parquet conversion
- `google-cloud-storage` - GCS upload
- `duckdb` - local database (optional)
- `python-dotenv` - environment variable management

## 🚀 Complete Workflow

### Step 1: Upload data to GCS
```bash
# Upload trips data (Green & Yellow)
python fetch_csv__parquert_to_datalake.py

# Upload zones
python upload_location_to_datalake.py
```

### Step 2: Create External Tables in BigQuery

Once the data is uploaded to GCS, create External Tables in BigQuery:

```sql
-- Green trips
CREATE OR REPLACE EXTERNAL TABLE `your-project.taxiny_raw.green_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://your-bucket/taxi-ny/green/*.parquet']
);

-- Yellow trips
CREATE OR REPLACE EXTERNAL TABLE `your-project.taxiny_raw.yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://your-bucket/taxi-ny/yellow/*.parquet']
);

-- Taxi zones
CREATE OR REPLACE EXTERNAL TABLE `your-project.taxiny_raw.taxi_zones`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://your-bucket/taxi-ny/zones/*.parquet']
);
```

> **Note:** External Tables are required for DBT and other tools to access data stored in GCS.

## ⚠️ Known Issues and Solutions

### Issue: "NA"/"N/A" values converted to NULL
**Context:** `pandas.read_csv()` converts "NA" and "N/A" strings to `NaN` by default.  
**Impact:** The `service_zone` column in taxi_zones contained NULL instead of "N/A" for some zones.  
**Solution:** Added `keep_default_na=False` in `upload_location_to_datalake.py`.

```python
df = pd.read_csv(
    csv_file_path,
    dtype=filtered_dtypes,
    keep_default_na=False,  # ← Preserves "NA" and "N/A" as strings
)
```

## 📊 Data Structure

### Green/Yellow Trips
- VendorID, RatecodeID, PULocationID, DOLocationID
- passenger_count, trip_distance, fare_amount
- pickup/dropoff datetime
- payment_type, total_amount, etc.

### Taxi Zones
- locationid (int)
- borough (string)
- zone (string)
- service_zone (string) - contains "N/A" for some zones

## 🔗 Data Sources
- NYC TLC Trip Data: https://github.com/DataTalksClub/nyc-tlc-data/releases
- Taxi Zone Lookup: https://github.com/DataTalksClub/data-engineering-zoomcamp