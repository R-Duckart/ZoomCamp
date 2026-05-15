# 06-Batch: Spark Processing Pipeline

## Overview

This module demonstrates Spark-based data processing for building revenue reports from taxi data. It reads raw parquet files from a data lake, aggregates them by location and month, and writes the results to either **DuckDB (dev)** or **BigQuery (prod)**.

> **Architecture Note:** In production, if BigQuery already has the gold mart table, BigQuery SQL aggregation may be more cost-effective than Spark. Spark adds value when: (1) data is too large for single SQL engine, (2) complex ML/transformations needed, or (3) reading raw GCS parquet directly to bypass BigQuery ingestion costs.

## Scripts Available

| Script | Purpose | Input | Output |
|--------|---------|-------|--------|
| `00_get_parquets_files` | Download from zoomcamp repo taxi csv files and convert them to parquet | Taxi csv.gz files | Parquet files |
| `01_spark_to_parquet.py` | Convert processed data to parquet format | Taxi parquet files | Parquet files |
| `02_upload_to_db.py` | Upload data directly to database | Data files | DuckDB or BigQuery |
| `03_spark_to_db.py` | **Main pipeline** - reads taxi parquet files, aggregates revenue by zone/month, writes to DB | Parquet files (GCS or local) | DuckDB (dev) or BigQuery (prod) |
| `duckdb_admin.py` | Database management utility | DuckDB file | Database operations |
| `TEST_spark_*.py` | Connection and integration tests | GCP/DuckDB credentials | Test results |

## Setup

### 1. Docker Environment

Build and start Spark cluster:

```bash
docker build -t zoomcamp-spark:latest .
docker compose up -d
```

Verify Spark is running:
```bash
docker ps  # Should show spark-master and spark-worker containers
```

### 2. Configuration

Copy the example env file and configure:

```bash
cp .env.example .env
```

Edit `.env` with your values (see below for details).

### 3. GCP Credentials

Place your GCP service account key at:
```
gcp-key/application_default_credentials.json
```

## Environment Variables

### For Development (DuckDB)

```env
ENV=dev
DATALAKE_SOURCES=/opt/workspace/datalake/raw/taxi
DB_PATH=/opt/workspace/datawarehouse/duckdb/taxi.duckdb
DB_TABLE=revenue_zone_monthly
DB_SCHEMA=reporting
```

### For Production (BigQuery)

```env
ENV=prod
GOOGLE_APPLICATION_CREDENTIALS=/opt/workspace/gcp-key/application_default_credentials.json
GCP_PROJECT_ID=your-project-id
GCP_BUCKET=your-temp-bucket  # Temporary staging bucket for BigQuery connector
GCP_DATASET=your_dataset
DB_TABLE=revenue_zone_monthly
DATALAKE_SOURCES=gs://your-bucket/raw/taxi  # Path to source parquet files
```

## Running the Pipeline

### Development (DuckDB)

**Windows (PowerShell):**
```powershell
# Load environment variables from .env
Get-Content .env | ForEach-Object {
    $key, $value = $_ -split '='
    [Environment]::SetEnvironmentVariable($key, $value)
}

# Run pipeline for year 2019
docker exec -it spark-master `
  /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  /opt/workspace/scripts/03_spark_to_db.py `
  --year 2019
```

**With Debug Output:**
```powershell
docker exec -it spark-master `
  /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  /opt/workspace/scripts/03_spark_to_db.py `
  --year 2019 `
  --debug
```

### Production (BigQuery)

Same command, but ensure `.env` has `ENV=prod` and valid GCP credentials.

```powershell
docker exec -it spark-master `
  /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  /opt/workspace/scripts/03_spark_to_db.py `
  --year 2019
```

## Troubleshooting

### DuckDB Lock Error
```
java.sql.SQLException: IO Error: Could not set lock on file ... Conflicting lock is held
```

**Solution:** DuckDB doesn't support concurrent writes. Ensure:
- No other Spark jobs are running
- No DuckDB client connections (`duckdb_admin.py`, DB viewer) are open
- Wait for previous job to fully complete before restarting
- Script uses single-partition write to minimize conflicts

### BigQuery Cleanup Error
```
ERROR IntermediateDataCleaner: Failed to delete path gs://taxiny_tmp/...
```

**Solution:** Non-fatal. Temporary staging files couldn't be deleted due to GCS permissions. Your data was written successfully. Grant delete permissions on the temp bucket to the service account, or ignore if write succeeded.

### Out of Memory

Increase Spark memory in docker-compose.yaml:
```yaml
environment:
  - SPARK_DRIVER_MEMORY=4g
  - SPARK_EXECUTOR_MEMORY=4g
```

## Tests

Run specific test files to validate your setup and connections:

**Test GCP Connection:**
```powershell
docker exec -it spark-master `
  /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  /opt/workspace/scripts/TEST_spark_to_gcp_connection.py
```

**Test Spark to GCP Write:**
```powershell
docker exec -it spark-master `
  /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  /opt/workspace/scripts/TEST_spark_to_gcp.py
```

**Test Spark to DuckDB Write:**
```powershell
docker exec -it spark-master `
  /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  /opt/workspace/scripts/TEST_spark_duckdb.py
```
