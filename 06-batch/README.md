In prod, if BigQuery already has the gold mart table, why use Spark to build the report? BigQuery can do that SQL aggregation natively, cheaper and faster, without a Spark cluster
Spark adds value when data is too large for a single SQL engine, or when you need ML / complex transformations that SQL can't express

Spark in prod makes more sense if the source is raw GCS parquet (bypassing BigQuery entirely for cost reasons), or if the report logic becomes too complex for SQL.


## Configuration

Before running the project, copy the `.env.example` file to `.env` and configure your GCP credentials:

```bash
cp .env.example .env
```

Edit `.env` and update the following variables with your actual values:
- `GCP_PROJECT_ID`: Your Google Cloud Project ID
- `GCP_BUCKET`: Your GCS bucket name for temporary BigQuery data
- Place your GCP service account key file in `gcp-key/application_default_credentials.json`

### Use Docker to launch spark master and workers
docker build -t zoomcamp-spark:lastest .
docker compose build 
docker compose up -d

### Command to execute spark script directly from parquet file to a parquet file

**Environment Setup:**

Linux/macOS:
```bash
export WORKSPACE="/opt/workspace"
export DATALAKE_SOURCES = "${WORKSPACE}/datalake/raw/taxi"
export DATAWAREHOUSE ="${WORKSPACE}/datawarehouse/parquet_report"
export YEAR="2019"
```

Windows (PowerShell):
```powershell
$env:WORKSPACE = "/opt/workspace"
$env:DATALAKE_SOURCES = "${env:WORKSPACE}/datalake/raw/taxi"
$env:DATAWAREHOUSE = "${env:WORKSPACE}/datawarehouse/parquet_report"
$env:YEAR = "2019"
```

Windows (CMD):
```cmd
set WORKSPACE=/opt/workspace
set DATALAKE_SOURCES=%WORKSPACE%/datalake/raw/taxi
set DATAWAREHOUSE=%WORKSPACE%/datawarehouse/parquet_report
set YEAR=2019
```

**Run Spark Submit:**

Linux/macOS:
```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/workspace/spark_datalake.py \
  --raw_sources $DATALAKE_SOURCES \
  --output $DATAWAREHOUSE \
  --year $YEAR
```

Windows (PowerShell):
```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  /opt/workspace/spark_datalake.py `
  --raw_sources $env:DATALAKE_SOURCES `
  --output $env:DATAWAREHOUSE `
  --year $env:YEAR
```

Windows (CMD):
```cmd
docker exec -it spark-master /opt/spark/bin/spark-submit ^
  --master spark://spark-master:7077 ^
  /opt/workspace/spark_datalake.py ^
  --raw_sources %DATALAKE_SOURCES% ^
  --output %DATAWAREHOUSE% ^
  --year %YEAR%
```


### Command to execute spark script from and into the a DB

Windows (PowerShell):
```powershell
docker exec -it spark-master `
  /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  /opt/workspace/spark_to_db.py `
  --year <year>
```

If you hit `java.sql.SQLException: IO Error: Could not set lock on file ... Conflicting lock is held`, DuckDB is refusing concurrent writers on the same `.duckdb` file.

Quick checks:
- Ensure no other process is connected to the same DB file (`duckdb_admin.py`, DB client, another Spark run).
- Re-run after the first job is fully stopped.
- Keep Spark JDBC write serialized for DuckDB (the script now uses a single partition write).

Windows (PowerShell):
```powershell
docker exec -it spark-master `
  /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  /opt/workspace/spark_to_db.py `
  --year <year>
```


## GCP

Use the Google Cloud Storage connector for Hadoop/Spark for high-performance

docker exec -it spark-master `
  /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  /opt/workspace/spark_to_gcp.py `
  --year <year>