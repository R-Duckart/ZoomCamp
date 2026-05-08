## ZoomCamp NY Taxi — dbt Project

### Architecture

```
staging (views)        → intermediate (views)          → marts (tables)
stg_green_tripdata       int_trips_unioned                fct_trips (incremental)
stg_yellow_tripdata      int_trips_clean_and_enrich       dim_zones
stg_taxi_zones                                            fct_monthly_zone_revenue
```

**Strategy**: Incremental materialization for fact tables

---

## Corrections and Adaptations Applied

### Cross-Database Compatibility with dbt Macros

To ensure compatibility across different database engines (DuckDB, PostgreSQL, BigQuery), use dbt's built-in type macros instead of hardcoded types:

| DuckDB/Postgres | BigQuery | dbt Macro |
|----------------|----------|-----------|
| float | float64 | `{{ dbt.type_float() }}` |
| int | int64 | `{{ dbt.type_int() }}` |
| string / varchar | string | `{{ dbt.type_string() }}` |
| timestamp | timestamp | `{{ dbt.type_timestamp() }}` |

**Before** (hardcoded types):
```sql
cast(VendorID as int) as vendor_id,
cast(lpep_pickup_datetime as timestamp()) as pickup_datetime,
```

**After** (cross-database compatible):
```sql
cast(VendorID as {{ dbt.type_int() }}) as vendor_id,
cast(lpep_pickup_datetime as {{ dbt.type_timestamp() }}) as pickup_datetime,
```


---

### Issue #1: Duplicate Rows in Source Parquet Files

**Problem**: Source parquet files contain exact duplicate rows (e.g., 45 identical rows for the same yellow taxi trip).  
**Consequence**: Uniqueness test failure on `trip_id` (surrogate key).  
**Solution**: Added `SELECT DISTINCT` in staging models (`stg_green_tripdata.sql` and `stg_yellow_tripdata.sql`) to deduplicate at the source.

Analysis revealed thousands of duplicates when checking what should be unique combinations:

```sql
SELECT count(*) as count 
FROM `zoomcamp-490614.taxiny_raw.green_tripdata` 
GROUP BY
  vendorid, 
  lpep_pickup_datetime,
  lpep_dropoff_datetime,
  PULocationID,
  DOLocationID,
  store_and_fwd_flag,
  RatecodeID
HAVING count > 1
```

---

### Issue #2: Insufficient Surrogate Key

**Problem**: The surrogate key `trip_id` was based on only 4 columns (`vendor_id`, `pickup_datetime`, `pickup_location_id`, `service_type`).  
**Consequence**: Hash collisions — two different trips could have the same `trip_id`.  
**Solution**: Added `dropoff_datetime` and `dropoff_location_id` to the key (now 6 columns total).

---

### Issue #3: Unexpected Values in Data

**Discovered Issues**:
- **`ratecode_id = 99`**: TLC code for "not available" → added to `accepted_values` and to seed file `ratecode_lookup.csv`
- **`vendor_id = 4`**: Undocumented vendor in yellow_tripdata (128K rows) → added to `accepted_values` and to seed file `vendor_lookup.csv`


---

### Issue #4: BigQuery Type Strictness with `accepted_values` Tests

**Problem**: DuckDB accepts tests like `IN ('1', '2', '3')` for integer columns, but BigQuery strictly requires type matching.

**Error Message**:
```
Database Error in test accepted_values_stg_green_tripdata_payment_type__1__2__3__4__5__6
No matching signature for operator IN for argument types INT64 and {STRING} at [28:23]
```

**Root Cause**: YAML treats `[1, 2, 3]` as integers, but dbt converts them to strings in the generated SQL (`IN ('1', '2', '3')`). DuckDB is lenient and accepts this type mixing; BigQuery does not.

**Solution**: Add `quote: false` to force dbt to generate `IN (1, 2)` without quotes:

```yaml
data_tests:
  - accepted_values:
      arguments:
        values: [1, 2]
        quote: false
```

---

### Issue #5: Custom Schema Naming

**Problem**: By default, dbt prefixes schema names (e.g., `main_staging` instead of `staging`).  
**Solution**: Created a custom macro `generate_schema_name.sql` that returns the schema name without the prefix.

---

### Issue #6: DuckDB Bug with `row_number()` in CTEs

**Problem**: DuckDB 1.5.1 has a bug when `row_number()` is used in a CTE with `SELECT * EXCLUDE (rn)` and type casts.

**Solution**: Use `QUALIFY` clause instead, which is supported natively by both DuckDB and BigQuery:

**Before** (buggy with DuckDB 1.5.1):
```sql
WITH ranked AS (
  SELECT *, row_number() OVER (PARTITION BY ... ORDER BY ...) as rn
  FROM source
)
SELECT * EXCLUDE (rn) FROM ranked WHERE rn = 1
```

**After** (cleaner and compatible):
```sql
SELECT * FROM source
QUALIFY row_number() OVER (PARTITION BY ... ORDER BY ...) = 1
```

This approach is:
- ✅ More concise (no CTE needed)
- ✅ Compatible with both DuckDB and BigQuery
- ✅ Avoids the DuckDB 1.5.1 bug

---

## Useful Commands

### Install Dependencies
```bash
uv add dbt-bigquery    # Add BigQuery adapter
uv add dbt-duckdb      # Add DuckDB adapter
```

### Core dbt Commands
```bash
uv run dbt deps                  # Install dbt packages (e.g., dbt_utils)
uv run dbt seed                  # Load seed files (lookup tables)
uv run dbt run --full-refresh    # Rebuild all models from scratch
uv run dbt test                  # Run all data tests
uv run dbt build --full-refresh  # Run seed + run + test in one command
```

### Development vs Production

**Test connection to production**:
```bash
uv run dbt debug --target prod
```

**Build models in production**:
```bash
uv run dbt build --target prod --full-refresh
```

**Build specific models (e.g., staging only)**:
```bash
uv run dbt build --target prod --full-refresh --select models/staging
```

### Important Notes

⚠️ **Location Configuration**: Make sure to specify the exact BigQuery location in your `profiles.yml` or set it as an environment variable for consistency.

**Example profiles.yml**:
```yaml
zoomcamp_nytaxi:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: taxi.duckdb
    prod:
      type: bigquery
      method: oauth
      project: your-gcp-project-id
      dataset: your_dataset
      location: US  # or EU, asia-northeast1, etc.
```