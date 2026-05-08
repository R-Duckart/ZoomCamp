# Analytics Engineering with dbt

This project demonstrates analytics engineering using dbt (data build tool) with BigQuery integration.

## Project Setup

### Environment Configuration

This project uses:
- **UV** for Python environment and dependency management
- **`.env` file** for storing BigQuery connection credentials

### Verify Environment Setup

After running the activation script, verify that environment variables are properly loaded:

```powershell
$env:GCP_PROJECT_ID
```

This should display your configured Google Cloud Platform project ID. If it returns empty, the environment variables were not loaded correctly.

## Getting Started

1. Ensure you have UV installed
2. Create a `.env` file with your BigQuery credentials (see `.env.example` if available)
3. Run the activation script:
   ```powershell
   .\activate.ps1
   ```
4. Verify environment variables are loaded
5. Navigate to the dbt project directory and run your dbt commands

## Project Structure

- `activate.ps1` - Custom environment activation script
- `pyproject.toml` - Python dependencies managed by UV
- `.env` - BigQuery connection credentials (not committed to git)
- `zoomcamp_nytaxi/` - dbt project directory
  - `dbt_project.yml` - dbt project configuration
  - `profiles.yml` - dbt connection profiles
  - `models/` - dbt transformation models
  - `macros/` - dbt macros and reusable SQL
  - `seeds/` - Static CSV files to load
  - `tests/` - Data quality tests

## Technologies

- **dbt** - Data transformation and modeling
- **BigQuery** - Cloud data warehouse
- **DuckDB** - Local development database
- **UV** - Fast Python package manager
- **PowerShell** - Environment automation