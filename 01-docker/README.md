# Dockerizing the Pipeline
### Lightweight pipeline – first hands-on with UV and Docker.

> Ingest data from a CSV file into a PostgreSQL database.

**Labs from ZoomCamp**
https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/01-docker-terraform/docker-sql/03-dockerizing-pipeline.md

**Data sources:**

https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/yellow  *[from 2019-01 to 2021-07]*

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## Project Structure

- `scripts/taxi_ingest.py` - Ingestion script that loads CSV data into PostgreSQL
- `pyproject.toml` - Project dependencies managed by UV
- `uv.lock` - Locked dependencies for reproducible builds
- `Dockerfile` - Container image with Python 3.13 and UV
- `docker-compose.yaml` - PostgreSQL and pgAdmin services
- `.python-version` - Python version (3.13)

## About UV

This project uses [UV](https://github.com/astral-sh/uv), a fast Python package installer and resolver. UV provides:
- Faster dependency resolution than pip
- Reproducible builds with lock files
- Built-in virtual environment management

## Running the Ingestion Script with Docker Compose

**Navigate to the project directory:**

```bash
cd .\01-docker\
```

**Build the container image:**

```bash
docker build -t taxi_ingest:v001 .
```

**Start services with Docker Compose:**

```bash
docker-compose up
```

This starts:
- PostgreSQL database on port 5432 (db: `ny_taxi`, user/pass: `root/root`)
- pgAdmin on port 8085 (email: `admin@admin.com`, pass: `root`)

**Check the Docker network:**

```bash
docker network ls
```

*Network name: `01-docker_default` (based on directory name)*

**Run the ingestion script:**

```bash
docker run -it --rm --network=01-docker_default taxi_ingest:v001 --pg_host=pgdatabase
```

**Available command-line arguments:**

```bash
docker run -it --rm --network=01-docker_default taxi_ingest:v001 --help
```

**Arguments:**
- `--period` - Data period (default: 2021-01)
- `--pg_user` - PostgreSQL user (default: root)
- `--pg_pass` - PostgreSQL password (default: root)
- `--pg_host` - PostgreSQL host (default: localhost, use `pgdatabase` for Docker)
- `--pg_port` - PostgreSQL port (default: 5432)
- `--pg_db` - PostgreSQL database (default: ny_taxi)

**Example with custom period:**

```bash
docker run -it --rm --network=01-docker_default taxi_ingest:v001 \
  --pg_host=pgdatabase \
  --period=2021-02
```

## View Results with pgAdmin

**Access pgAdmin:**

```
http://localhost:8085
```

**Login credentials:**
- Email: `admin@admin.com`
- Password: `root`

**Register the database:**
1. Right-click "Servers" → Register → Server
2. General tab - Name: `ny_taxi` (or any name)
3. Connection tab:
   - Host: `pgdatabase`
   - Port: `5432`
   - Database: `ny_taxi`
   - Username: `root`
   - Password: `root`

**View the data:**
- Navigate to: Servers → ny_taxi → Databases → ny_taxi → Schemas → public → Tables
- Table name format: `yellow_taxi_YYYY_MM` (e.g., `yellow_taxi_2021_01`)

## Stop and Clean Up

**Stop services:**

```bash
docker-compose down
```

**Stop and remove volumes (deletes all data):**

```bash
docker-compose down -v
```

**Remove the image:**

```bash
docker rmi taxi_ingest:v001
```

**For deeper cleanup, see:**

https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/01-docker-terraform/docker-sql/11-cleanup.md