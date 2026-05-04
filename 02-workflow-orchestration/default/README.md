# Kestra Workflow Orchestration - Default Setup

Default Kestra orchestration setup with PostgreSQL and local storage.

## Prerequisites

- Docker and Docker Compose
- Python 3.13+

## Quick Start

### 1. Start Services

```bash
docker-compose up -d
```

### 2. Access Kestra UI

- **URL**: http://localhost:8080
- **Username**: admin@kestra.io
- **Password**: Admin1234!

### 3. Access pgAdmin (PostgreSQL)

- **URL**: http://localhost:8085
- **Username**: admin@admin.com
- **Password**: root

## Available Workflows

### Basic Examples
- `01_hello_world.yaml` - Simple Hello World workflow
- `02_python.yaml` - Python task execution
- `03_getting_started_data_pipeline.yaml` - Introduction to data pipelines

### Data Pipeline Examples
- `04_postgres_taxi.yaml` - Load taxi data to PostgreSQL
- `05_postgres_taxi_scheduled.yaml` - Scheduled taxi data ingestion

### GCP Integration
- `06_gcp_kv.yaml` - GCP Key Vault setup
- `07_gcp_setup.yaml` - GCP environment configuration
- `08_gcp_taxi.yaml` - Load taxi data to GCP
- `09_gcp_taxi_scheduled.yaml` - Scheduled GCP taxi data ingestion

### AI/RAG Examples
- `10_chat_without_rag.yaml` - Chat workflow without RAG
- `11_chat_with_rag.yaml` - Chat workflow with Retrieval-Augmented Generation

## Accessing Workflows

1. Open Kestra UI at http://localhost:8080
2. Navigate to **Flows** section
3. Flows are automatically loaded from the `flows/default/` directory

## Stopping Services

```bash
docker-compose down
```

## Troubleshooting

- **Can't connect to database?** Ensure PostgreSQL is running: `docker-compose ps`
- **Port conflicts?** Modify ports in `docker-compose.yaml` and update connection strings accordingly
- **Permission denied?** Run docker commands with appropriate permissions or add user to docker group

## Further Reading

- [Kestra Documentation](https://kestra.io/docs)
- [PostgreSQL Docs](https://www.postgresql.org/docs/)
