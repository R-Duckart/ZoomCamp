# Data Engineering ZoomCamp - Learning Project

> **⚠️ Learning Project Notice:** This repository is a hands-on learning project focused on exploring and practicing data engineering concepts. The architectures and implementations are **not production-compliant** and are intentionally simplified for educational purposes.

## Overview

This project is part of the [DataTalks Club Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp), designed to gain practical experience with modern data engineering tools, technologies, and workflows. The primary goal is to **learn by doing** – experimenting with different approaches, comparing cloud platforms, and understanding the trade-offs between various data engineering solutions.

## Project Structure

The repository is organized into modules, each covering a specific aspect of data engineering:

### 📦 01-docker
**Dockerizing the Pipeline**  
- First hands-on experience with containerization
- Ingesting CSV data into PostgreSQL using Docker
- Introduction to UV (fast Python package manager)
- Technologies: Docker, PostgreSQL, pgAdmin, Python

### 🔄 02-workflow-orchestration
**Workflow Orchestration with Kestra**  
- Multiple implementations across different environments:
  - **Default**: Local setup with PostgreSQL
  - **Azure**: Azure-specific workflow configurations
  - **GCP**: Google Cloud Platform integration
- Learning workflow orchestration patterns and cloud platform differences
- Technologies: Kestra, Docker Compose

### 🏢 03-data-warehouse
**Data Warehouse & Data Lake Operations**  
- Working with DuckDB as a lightweight data warehouse
- CSV to Parquet conversion and optimization
- Data lake storage patterns
- Upload workflows to cloud storage
- Technologies: DuckDB, Parquet, Python

### 📊 04-analytics-engineering
**Analytics Engineering with dbt**  
- dbt project for data transformation and modeling
- Connection to BigQuery for cloud-based analytics
- Environment management with UV
- Learning SQL-based transformation workflows
- Technologies: dbt, DuckDB, BigQuery, Python

### 🔧 05-data-platforms
**Data Platform Exploration**  
- Intended to explore Bruin CLI for data pipeline orchestration
- **Not completed**: The Bruin CLI tool was not mature enough at the time to warrant a full implementation
- Technologies: Bruin CLI (evaluation only - nothing to publish)

### ⚡ 06-batch
**Batch Processing with Spark**  
- Distributed data processing with Apache Spark
- Reading from data lakes (Parquet files)
- Writing to data warehouses (DuckDB, Parquet reports)
- Learning when Spark adds value vs. native SQL engines
- Spark cluster setup with Docker (master + workers)
- Technologies: Apache Spark, Docker, DuckDB, Google Cloud Storage

### 🌊 07-stream
**Stream Processing**  
- Real-time data processing with Apache Flink
- Event streaming and message queuing
- Learning streaming architectures and patterns
- Technologies: Apache Flink, Kafka/Redpanda, Docker


## Primary Dataset

The project primarily uses **NYC Taxi Trip Record Data** from:
- [DataTalks Club NYC TLC Data](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/yellow) (2019-01 to 2021-07)
- [Official NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

## Key Technologies & Tools

- **Containerization**: Docker, Docker Compose
- **Programming**: Python 3.13, UV package manager
- **Databases**: PostgreSQL, DuckDB
- **Cloud Platforms**: Google Cloud Platform (GCP), Microsoft Azure
- **Data Warehousing**: DuckDB, BigQuery
- **Workflow Orchestration**: Kestra, Apache Airflow
- **Batch Processing**: Apache Spark
- **Stream Processing**: Apache Flink, Kafka/Redpanda
- **Analytics Engineering**: dbt (data build tool)
- **Data Formats**: CSV, Parquet


## Development Philosophy

This project intentionally explores **multiple approaches** to similar problems:
- Different workflow orchestration tools (Kestra, Airflow)
- Multiple cloud platforms (Azure, GCP)
- Various data storage formats (CSV, Parquet)
- Alternative data processing engines (Spark, SQL, dbt)

The purpose is educational: understanding when to use each tool, their trade-offs, and how they compare in real-world scenarios. Production systems would typically standardize on a single approach.

## Getting Started

Each module contains its own `README.md` with specific setup instructions. Generally:

1. Navigate to the desired module folder
2. Review the module-specific README
3. Set up the required environment (Docker, Python, cloud credentials)
4. Follow the module instructions to run the pipelines

## Learning Goals Achieved

✅ Containerization and Docker orchestration  
✅ Workflow orchestration patterns  
✅ Data warehouse design and operations  
✅ Analytics engineering with dbt  
✅ Distributed batch processing  
✅ Stream processing architectures  
✅ Cloud platform integration (GCP, Azure)  
✅ End-to-end data pipeline development  

---

**Note**: This is a learning project. Code quality, error handling, security practices, and architectural decisions prioritize experimentation and learning over production readiness.
