FROM apache/spark:3.4.2

USER root

# Versions
ARG HADOOP_VERSION=3.3.4
ARG GCS_CONNECTOR_VERSION=hadoop3-2.2.22
ARG BQ_CONNECTOR_VERSION=0.36.1


# System dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# Python: duckdb + GCP client
RUN pip install --no-cache-dir \
    google-cloud-storage \
    google-auth


# # GCS connector → Spark reads gs:// paths (Google Cloud Storage)
RUN curl -fsSL -o /opt/spark/jars/gcs-connector-${GCS_CONNECTOR_VERSION}.jar \
    https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-${GCS_CONNECTOR_VERSION}.jar

# # BigQuery connector → Spark reads/writes BigQuery tables
RUN curl -fsSL -o /opt/spark/jars/spark-bigquery-with-dependencies_2.12-${BQ_CONNECTOR_VERSION}.jar \
    https://github.com/GoogleCloudDataproc/spark-bigquery-connector/releases/download/${BQ_CONNECTOR_VERSION}/spark-bigquery-with-dependencies_2.12-${BQ_CONNECTOR_VERSION}.jar

USER spark
