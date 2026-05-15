#!/usr/bin/env python

"""BigQuery I/O PySpark example."""

import os

from pyspark.sql import SparkSession

# Get GCP configuration from environment variables
gcp_project_id = os.getenv('GCP_PROJECT_ID')
gcp_location = os.getenv('GCP_LOCATION', 'europe-west1')
gcp_bucket = os.getenv('GCP_BUCKET')
gcp_dataset = os.getenv('GCP_DATASET', 'marts')

spark = (
    SparkSession.builder
        .master("local") \
        .appName("GCP-Spark-Session") \
        .config("spark.datasource.bigquery.project", gcp_project_id) \
        .getOrCreate()
)

# Set location for BigQuery operations
spark.conf.set("location", gcp_location)

# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the connector.
spark.conf.set('spark.datasource.bigquery.temporaryGcsBucket', gcp_bucket)

# Load data from BigQuery.
words = spark.read.format('bigquery').load(f'{gcp_project_id}.{gcp_dataset}.fct_trips') 

words.createOrReplaceTempView('test_trips')

# Perform word count.
word_count = spark.sql(
    'SELECT * FROM test_trips LIMIT 10')
word_count.show()
word_count.printSchema()

# Save the data to BigQuery
# word_count.write.format('bigquery') \
#   .save('wordcount_dataset.wordcount_output')