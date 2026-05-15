"""
Kafka Avro Consumer for NYC Taxi Trip Data

Consumes taxi ride messages from a Kafka topic using Avro deserialization with Schema Registry,
batches them, and writes to partitioned Parquet files organized by date.

Features:
    - Consumes Avro-encoded messages from Kafka with automatic schema resolution
    - Batches messages for efficient writes (configurable batch size)
    - Writes to partitioned Parquet files: {year}/{month}/yellow_tripdata_{timestamp}.parquet
    - Automatic directory creation with date-based partitioning
    - Consumer group management for distributed consumption
    - Auto-commit enabled for offset tracking
    - Graceful shutdown handling

Usage:
    # Run from project root
    uv run --env-file .env python -m dev.src.consumers.consumer
    
    # Alternative with direct Python
    python -m dev.src.consumers.consumer

Prerequisites:
    - Kafka/Redpanda cluster with messages in the target topic
    - Schema Registry with registered taxi_trip schema
    - Producer should be running to generate messages
    - Required packages: confluent-kafka, pandas, pyarrow

Environment Variables Required (.env):
    - REDPANDA_BROKERS: Comma-separated broker addresses (e.g., 'localhost:9092')
    - KAFKA_TOPIC: Source Kafka topic to consume from (e.g., 'taxi-rides')
    - KAFKA_CONSUMER_GROUP: Consumer group ID for offset management
    - SCHEMA_REGISTRY_URL: Schema Registry endpoint (e.g., 'http://localhost:8081')
    - DATA_DEST_PATH: Base directory for output parquet files (default: 'data')

Configuration:
    - BATCH_SIZE: Number of messages per parquet file (default: 10)
    - auto.offset.reset: 'earliest' - starts from beginning if no offset stored
    - enable.auto.commit: True - automatically commits offsets

Output Structure:
    data/consumed/yellow/
        2026/
            05/
                yellow_tripdata_2026-05-15_14-30-45.parquet
                yellow_tripdata_2026-05-15_14-31-20.parquet

Batch Processing:
    - Accumulates messages until BATCH_SIZE is reached
    - Converts batch to pandas DataFrame
    - Writes as Parquet with PyArrow
    - Clears batch and continues


Schema Compatibility:
    Uses AvroDeserializer with Schema Registry client for automatic schema evolution
    support. Handles both green and yellow taxi schemas via taxi_trip.avsc.
"""

import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from typing import Iterator, Optional, Any
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer


def get_avro_deserialize(schema_registry: str) -> AvroDeserializer:
    schema_registry_client = SchemaRegistryClient({'url': schema_registry})
    return AvroDeserializer(schema_registry_client)

def consume_messages(consumer: DeserializingConsumer, topic: str) -> Iterator[Optional[Any]]:
    while True:
        msg = consumer.poll(1.0)  # 1 second timeout
        
        if msg is None:
            continue
            
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue
        
        try:
            print(f"Consumed message from topic '{topic}' at offset {msg.offset()}")
            yield msg.value()                
        except Exception as e:
            print(f"Failed to deserialize message from topic '{topic}': {e}")

def write_messages_to_parquet(batch, output_dir):   
    df = pd.DataFrame(batch)
    table = pa.Table.from_pandas(df)
    output_file = os.path.join(output_dir, f'yellow_tripdata_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.parquet')
    pq.write_table(table, output_file)

    print(f"Wrote {len(batch)} records to {os.path.basename(output_file)}")
    
def print_out_message(ride):
    # Field names aligned to dev/src/schemas/taxi_trip.avsc
    pickup_dt = datetime.fromtimestamp(ride['tpep_pickup_datetime'] / 1000)
    print(f"Received: Pickup {ride['PULocationID']} → Dropoff {ride['DOLocationID']}")
    print(f"  Distance: {ride['trip_distance']:.2f} mi | Amount: ${ride['total_amount']:.2f} | Time: {pickup_dt}")
    print()

def __main__():

    schema_registry = os.environ['SCHEMA_REGISTRY_URL']
    bootstrap_servers = os.environ['REDPANDA_BROKERS'].split(',')
    topic = os.environ['KAFKA_TOPIC']
    kafka_consumer_group = os.environ['KAFKA_CONSUMER_GROUP']

    avro_deserializer = get_avro_deserialize(schema_registry)

    # Create consumer
    consumer_conf = {
        'bootstrap.servers': ','.join(bootstrap_servers),
        'group.id': kafka_consumer_group,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
        'value.deserializer': avro_deserializer,
    }
    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe([topic])

    print(f"Starting Avro consumer for topic '{topic}'...\n")

    try:
        ride_batch = []
        BATCH_SIZE = 10
        output_dir = os.path.join(os.getenv('DATA_DEST_PATH', 'data'), 'consumed', 'yellow', datetime.now().strftime('%Y'), datetime.now().strftime('%m'))
        os.makedirs(output_dir, exist_ok=True)
        for ride in consume_messages(consumer, topic):
            # print_out_message(ride)
            ride_batch.append(ride)
            if len(ride_batch) >= BATCH_SIZE:
                write_messages_to_parquet(ride_batch, output_dir)
                ride_batch.clear() 
    finally:
        consumer.close()
        print("Consumer closed")

if __name__ == "__main__":
    __main__()