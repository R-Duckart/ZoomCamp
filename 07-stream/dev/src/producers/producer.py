"""
Kafka Avro Producer for NYC Taxi Trip Data

Streams taxi ride records from a parquet file to a Kafka topic using Avro serialization
with Schema Registry integration. Simulates real-time data production with random delays.

Features:
    - Reads taxi trip data from parquet file (mock data source)
    - Encodes messages using Avro schema with Confluent wire format
    - Registers and retrieves schema from Schema Registry
    - Produces messages with delivery confirmation callbacks
    - Simulates real-time streaming with random delays (3-30 seconds)
    - Converts timestamps to milliseconds for Avro long type compatibility

Usage:
    # Run from project root
    uv run --env-file .env python -m dev.src.producers.producer
    
    # Alternative with direct Python
    python -m dev.src.producers.producer

Prerequisites:
    - Kafka/Redpanda cluster running and accessible
    - Schema Registry running with taxi_trip schema registered
    - Mock parquet data file available at DATA_MOCK_PATH
    - Required packages: confluent-kafka, fastavro, pandas, requests

Environment Variables Required (.env):
    - REDPANDA_BROKERS: Comma-separated list of broker addresses (e.g., 'localhost:9092')
    - KAFKA_TOPIC: Target Kafka topic name (e.g., 'taxi-rides')
    - SCHEMA_REGISTRY_URL: Schema Registry endpoint (e.g., 'http://localhost:8081')
    - DATA_MOCK_PATH: Directory containing yellow_tripdata_mock.parquet

Schema Format:
    Uses Confluent wire format with magic byte (0x00) + schema ID (4 bytes) + Avro payload.
    Schema file: dev/src/schemas/taxi_trip.avsc

Output:
    Produces 100 taxi ride messages to Kafka with Avro encoding, logging each delivery
    to console with topic, partition, and offset information.

Example Output:
    Starting Avro producer for topic 'taxi-rides' with schema ID 1...
    Produced message for ride with pickup at 1234567890000 and dropoff at 1234567900000
    Produced to taxi-rides [0] @ offset 42
"""

import pandas as pd
import random
from confluent_kafka import Producer
from fastavro import schemaless_writer, parse_schema
import os, io, json, struct, time, requests

# Load data to simulate a real-world scenario where we read from a file and produce messages to Kafka.
file_path = os.getenv('DATA_MOCK_PATH') + '/yellow_tripdata_mock.parquet'
df = pd.read_parquet(file_path).head(100)
df.sort_values('tpep_pickup_datetime', inplace=True)

# Load Avro schema
def get_schema_file() -> dict:
    taxi_schema_path = os.path.join(os.path.dirname(__file__), '..', 'schemas', 'taxi_trip.avsc')
    with open(taxi_schema_path) as f:
        taxi_schema_dict = json.load(f)
    return taxi_schema_dict

def get_schema_id(schema_registry: str, subject: str) -> int:
    try:
        r = requests.get(f"{schema_registry}/subjects/{subject}/versions/latest")
        r.raise_for_status()
        return r.json()['id']
    except requests.exceptions.RequestException as e:
        print(f"Error fetching schema versions: {e}")
        raise

def avro_encode_confluent(record: dict, schema_id: int, taxi_schema_dict: dict) -> bytes:
    buf = io.BytesIO()
    # magic byte + schema id
    buf.write(b"\x00")
    buf.write(struct.pack(">I", schema_id))
    # avro payload
    schemaless_writer(buf, parse_schema(taxi_schema_dict), record)
    return buf.getvalue()

def delivery_report(err, msg):
    if err:
        print("Delivery failed:", err)
    else:
        print(f"Produced to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

def __main__():

    schema_registry = os.environ['SCHEMA_REGISTRY_URL']
    topic = os.environ['KAFKA_TOPIC']
    subject = topic + '-value'

    bootstrap_servers = os.environ['REDPANDA_BROKERS'].split(',')
    topic = os.environ['KAFKA_TOPIC']
    p = Producer({"bootstrap.servers": ",".join(bootstrap_servers)})
    
    taxi_schema_dict = get_schema_file()
    schema_id = get_schema_id(schema_registry, subject)


    print(f"Starting Avro producer for topic '{topic}' with schema ID {schema_id}...\n")

    for _, ride in df.iterrows():
        ride['tpep_pickup_datetime'] = int(ride['tpep_pickup_datetime'].timestamp() * 1000) # convert into milliseconds
        ride['tpep_dropoff_datetime'] = int(ride['tpep_dropoff_datetime'].timestamp() * 1000) # convert into milliseconds
        ride['passenger_count'] = int(ride['passenger_count']) # convert into integer
        
        payload = avro_encode_confluent(ride.to_dict(), schema_id, taxi_schema_dict)
        p.produce(topic, value=payload, on_delivery=delivery_report)
        print(f"Produced message for ride with pickup at {ride['tpep_pickup_datetime']} and dropoff at {ride['tpep_dropoff_datetime']}")
        # Random pause between 3 and 30 seconds
        pause = random.uniform(3, 30)
        time.sleep(pause)
        
    p.flush()

if __name__ == "__main__":
    __main__()