"""
Avro Schema Registration Script for Kafka Schema Registry

Registers the taxi trip Avro schema to the Schema Registry, enabling producers
and consumers to encode/decode messages with automatic schema validation and evolution.

Purpose:
    - Deletes existing schema subject (development mode - safe reset)
    - Registers taxi_trip.avsc schema to Schema Registry
    - Returns schema ID used by producer to encode messages
    - Enables consumer to automatically deserialize with correct schema version

Why This is Required:
    Without schema registration, both producer and consumer will fail:
    - Producer: Cannot fetch schema ID to encode Avro messages
    - Consumer: Cannot deserialize messages (schema not found)

Usage:
    # MUST run before starting producer/consumer (FIRST STEP!)
    uv run --env-file .env python -m dev.src.schemas.schema_register

When to Run:
    ✅ First time setup (before any producer/consumer starts)
    ✅ After modifying taxi_trip.avsc schema
    ✅ After clearing Schema Registry data
    ❌ Not needed during normal operation (schema persists in registry)

Prerequisites:
    - RedPanda/Kafka with Schema Registry running on SCHEMA_REGISTRY_URL
    - taxi_trip.avsc file exists in dev/src/schemas/
    - Network access to Schema Registry endpoint

Environment Variables Required (.env):
    - SCHEMA_REGISTRY_URL: Schema Registry endpoint (e.g., http://localhost:8081)
    - KAFKA_TOPIC: Base topic name (subject becomes '{KAFKA_TOPIC}-value')

Warnings:
    Safe for: Development, testing, local environments
    DANGEROUS for: Production with active producers/consumers
    
    Production Best Practice:
    - Comment out delete_schema() call
    - Use schema evolution with compatibility modes (FORWARD/BACKWARD/FULL)
    - Never delete schemas with active message flows
    - Test schema changes in staging first

Schema Subject Naming:
    Follows Confluent convention: '{topic_name}-value'
    Example: KAFKA_TOPIC='taxi-trips' → subject='taxi-trips-value'

Related Files:
    - dev/src/schemas/taxi_trip.avsc: Avro schema definition
    - dev/src/producers/producer.py: Fetches schema ID to encode messages
    - dev/src/consumers/consumer.py: Uses Schema Registry to decode messages
"""

import os
import requests
import json


SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL')
SUBJECT = os.environ['KAFKA_TOPIC'] + '-value'

# Load Avro schema
taxi_schema_path = os.path.join(os.path.dirname(__file__), 'taxi_trip.avsc')
with open(taxi_schema_path) as f:
    taxi_schema_dict = json.load(f)

def delete_schema() -> None:
    try:       
        r = requests.delete(
            f"{SCHEMA_REGISTRY_URL}/subjects/{SUBJECT}",
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            timeout=10,
        )
        r.raise_for_status()
        print(f"Deleted existing schema subject '{SUBJECT}'")

    except requests.exceptions.HTTPError as e:
        print(f"Warning deleting schema: {e.response.text}")

def register_schema(schema_dict) -> int:
    try:
        payload = {"schema": json.dumps(schema_dict)}
    
        r = requests.post(
            f"{SCHEMA_REGISTRY_URL}/subjects/{SUBJECT}/versions",
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            data=json.dumps(payload),
            timeout=10,
        )
        r.raise_for_status()
        return r.json()["id"]
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 409:
            print("Schema incompatible. Updating compatibility mode to NONE...")
        else:
            print(f"Error registering schema: {e.response.text}")
        
        raise

# Delete existing schema first - DEV ONLY
delete_schema()
# Register new schema
schema_id = register_schema(taxi_schema_dict)

if schema_id:
    print("Schema registered with ID =", schema_id) 
else:
    print("Failed to register schema")
