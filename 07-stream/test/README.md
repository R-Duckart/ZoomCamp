# 07__TEST - Stream Processing with Kafka / deal with RedPanda

> **Note:** This is a simplified test version using Pydantic models for JSON serialization.  
> ***Real-time taxi ride data streaming using Kafka/RedPanda with Pydantic class models and JSON encoding.***


## Architecture

```
Producer → Kafka/RedPanda → Consumer → Parquet Files
                                          ↓
                              data/consumed/{year}/{month}/
```

**Components:**
- **RedPanda**: Kafka-compatible streaming platform (simpler setup)
- **Producer**: Reads mock taxi data, serializes with Pydantic/JSON, publishes to Kafka
- **Consumer**: Consumes JSON messages, validates with Pydantic, batches, writes to partitioned Parquet files

## Project Structure

```
07-stream/
├── test/
│   └── src/
│       ├── consumers/         # Kafka consumers
│       │   └── consumer.py    # Main Avro consumer
│       ├── producers/         # Kafka producers
│       │   └── producer.py    # Main Avro producer
│       ├── model/             # Taxi Ride Class
│       │   └── taxi_ride_yellow.py # Main Class
├── data/                      # Consumer output
├── data_mock/                 # Mock taxi data for producer
├── docker-compose.yaml        # RedPanda + Flink services
├── .env                       # Configuration
└── pyproject.toml             # Python dependencies
```

## Setup

### 1. Start Infrastructure

**Start RedPanda (Kafka broker):**
```powershell
docker compose up redpanda -d
```

This starts:
- Kafka broker on `localhost:9092`
- Pandaproxy (REST API) on `localhost:8082`

**Verify services:**
```powershell
# Check RedPanda is running
docker ps

# Test Kafka is accessible (optional)
docker exec -it <container-id> rpk cluster info
```

### 2. Install Python Dependencies

```powershell
uv sync
```

### 3. Configure Environment

Copy `.env.example` to `.env` if needed, or verify your `.env` has:

```env
# Data paths
DATA_MOCK_PATH=data_mock
DATA_DEST_PATH=data

# Kafka brokers
REDPANDA_BROKERS=localhost:9092

# Topics and consumer groups
KAFKA_TOPIC=taxi-trips
KAFKA_CONSUMER_GROUP=taxi-consumers
```

## Usage - Correct Execution Order

### Step 1: Start Consumer (Waits for Messages)

```powershell
uv run --env-file .env python -m test.src.consumers.consumer
```

**Expected output:**
```
Starting consumer for topic 'taxi-trips'...
[Waiting for messages...]
```

**What it does:**
- Connects to Kafka topic `taxi-trips`
- Deserializes JSON messages and validates with Pydantic TaxiRideYellow model
- Batches 10 messages
- Writes to Parquet: `data/consumed/yellow/{year}/{month}/yellow_tripdata_{timestamp}.parquet`

### Step 2: Start Producer (Sends Messages)

**In a new terminal:**
```powershell
uv run --env-file .env python -m test.src.producers.producer
```

**Expected output:**
```
Starting producer for topic 'taxi-trips'...
Produced message for ride: pickup_location=161, dropoff_location=236, distance=2.5
Produced to taxi-trips [0] @ offset 0
[Random pause 3-30 seconds]
Produced message for ride: pickup_location=237, dropoff_location=161, distance=1.8
Produced to taxi-trips [0] @ offset 1
```

**What it does:**
- Reads 100 taxi rides from `data_mock/yellow_tripdata_mock.parquet`
- Converts each row to TaxiRideYellow Pydantic model
- Serializes to JSON and publishes to Kafka topic
- Random delays (3-30 sec) to simulate real-time streaming

### Step 3: Monitor Consumer Output

**Check consumer terminal:**
```
Consumed message from topic 'taxi-trips' at offset 0
Consumed message from topic 'taxi-trips' at offset 1
...
Wrote 10 records to yellow_tripdata_2026-05-15_14-30-45.parquet
```

**Verify parquet files:**
```powershell
ls data/consumed/yellow/2026/05/
```

## Data Flow

### Pydantic Data Model (`test/src/model/taxi_ride_yellow.py`)

The `TaxiRideYellow` Pydantic class provides:
- **Type safety**: Automatic validation of field types
- **Serialization**: Easy conversion to/from JSON
- **Documentation**: Clear field definitions with type hints

**Key fields:**
- `pickup_location_id` (int)
- `dropoff_location_id` (int)
- `trip_distance` (float)
- `total_amount` (float)
- `pickup_datetime` (int) - milliseconds since epoch

**Methods:**
- `from_row(row)` - Create from pandas DataFrame row
- `from_dict(data)` - Create from Kafka message dict
- `to_dict()` - Serialize for Kafka (JSON)
- `get_datetime()` - Convert timestamp to datetime

### Output Parquet Structure

```
data/consumed/yellow/
├── 2026/
│   ├── 05/
│   │   ├── yellow_tripdata_2026-05-15_14-30-45.parquet  # Batch 1 (10 records)
│   │   ├── yellow_tripdata_2026-05-15_14-31-20.parquet  # Batch 2 (10 records)
│   │   └── ...
```

**Batch Size:** 10 messages per file (configurable in `consumer.py` BATCH_SIZE)


### Start Flink Cluster

```powershell
# Build Flink image (choose based on your CPU)
docker compose build jobmanager

# Start Flink JobManager and TaskManager
docker compose up jobmanager taskmanager -d
```

## Configuration Reference

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DATA_MOCK_PATH` | Directory with mock parquet files | `data_mock` |
| `DATA_DEST_PATH` | Output directory for consumed data | `data` |
| `REDPANDA_BROKERS` | Kafka broker addresses | `localhost:9092` |
| `KAFKA_TOPIC` | Topic name for taxi trips | `taxi-trips` |
| `KAFKA_CONSUMER_GROUP` | Consumer group ID | `taxi-consumers` |

**Note:** Schema Registry URL is NOT needed for this Pydantic/JSON implementation.

### Consumer Settings (in code)

- `BATCH_SIZE`: 10 messages per Parquet file
- `auto.offset.reset`: `earliest` (start from beginning)
- `enable.auto.commit`: `True`

### Producer Settings (in code)

- Source: First 100 records from `data_mock/yellow_tripdata_mock.parquet`
- Delay: Random 3-30 seconds between messages
- Serialization: JSON encoding via Pydantic TaxiRideYellow model
- Validation: Automatic type checking and field validation with Pydantic

## Cleanup

```powershell
# Stop all services
docker compose down

# Remove volumes (deletes all Kafka data)
docker compose down -v

# Clear consumed parquet files
rm -r data/consumed/*
```
