# 07__DEV - Stream Processing with Kafka / deal with RedPanda

> **Note:** This is an improved dev version using Avro schema & Flink to store in a specific Data Mart a preprocessed table.  
> ***Real-time taxi ride data streaming using Kafka/RedPanda with Avro serialization, Schema Registry, and optional Apache Flink processing.***



## Architecture

```
   Producer 
      ↓
Schema Registry → Avro Schema
      ↓                   
Kafka Topic (taxi-trips)
    ↙                    ↘
Consumer.py            Flink Job
(CONSUMER 1)           (CONSUMER 2)
    ↓                      ↓
Parquet Files          PostgreSQL
(Raw Data)             (Aggregated Metrics) → Analytics/Dashboards                                               
```

**Components:**
- **RedPanda**: Kafka-compatible streaming platform with built-in Schema Registry
- **Producer**: Reads mock taxi data, encodes with Avro, publishes to Kafka
- **Consumer.py**: CONSUMER 1 - Consumes Avro messages, batches, writes raw events to Parquet (archive)
- **Flink Job**: CONSUMER 2 - Consumes same Avro messages, aggregates by 10-min windows per pickup location, writes to PostgreSQL (real-time metrics)
- **Schema Registry**: Manages Avro schema versions for taxi trip events

## Project Structure

```
07-stream/
├── dev/
│   └── src/
│       ├── consumers/         # Kafka consumers
│       │   └── consumer.py    # Main Avro consumer
│       ├── producers/         # Kafka producers
│       │   └── producer.py    # Main Avro producer
│       ├── schemas/           # Avro schemas
│       │   ├── taxi_trip.avsc # Taxi trip schema definition
│       │   └── schema_register.py  # Schema registration script
│       └── job/               # Flink jobs (optional)
├── data/                      # Consumer output
├── data_mock/                 # Mock taxi data for producer
├── docker-compose.yaml        # RedPanda + Flink services
├── .env                       # Configuration
└── pyproject.toml            # Python dependencies
```

## Setup

### 1. Start Infrastructure

**Start RedPanda (Kafka + Schema Registry):**
```powershell
docker compose up redpanda -d
```

This starts:
- Kafka broker on `localhost:9092`
- Schema Registry on `http://localhost:8081`
- Pandaproxy (REST API) on `localhost:8082`

**Verify services:**
```powershell
# Check RedPanda is running
docker ps

# Test Schema Registry
curl http://localhost:8081/subjects
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
SCHEMA_REGISTRY_URL=http://localhost:8081

# Topics and consumer groups
KAFKA_TOPIC=taxi-trips
KAFKA_CONSUMER_GROUP=taxi-consumers
```

## Usage - Correct Execution Order

### Step 1: Register Avro Schema (Required First!)

**IMPORTANT:** Run this before starting producer or consumer, or when schema changes.

```powershell
uv run --env-file .env python -m dev.src.schemas.schema_register
```

**Expected output:**
```
Deleted existing schema subject 'taxi-trips-value'
Schema registered with ID = 1
```

**What it does:**
- Deletes existing schema (dev mode)
- Registers `taxi_trip.avsc` schema to Schema Registry
- Returns schema ID needed by producer/consumer

### Step 2: Start Consumer (Waits for Messages)

```powershell
uv run --env-file .env python -m dev.src.consumers.consumer
```

**Expected output:**
```
Starting Avro consumer for topic 'taxi-trips'...
[Waiting for messages...]
```

**What it does:**
- Connects to Kafka topic `taxi-trips`
- Deserializes Avro messages using Schema Registry
- Batches 10 messages
- Writes to Parquet: `data/consumed/yellow/{year}/{month}/yellow_tripdata_{timestamp}.parquet`

### Step 3: Start Producer (Sends Messages)

**In a new terminal:**
```powershell
uv run --env-file .env python -m dev.src.producers.producer
```

**Expected output:**
```
Starting Avro producer for topic 'taxi-trips' with schema ID 1...
Produced message for ride with pickup at 1234567890000 and dropoff at 1234567900000
Produced to taxi-trips [0] @ offset 0
[Random pause 3-30 seconds]
Produced message for ride with pickup at 1234567920000 and dropoff at 1234567930000
Produced to taxi-trips [0] @ offset 1
```

**What it does:**
- Reads 100 taxi rides from `data_mock/yellow_tripdata_mock.parquet`
- Encodes each ride as Avro with Confluent wire format
- Publishes to Kafka topic
- Random delays (3-30 sec) to simulate real-time streaming

### Step 4: Monitor Consumer Output

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

### Avro Schema (`dev/src/schemas/taxi_trip.avsc`)

Key fields:
- `VendorID` (int)
- `tpep_pickup_datetime` (long) - milliseconds since epoch
- `tpep_dropoff_datetime` (long)
- `PULocationID`, `DOLocationID` (int)
- `trip_distance` (double)
- `total_amount` (double)
- `passenger_count` (int)

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

## Flink Integration (Optional)

### Start Flink Cluster

```powershell
# Build Flink image (choose based on your CPU)
docker compose build jobmanager

# Start Flink JobManager and TaskManager
docker compose up jobmanager taskmanager -d
```

**Access Flink Web UI:** http://localhost:8085

### Flink Configuration

Uses `pyproject.flink.toml` for Flink-specific Python dependencies.  
REST API port changed to `8085` in `flink-config.yaml` (avoids conflict with Schema Registry on 8081).

## Troubleshooting

### Schema Not Found
```
Error: 404 Not Found - Subject 'taxi-trips-value' not found
```
**Solution:** Run schema registration first (Step 1).

### Consumer Can't Deserialize
```
Failed to deserialize message from topic 'taxi-trips'
```
**Solution:** Ensure schema is registered and matches producer schema version.

### Port Already in Use
```
Error starting redpanda: bind: address already in use
```
**Solution:** Check if ports 9092, 8081, 8082 are free. Stop conflicting services.

### No Messages Received
**Checklist:**
- ✅ RedPanda is running (`docker ps`)
- ✅ Schema registered (Step 1)
- ✅ Consumer started and connected
- ✅ Producer running and producing messages
- ✅ Topic name matches in `.env` for producer/consumer

### View Kafka Topics
```powershell
# Using rpk (RedPanda CLI) inside container
docker exec -it <container-id> rpk topic list
docker exec -it <container-id> rpk topic consume taxi-trips
```

## Configuration Reference

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DATA_MOCK_PATH` | Directory with mock parquet files | `data_mock` |
| `DATA_DEST_PATH` | Output directory for consumed data | `data` |
| `REDPANDA_BROKERS` | Kafka broker addresses | `localhost:9092` |
| `SCHEMA_REGISTRY_URL` | Schema Registry endpoint | `http://localhost:8081` |
| `KAFKA_TOPIC` | Topic name for taxi trips | `taxi-trips` |
| `KAFKA_CONSUMER_GROUP` | Consumer group ID | `taxi-consumers` |

### Consumer Settings (in code)

- `BATCH_SIZE`: 10 messages per Parquet file
- `auto.offset.reset`: `earliest` (start from beginning)
- `enable.auto.commit`: `True`

### Producer Settings (in code)

- Source: First 100 records from `data_mock/yellow_tripdata_mock.parquet`
- Delay: Random 3-30 seconds between messages
- Encoding: Avro with Confluent wire format (magic byte + schema ID)

## Development vs Production

### Current Setup (Development)

```python
# In schema_register.py
delete_schema()  # ⚠️ Deletes schema every time
register_schema(taxi_schema_dict)
```

**Production Considerations:**
- ❌ Don't delete schema in production
- ✅ Use schema evolution (FORWARD/BACKWARD compatibility)
- ✅ Version schemas carefully
- ✅ Test compatibility before registering
- ✅ Set appropriate retention policies

## Cleanup

```powershell
# Stop all services
docker compose down

# Remove volumes (deletes all Kafka data)
docker compose down -v

# Clear consumed parquet files
rm -r data/consumed/*
```
