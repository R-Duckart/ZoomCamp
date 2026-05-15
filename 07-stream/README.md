# 07 - Stream Processing with Kafka/RedPanda

Real-time taxi trip data streaming using Kafka/RedPanda with two implementation approaches: a simplified test version and a production-ready dev version.

## Project Overview

This module demonstrates event streaming architectures with NYC taxi trip data. Messages flow from producers through Kafka topics to consumers, which batch and persist the data as partitioned Parquet files.

**Key Technologies:**
- **Kafka/RedPanda**: Event streaming platform
- **Avro**: Binary serialization format (dev version)
- **Schema Registry**: Schema versioning and validation (dev version)
- **Pydantic**: Type-safe data models (test version)
- **Apache Flink**: Stream processing (optional)

## Two Implementations

### 📁 [`test/`](test/README.md) - Simple Test Version
**Best for:** Learning, debugging, quick experimentation

**Features:**
- ✅ Simple setup (no Schema Registry)
- ✅ JSON serialization (human-readable)
- ✅ Pydantic models for validation
- ✅ Easy to debug and inspect messages
- ❌ Larger message size
- ❌ No schema evolution

**Start here if:** You're new to Kafka or want to understand the basics.

### 📁 [`dev/`](dev/README.md) - Production-Ready Version
**Best for:** Production deployments, high-throughput systems

**Features:**
- ✅ Avro binary serialization (compact)
- ✅ Schema Registry integration
- ✅ Schema evolution support (FORWARD/BACKWARD compatibility)
- ✅ Smaller message size
- ✅ Type validation at serialization
- ⚠️ More complex setup (3-step startup)

**Use this if:** You need production-grade streaming with schema management.

## Quick Start

### Prerequisites
```powershell
# Start RedPanda
docker compose up redpanda -d

# Install dependencies
uv sync
```

### Choose Your Path

**Option 1: Test Version (Simple)**
```powershell
# 1. Start consumer
uv run --env-file .env python -m test.src.consumers.consumer

# 2. Start producer (new terminal)
uv run --env-file .env python -m test.src.producers.producer
```

**Option 2: Dev Version (Production-Ready)**
```powershell
# 1. Register schema (REQUIRED FIRST!)
uv run --env-file .env python -m dev.src.schemas.schema_register

# 2. Start consumer
uv run --env-file .env python -m dev.src.consumers.consumer

# 3. Start producer (new terminal)
uv run --env-file .env python -m dev.src.producers.producer
```

## Project Structure

```
07-stream/
├── test/                      # Simple JSON/Pydantic implementation
│   ├── src/
│   │   ├── consumers/         # JSON consumer
│   │   ├── producers/         # JSON producer
│   │   └── model/             # Pydantic data models
│   └── README.md             # Test version docs
│
├── dev/                       # Production Avro implementation
│   ├── src/
│   │   ├── consumers/         # Avro consumer
│   │   ├── producers/         # Avro producer
│   │   ├── schemas/           # Avro schemas + registration
│   │   └── job/               # Flink jobs (optional)
│   └── README.md             # Dev version docs
│
├── data/                      # Consumer output (parquet files)
├── data_mock/                 # Mock taxi data for producers
├── docker-compose.yaml        # RedPanda + Flink services
├── .env                       # Configuration
└── pyproject.toml            # Python dependencies
```

## Comparison: Test vs Dev

| Feature | Test | Dev |
|---------|------|-----|
| **Serialization** | JSON | Avro (binary) |
| **Schema Registry** | ❌ Not needed | ✅ Required |
| **Message Size** | Larger | Smaller (~50% reduction) |
| **Setup Steps** | 2 | 3 (schema registration first) |
| **Debugging** | Easy (readable JSON) | Requires tools |
| **Schema Evolution** | Manual code changes | Automatic compatibility |
| **Best For** | Learning, testing | Production |

## Documentation

- **[Test Version](test/README.md)** - Full test version guide
- **[Dev Version](dev/README.md)** - Full dev version guide with Avro/Schema Registry


