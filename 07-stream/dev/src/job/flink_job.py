"""
Flink is used to aggregate real-time taxi trip data into time-windowed metrics.

1. Input: Read Kafka Stream
 - Consumes taxi trip messages from Kafka topic
 - Deserializes Avro messages using Schema Registry
 - Extracts timestamp from tpep_pickup_datetime for event-time processing

2. Watermark: Handle Late Data
 - Waits up to 5 seconds for late-arriving messages
 - Prevents data loss from network delays

3. Transform: 10-Minute Tumbling Windows
    - Groups data into 10-minute intervals based on pickup time
    - Aggregates by PULocationID to compute:
        - num_trips: Total trips per location
        - avg_distance: Average trip distance
        - total_revenue: Sum of total_amount

4. Output: Write to PostgreSQL
    - Writes aggregated results to taxitrips_events table
    - ensures idempotent writes with PRIMARY KEY on (window_start, PULocationID)
    - enables real-time analytics on taxi trip patterns by location and time

RUN WITH : docker compose exec jobmanager ./bin/flink run -py /opt/src/job/flink_job.py --pyFiles /opt/src -d
UI : http://localhost:8085

# Postgres connexion
## with pgcli:

uvx pgcli -h localhost -p 5432 -U postgres -d postgres

## With Docker :

docker compose exec postgres psql -U postgres -d postgres

# Create the table in Postgres before running the Flink job:
 CREATE TABLE taxitrips_events (
   window_start TIMESTAMP(3),
   PULocationID INTEGER,
   num_trips BIGINT,
   avg_distance DOUBLE PRECISION,
   total_revenue DOUBLE PRECISION,          
   PRIMARY KEY (window_start, PULocationID)             
  );

# To check the results in Postgres, you can run:
uvx pgcli -h localhost -p 5432 -U postgres -d postgres

SELECT window_start, count(*) as locations, sum(num_trips) as total_t
rips,
    round(sum(total_revenue)::numeric, 2) as revenue
FROM taxitrips_events
GROUP BY window_start
ORDER BY window_start;

"""

import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

BOOTSTRAP_SERVERS = os.environ["REDPANDA_BROKERS_INTERNAL"]
TOPIC_NAME = os.environ["KAFKA_TOPIC"]
SCHEMA_REGISTRY_URL = os.environ["SCHEMA_REGISTRY_URL_INTERNAL"]

POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]

def create_taxitrips_stream(table_env, table_name):
    table_env.execute_sql(f"""
        CREATE TABLE {table_name} (
            VendorID INTEGER,
            tpep_pickup_datetime BIGINT,
            tpep_dropoff_datetime BIGINT,
            passenger_count INTEGER,
            trip_distance DOUBLE,
            RatecodeID FLOAT,
            store_and_fwd_flag STRING,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            payment_type INTEGER,
            fare_amount DOUBLE,
            extra DOUBLE,
            mta_tax DOUBLE,
            tip_amount DOUBLE,
            tolls_amount DOUBLE,
            improvement_surcharge DOUBLE,
            total_amount DOUBLE,
            congestion_surcharge DOUBLE,
            Airport_fee DOUBLE,
            event_timestamp AS TO_TIMESTAMP_LTZ(tpep_pickup_datetime, 3),
            WATERMARK for event_timestamp as event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{BOOTSTRAP_SERVERS}',
            'topic' = '{TOPIC_NAME}',
            'format' = 'avro-confluent',
            'avro-confluent.schema-registry.url' = '{SCHEMA_REGISTRY_URL}',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest'
        )
        """
    )

def create_events_aggregated_table(table_env, table_name):
    table_env.execute_sql(f"""
        CREATE TABLE {table_name} (
            window_start TIMESTAMP(3),
            PULocationID INTEGER,
            num_trips BIGINT,
            avg_distance DOUBLE,
            total_revenue DOUBLE,
            PRIMARY KEY (window_start, PULocationID) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = '{POSTGRES_USER}',
            'password' = '{POSTGRES_PASSWORD}',
            'driver' = 'org.postgresql.Driver'
        )
        """
    )

def main():
    table_stream_name = "taxitrips_stream"
    table_aggregated_name = "taxitrips_events"

    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10000) # 10 seconds
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()

    table_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        create_taxitrips_stream(table_env, table_stream_name)
        create_events_aggregated_table(table_env, table_aggregated_name)

        table_env.execute_sql(f"""
            INSERT INTO 
                {table_aggregated_name}
            SELECT 
                CAST(window_start AS TIMESTAMP(3)) as window_start,
                PULocationID, 
                COUNT(*) as num_trips, 
                AVG(trip_distance) as avg_distance, 
                SUM(total_amount) as total_revenue
            FROM 
                TUMBLE(TABLE {table_stream_name}, DESCRIPTOR(event_timestamp), INTERVAL '10' MINUTES)
            GROUP BY
                window_start, PULocationID
        """).wait()
    except Exception as e:
        print(f"Error executing Flink job: {e}")

if __name__ == "__main__":
    main()