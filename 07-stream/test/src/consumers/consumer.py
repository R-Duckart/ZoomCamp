import json
import os
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime
from test.src.models.taxi_ride_yellow import TaxiRideYellow
from kafka import KafkaConsumer

# Configure logging
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Output configuration
OUTPUT_DIR = os.path.join(os.getenv('DATA_DEST_PATH', 'data'), 'consumed', 'yellow', datetime.now().strftime('%Y'), datetime.now().strftime('%m'))
os.makedirs(OUTPUT_DIR, exist_ok=True)
# Write to parquet every N messages
BATCH_SIZE = 10  

# Strategy: Write separate files per batch, avoid reading entire dataset
batch_counter = 0

def json_deserializer(data):
    return json.loads(data.decode('utf-8'))

def process_message(message):
    taxi_ride = TaxiRideYellow.from_dict(message.value)
    
    # Convert to dict with datetime
    ride_data = {
        'pickup_location_id': taxi_ride.pickup_location_id,
        'dropoff_location_id': taxi_ride.dropoff_location_id,
        'trip_distance': taxi_ride.trip_distance,
        'total_amount': taxi_ride.total_amount,
        'pickup_datetime': taxi_ride.get_datetime(),
        'pickup_timestamp_ms': taxi_ride.pickup_datetime
    }
    
    return ride_data

def write_messages_to_parquet(batch):
    df = pd.DataFrame(batch)
    table = pa.Table.from_pandas(df)
    output_file = os.path.join(OUTPUT_DIR, f'yellow_tripdata_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.parquet')
    pq.write_table(table, output_file)
    

    print(f"Wrote {len(batch)} records to {os.path.basename(output_file)}")
    
def __main__():
    global batch_counter
    servers = ['localhost:9092']
    topic_name = os.getenv('TEST_TOPIC_NAME')

    taxi_rides_yellow_consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=servers,
        auto_offset_reset='earliest',
        group_id='taxi_rides_yellow_consumer_group',
        value_deserializer=json_deserializer
    )
    
    print(f"Start consuming messages from '{topic_name}'...")
    print(f"Writing to: {OUTPUT_DIR}\n")

    try:
        batch = []
        message_count = 0
        for message in taxi_rides_yellow_consumer:
            ride_data = process_message(message)
            
            batch.append(ride_data)
            message_count += 1
            
            print(f"[{message_count}] Pickup {ride_data['pickup_location_id']} → Dropoff {ride_data['dropoff_location_id']} | ${ride_data['total_amount']:.2f}")
            
            # Write batch to parquet
            if len(batch) >= BATCH_SIZE:
                batch_counter += 1
                write_messages_to_parquet(batch)      
                batch = []

    finally:
        # Write remaining batch
        if batch:
            batch_counter += 1
            write_messages_to_parquet(batch)
            
        taxi_rides_yellow_consumer.close()
        print(f"\nConsumer closed. Total messages processed: {message_count}")
        print(f"Data saved to: {OUTPUT_DIR}")

if __name__ == "__main__":
    __main__()