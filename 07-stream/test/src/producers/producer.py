
# /!\ Execute this script with command `uv run python -m producers.producer`/!\ 
# otherwise you will get an error that the module `models` cannot be found. 
# This is because the `models` package is not in the same directory as this script. 
# By running the script with `uv run`, you are executing it from the root directory of the project, 
# which allows it to find the `models` package.

## LOAD DATA to simulate a real-world scenario where we read from a file and produce messages to Kafka.
import os
import pandas as pd

file_path = os.getenv('DATA_MOCK_PATH') + '/yellow_tripdata_mock.parquet'
columns = ['PULocationID', 'DOLocationID', 'trip_distance', 'total_amount', 'tpep_pickup_datetime']
df = pd.read_parquet(file_path, columns=columns).head(100)

## END LOAD DATA


import json
import time
from kafka import KafkaProducer
from test.src.models.taxi_ride_yellow import TaxiRideYellow

def json_serializer(object_dict):
    return json.dumps(object_dict).encode('utf-8')

servers = ['localhost:9092']
producer = KafkaProducer(bootstrap_servers=servers, value_serializer=json_serializer)
topic_name = os.getenv('TEST_TOPIC_NAME')

t0 = time.time()
for _, row in df.iterrows():
    row['tpep_pickup_datetime'] = int(row['tpep_pickup_datetime'].timestamp() * 1000) # convert into milliseconds
    taxi_ride_yellow = TaxiRideYellow.from_row(row)
    producer.send(topic_name, taxi_ride_yellow.to_dict())
    print(f"Sent taxi ride with pickup location {taxi_ride_yellow.pickup_location_id} and dropoff location {taxi_ride_yellow.dropoff_location_id}")
    time.sleep(0.1) # simulate some delay between sending messages
    
producer.flush()


