# Generate random taxi trip data
def generate_random_trip() -> dict:
    """Generate a random yellow taxi trip."""
    pickup_time = datetime.now()
    trip_duration = random.randint(5, 60)  # 5-60 minutes
    dropoff_time = pickup_time + timedelta(minutes=trip_duration)
    
    return {
        'VendorID': random.choice([1, 2]),
        'tpep_pickup_datetime': pickup_time,
        'tpep_dropoff_datetime': dropoff_time,
        'passenger_count': random.randint(1, 6),
        'trip_distance': round(random.uniform(0.5, 20.0), 2),
        'RatecodeID': random.choice([1, 2, 3, 4, 5]),
        'store_and_fwd_flag': random.choice(['Y', 'N']),
        'PULocationID': random.randint(1, 263),
        'DOLocationID': random.randint(1, 263),
        'payment_type': random.choice([1, 2, 3, 4]),
        'fare_amount': round(random.uniform(5.0, 100.0), 2),
        'extra': random.choice([0.0, 0.5, 1.0]),
        'mta_tax': 0.5,
        'tip_amount': round(random.uniform(0.0, 20.0), 2),
        'tolls_amount': round(random.uniform(0.0, 10.0), 2),
        'improvement_surcharge': 0.3,
        'total_amount': 0.0,  # Will be calculated
        'congestion_surcharge': random.choice([0.0, 2.5, None]),
        'ehail_fee': None,
        'trip_type': None,
    }