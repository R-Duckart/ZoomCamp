with green_tripdata as (
    select 
    -- identifiers
    vendor_id,
    ratecode_id,
    pickup_location_id,
    dropoff_location_id,
    service_type,
    payment_type,
    trip_type,

    -- timestamps
    pickup_datetime,
    dropoff_datetime,

    -- trip info
    passenger_count,
    trip_distance,
    store_and_forward_flag,
    
    -- amounts
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    congestion_surcharge,
    total_amount

    from {{ ref('stg_green_tripdata') }}
),
yellow_tripdata as (
    select 
    -- identifiers
    vendor_id,
    ratecode_id,
    pickup_location_id,
    dropoff_location_id,
    service_type,
    payment_type,
    trip_type,

    -- timestamps
    pickup_datetime,
    dropoff_datetime,

    -- trip info
    passenger_count,
    trip_distance,
    store_and_forward_flag,
    
    -- amounts
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    congestion_surcharge,
    total_amount

    from {{ ref('stg_yellow_tripdata') }}
)
select * from green_tripdata
union all
select * from yellow_tripdata