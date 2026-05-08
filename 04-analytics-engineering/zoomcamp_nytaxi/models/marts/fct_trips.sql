{{
  config(
    materialized='incremental' if target.type == 'bigquery' else 'table',
    partition_by={
      "field": "pickup_datetime",
      "data_type": "timestamp",
      "granularity": "month"
    } if target.type == 'bigquery' else none,
    cluster_by=['service_type', 'pickup_borough'] if target.type == 'bigquery' else none
  )
}}

select
    -- identifiers
    trips.trip_id,
    trips.vendor_id,
    trips.ratecode_id,
    trips.service_type,
    trips.payment_type,
    trips.trip_type,

    -- location details
    trips.pickup_location_id,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    trips.dropoff_location_id,
    drop_zone.borough as dropoff_borough,
    drop_zone.zone as dropoff_zone,

    -- timestamps
    trips.pickup_datetime,
    trips.dropoff_datetime,
    trips.store_and_forward_flag,

    -- trip info
    trips.passenger_count,
    trips.trip_distance,
    trips.trip_duration_minutes,

    -- amounts
    trips.fare_amount,
    trips.extra,
    trips.mta_tax,
    trips.tip_amount,
    trips.tolls_amount,
    trips.ehail_fee,
    trips.improvement_surcharge,
    trips.congestion_surcharge,
    trips.total_amount,
    trips.payment_type_description

from {{ ref('int_trips_clean_and_enrich') }} as trips
left join {{ ref('dim_zones') }} as pickup_zone
    on trips.pickup_location_id = pickup_zone.location_id
left join {{ ref('dim_zones') }} as drop_zone
    on trips.dropoff_location_id = drop_zone.location_id
