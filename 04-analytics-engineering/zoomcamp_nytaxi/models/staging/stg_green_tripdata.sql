{{ config(
    materialized='incremental' if target.type == 'bigquery' else 'table',
    unique_key=['vendor_id', 'pickup_datetime', 'dropoff_datetime', 'pickup_location_id', 'dropoff_location_id', 'ratecode_id'],
    incremental_strategy='merge'
) }}

select
    -- identifiers
    cast(VendorID as {{ dbt.type_int() }}) as vendor_id,
    cast(RatecodeID as {{ dbt.type_int() }}) as ratecode_id,
    cast(PULocationID as {{ dbt.type_int() }}) as pickup_location_id,
    cast(DOLocationID as {{ dbt.type_int() }}) as dropoff_location_id,
    cast('Green' as {{ dbt.type_string() }}) as service_type,
    cast(payment_type as {{ dbt.type_int() }}) as payment_type,
    cast(trip_type as {{ dbt.type_int() }}) as trip_type,

    -- timestamps
    cast(lpep_pickup_datetime as {{ dbt.type_timestamp() }}) as pickup_datetime,
    cast(lpep_dropoff_datetime as {{ dbt.type_timestamp() }}) as dropoff_datetime,

    -- trip info
    cast(passenger_count as {{ dbt.type_int() }}) as passenger_count,
    cast(trip_distance as {{ dbt.type_float() }}) as trip_distance,
    cast(store_and_fwd_flag as {{ dbt.type_string() }}) as store_and_forward_flag,

    -- amounts
    cast(fare_amount as {{ dbt.type_float() }}) as fare_amount,
    cast(extra as {{ dbt.type_float() }}) as extra,
    cast(mta_tax as {{ dbt.type_float() }}) as mta_tax,
    cast(tip_amount as {{ dbt.type_float() }}) as tip_amount,
    cast(tolls_amount as {{ dbt.type_float() }}) as tolls_amount,
    cast(ehail_fee as {{ dbt.type_float() }}) as ehail_fee,
    cast(improvement_surcharge as {{ dbt.type_float() }}) as improvement_surcharge,
    cast(congestion_surcharge as {{ dbt.type_float() }}) as congestion_surcharge,
    cast(total_amount as {{ dbt.type_float() }}) as total_amount

from {{ source('main', 'green_tripdata') }}
{% if is_incremental() %}
where lpep_pickup_datetime > (select max(pickup_datetime) from {{ this }})
{% endif %}
qualify row_number() over (
    partition by VendorID, lpep_pickup_datetime, lpep_dropoff_datetime, PULocationID, DOLocationID, RatecodeID
    order by lpep_pickup_datetime
) = 1
