-- Enrich and clean the unioned trips data, preparing it for analysis and reporting in the marts layer.
-- The type description uses a macro that performs a lookup against a reference table, demonstrating how to use macros for dynamic transformations.

with unioned_trips as (select * from {{ ref('int_trips_unioned') }}),

cleaned_and_enriched as (
    select
        -- generate unique trip identifier (surrogate key pattern)
        {{ dbt_utils.generate_surrogate_key(['u.vendor_id', 'u.pickup_datetime', 'u.dropoff_datetime', 'u.pickup_location_id', 'u.dropoff_location_id', 'u.service_type', 'u.fare_amount', 'u.passenger_count', 'u.trip_distance']) }} as trip_id,
        
        -- identifiers
        u.vendor_id,
        u.ratecode_id,
        u.pickup_location_id,
        u.dropoff_location_id,
        u.service_type,
        u.payment_type,
        u.trip_type,

        -- timestamps
        u.pickup_datetime,
        u.dropoff_datetime,

        -- trip info
        u.passenger_count,
        u.trip_distance,
        u.store_and_forward_flag,

        -- amounts
        u.fare_amount,
        u.extra,
        u.mta_tax,        
        u.tip_amount,
        u.tolls_amount,
        u.ehail_fee,
        u.improvement_surcharge,
        u.congestion_surcharge,
        u.total_amount,

        -- enrichments
        {{ dbt.datediff('u.pickup_datetime', 'u.dropoff_datetime', 'minute') }} as trip_duration_minutes,
        {{ payment_type_lookup('u.payment_type') }} as payment_type_description

    from unioned_trips u
)

select * from cleaned_and_enriched
