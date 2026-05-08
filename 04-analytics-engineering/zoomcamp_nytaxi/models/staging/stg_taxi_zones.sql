{{ config(materialized='view') }}

select
    cast(locationid as {{ dbt.type_int() }}) as location_id,
    cast(borough as {{ dbt.type_string() }}) as borough,
    cast(zone as {{ dbt.type_string() }}) as zone,
    cast(service_zone as {{ dbt.type_string() }}) as service_zone
from {{ source('main', 'taxi_zones') }}
