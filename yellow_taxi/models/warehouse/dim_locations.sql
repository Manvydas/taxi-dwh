with source as (

    select PULocationID AS id, from {{ source('yellow_taxi', 'yellow_tripdata_2019_12') }}
    UNION ALL
    select DOLocationID AS id, from {{ source('yellow_taxi', 'yellow_tripdata_2019_12') }}

)
select distinct
    source.id,
    z_lookup.borough,
    z_lookup.zone,
    z_lookup.service_zone,
    current_timestamp() as ingestion_timestamp,
from source
left join {{ source('yellow_taxi', 'taxi_zones_lookup') }} AS z_lookup
    ON z_lookup.LocationID = source.id