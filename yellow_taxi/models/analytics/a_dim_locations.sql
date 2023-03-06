with source as (

    select
        id,
        borough,
        zone,
        service_zone,
        current_timestamp() as insertion_timestamp,
    from {{ ref('dim_locations') }}

)
select *
from source