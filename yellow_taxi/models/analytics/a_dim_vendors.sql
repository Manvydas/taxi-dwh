with source as (

    select
        id,
        description,
        current_timestamp() as insertion_timestamp,
    from {{ ref('dim_vendors') }}

)
select *
from source