with source as (

    select
        id,
        description,
        current_timestamp() as insertion_timestamp,
    from {{ ref('dim_payment_types') }}

)
select *
from source