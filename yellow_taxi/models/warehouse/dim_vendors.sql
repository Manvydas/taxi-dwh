with source as (

    select VendorID AS id,
    from {{ source('yellow_taxi', 'yellow_tripdata_2019_12') }}
    GROUP BY 1

)
select 
    id,
    CASE
        WHEN id = 1 THEN 'Creative Mobile Technologies'
        WHEN id = 2 THEN 'VeriFone Inc.'
        ELSE NULL END
    AS description,
    current_timestamp() as ingestion_timestamp,
from source
WHERE id IN (1,2)