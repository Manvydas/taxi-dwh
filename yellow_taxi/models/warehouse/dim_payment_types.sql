with source as (

    select distinct payment_type AS id, from {{ source('yellow_taxi', 'yellow_tripdata_2019_12') }}

)
select 
    id,
    CASE
        WHEN id = 1 THEN 'Credit card'
        WHEN id = 2 THEN 'Cash'
        WHEN id = 3 THEN 'No charge'
        WHEN id = 4 THEN 'Dispute'
        WHEN id = 5 THEN 'Unknown'
        WHEN id = 6 THEN 'Voided trip'
        ELSE NULL END
    AS description,
    current_timestamp() as ingestion_timestamp,
from source
WHERE id IN (1,2,3,4,5,6)