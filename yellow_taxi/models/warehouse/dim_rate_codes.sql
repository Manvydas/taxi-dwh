with source as (

    select distinct RatecodeID AS id, from {{ source('yellow_taxi', 'yellow_tripdata_2019_12') }}

)
select 
    id,
    CASE
        WHEN id = 1 THEN 'Standard rate'
        WHEN id = 2 THEN 'JFK'
        WHEN id = 3 THEN 'Newark'
        WHEN id = 4 THEN 'Nassau or Westchester'
        WHEN id = 5 THEN 'Negotiated fare'
        WHEN id = 6 THEN 'Group ride'
        ELSE NULL END
    AS description,
    current_timestamp() as ingestion_timestamp,
from source
WHERE id IN (1,2,3,4,5,6)