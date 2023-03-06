with source as (

    select
        ROW_NUMBER() OVER(ORDER BY tpep_pickup_datetime, tpep_dropoff_datetime) AS id,
        PULocationID AS pickup_location_id,
        DOLocationID AS dropoff_location_id,
        payment_type AS payment_type_id,
        VendorID AS vendor_id,
        RatecodeID AS rate_code_id,
        passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee,
        CASE
            WHEN store_and_fwd_flag = 'Y' THEN True
            WHEN store_and_fwd_flag = 'N' THEN False
            ELSE NULL END
        AS store_and_fwd_flag,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
    from {{ source('yellow_taxi', 'yellow_tripdata_2019_12') }}
    where EXTRACT(YEAR FROM tpep_pickup_datetime) = 2019
        AND EXTRACT(MONTH FROM tpep_pickup_datetime) = 12
        AND payment_type IN (1,2,3,4,5,6)
        AND VendorID IN (1,2)
        AND ratecodeid IN (1,2,3,4,5,6)
        AND passenger_count NOT IN (0)

)
select
    *,
    current_timestamp() as ingestion_timestamp,
from source