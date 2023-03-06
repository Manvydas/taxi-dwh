with source as (
    select
        id,
        pickup_location_id,
        dropoff_location_id,
        payment_type_id,
        vendor_id,
        rate_code_id,
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
        store_and_fwd_flag,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        TIMESTAMP_DIFF(tpep_dropoff_datetime, tpep_pickup_datetime, SECOND) AS trip_time_second,
        current_timestamp() as insertion_timestamp,
    from {{ ref('fact_trips') }}
)
select * 
from source
