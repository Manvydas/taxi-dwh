with source as (
    select
        trips.id AS trip_id,
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
        vendors.description AS vendor_description,
        rate_codes.description AS rate_code_description,
        payment_types.description AS payment_type_description,
        pickup_locations.borough AS pickup_location_borough,
        pickup_locations.zone AS pickup_locations_zone,
        pickup_locations.service_zone AS pickup_locations_service_zone,
        dropoff_locations.borough AS dropoff_location_borough,
        dropoff_locations.zone AS dropoff_locations_zone,
        dropoff_locations.service_zone AS dropoff_locations_service_zone,
        current_timestamp() as insertion_timestamp,
    from {{ ref('fact_trips') }} AS trips
    left join {{ ref('dim_vendors') }} AS vendors
        on vendors.id = trips.vendor_id
    left join {{ ref('dim_rate_codes') }} AS rate_codes
        on rate_codes.id = trips.rate_code_id
    left join {{ ref('dim_payment_types') }} AS payment_types
        on payment_types.id = trips.payment_type_id
    left join {{ ref('dim_locations') }} AS pickup_locations
        on pickup_locations.id = trips.pickup_location_id
    left join {{ ref('dim_locations') }} AS dropoff_locations
        on dropoff_locations.id = trips.dropoff_location_id

)
select * 
from source
