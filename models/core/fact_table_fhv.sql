{{ config(materialized='table') }}

with fhv_data as (
    select *, 
        'Fhv' as service_type 
    from {{ ref('stg_fhv_tripdata') }}
),

dim_zones as (
    select * from {{ ref('taxi_zone_lookup') }}
    where borough != 'Unknown'
)
select 
    fhv_data.dispatching_base_num,
    fhv_data.PULocationID,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone,  
    fhv_data.pickup_datetime,
    fhv_data.DOLocationID,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone, 
    fhv_data.dropoff_datetime,
    fhv_data.SR_Flag,
    fhv_data.Affiliated_base_number
from fhv_data
inner join dim_zones as pickup_zone
on fhv_data.PULocationID = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_data.DOLocationID = dropoff_zone.locationid