SELECT 
    DATE(tpep_pickup_datetime) AS trip_date,
    COUNT(*) AS trips,
FROM 
    {{ref('stg_uber')}}

GROUP BY 
    trip_date