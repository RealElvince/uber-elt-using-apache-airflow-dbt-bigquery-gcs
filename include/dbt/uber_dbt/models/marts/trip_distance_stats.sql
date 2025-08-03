SELECT 
    MIN(trip_distance) AS min_distance,
    MAX(trip_distance) AS max_distance,
    AVG(trip_distance) AS avg_distance
FROM {{ ref('stg_uber') }}
