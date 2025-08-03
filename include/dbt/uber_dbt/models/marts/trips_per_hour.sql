SELECT 
  EXTRACT(HOUR FROM tpep_pickup_datetime) AS pickup_hour,
  COUNT(*) AS trip_count
FROM {{ ref('stg_uber') }}
GROUP BY pickup_hour
ORDER BY pickup_hour
