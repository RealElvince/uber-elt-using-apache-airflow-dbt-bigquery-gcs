SELECT 
  CASE 
    WHEN EXTRACT(DAYOFWEEK FROM tpep_pickup_datetime) IN (1, 7) THEN 'Weekend'
    ELSE 'Weekday'
  END AS trip_day_type,
  COUNT(*) AS trip_count
FROM {{ ref('stg_uber') }}
GROUP BY trip_day_type
