SELECT 
    passenger_count,
    AVG(fare_amount) AS avg_fare
FROM    
    {{ref('stg_uber')}}
GROUP BY
    passenger_count