SELECT
    COUNT(*) AS total_trip,
    SUM(total_amount) AS total_revenue

FROM 
    {{ref('stg_uber')}}