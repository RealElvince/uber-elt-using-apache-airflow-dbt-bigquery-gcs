SELECT 
  payment_type,
  SUM(total_amount) AS total_revenue
FROM {{ ref('stg_uber') }}
GROUP BY payment_type
