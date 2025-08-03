SELECT 
   VendorID,
   COUNT(*) AS trip_count
FROM 
    {{ ref('stg_uber') }}
GROUP BY VendorID
