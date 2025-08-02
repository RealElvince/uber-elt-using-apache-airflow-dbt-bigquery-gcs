SELECT *
FROM {{ source('uber_source', 'uber_data') }}
