SELECT *
FROM {{ ref('stg_customers') }}
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY customer_id
  ORDER BY updated_at DESC NULLS LAST, ingestion_timestamp DESC NULLS LAST
) = 1
