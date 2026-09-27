SELECT customer_id, plan_tier, risk_segment, updated_at, ingestion_timestamp
FROM {{ source('raw', 'customers') }}
WHERE customer_id IS NOT NULL
