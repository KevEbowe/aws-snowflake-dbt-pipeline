SELECT
  TRIM(event_id) AS event_id,
  TRIM(customer_id) AS customer_id,
  LOWER(TRIM(event_type)) AS event_type,
  LOWER(TRIM(severity)) AS severity,
  risk_score,
  event_timestamp,
  updated_at,
  ingestion_timestamp
FROM {{ source('raw', 'security_events') }}
WHERE event_id IS NOT NULL
