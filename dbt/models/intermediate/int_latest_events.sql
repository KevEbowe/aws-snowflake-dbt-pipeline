-- Full-history dedup BEFORE incremental filtering handles late-arriving updates.
SELECT *
FROM {{ ref('stg_security_events') }}
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY event_id
  ORDER BY updated_at DESC NULLS LAST, ingestion_timestamp DESC NULLS LAST
) = 1
