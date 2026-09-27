{{ config(materialized='incremental', incremental_strategy='merge', unique_key='event_id',
          on_schema_change='sync_all_columns') }}
SELECT
  event_id, customer_id, event_type, severity, risk_score,
  event_timestamp, updated_at,
  ingestion_timestamp AS source_ingested_at
FROM {{ ref('int_latest_events') }}
{% if is_incremental() %}
-- Look back by INGESTION time, not event time, so a 3-day-old event
-- first ingested today is still included.
WHERE ingestion_timestamp >= (
  SELECT DATEADD(day, -2, COALESCE(MAX(source_ingested_at), '1900-01-01'::TIMESTAMP_NTZ))
  FROM {{ this }}
)
{% endif %}
