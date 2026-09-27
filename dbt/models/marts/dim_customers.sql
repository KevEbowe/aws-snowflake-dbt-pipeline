-- One row per customer CURRENTLY active in SCD2 snapshot.
SELECT customer_id, plan_tier, risk_segment, dbt_valid_from
FROM {{ ref('customer_history') }}
WHERE dbt_valid_to IS NULL
