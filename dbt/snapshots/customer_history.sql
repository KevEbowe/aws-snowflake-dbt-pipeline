{% snapshot customer_history %}
{{ config(
    target_schema='SNAPSHOTS',
    unique_key='customer_id',
    strategy='timestamp',
    updated_at='updated_at',
    invalidate_hard_deletes=False
) }}
SELECT customer_id, plan_tier, risk_segment, updated_at
FROM {{ ref('int_latest_customers') }}
{% endsnapshot %}
