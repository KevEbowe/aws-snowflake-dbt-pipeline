COPY INTO CYBER_INSURANCE.RAW.SECURITY_EVENTS
(
    event_id,
    customer_id,
    event_type,
    severity,
    risk_score,
    event_timestamp,
    updated_at
)
FROM (
    SELECT
        $1:event_id::VARCHAR,
        $1:customer_id::VARCHAR,
        $1:event_type::VARCHAR,
        $1:severity::VARCHAR,
        $1:risk_score::NUMBER,
        $1:event_timestamp::TIMESTAMP_NTZ,
        $1:updated_at::TIMESTAMP_NTZ
    FROM @CYBER_INSURANCE.RAW.SECURITY_EVENTS_STAGE
);

COPY INTO CYBER_INSURANCE.RAW.CUSTOMERS
(
    customer_id,
    plan_tier,
    risk_segment,
    updated_at
)
FROM (
    SELECT
        $1:customer_id::VARCHAR,
        $1:plan_tier::VARCHAR,
        $1:risk_segment::VARCHAR,
        $1:updated_at::TIMESTAMP_NTZ
    FROM @CYBER_INSURANCE.RAW.CUSTOMERS_STAGE
);