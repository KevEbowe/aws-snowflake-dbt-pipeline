SELECT DATE_TRUNC('day', event_timestamp)::DATE AS event_date,
       severity, COUNT(*) AS event_count,
       AVG(risk_score) AS avg_risk_score
FROM {{ ref('fct_security_events') }}
GROUP BY 1, 2
