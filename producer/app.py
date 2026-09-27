"""Local demonstration API: realistic events, updates, and mutable customers.
Replace API_URL in Airflow to use a real permitted source with the same schema.
In-memory state resets on container restart; no production reliability implied.
"""
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI

app = FastAPI(title="Cyber insurance demo API")
request_count = 0

@app.get("/events")
def events():
    global request_count
    request_count += 1
    now = datetime.now(timezone.utc)
    # e-100 is updated each call; e-200 arrives late on the second call.
    rows = [
        {"event_id": "e-100", "customer_id": "c-1", "event_type": "phishing",
         "severity": "high" if request_count % 2 else "critical", "risk_score": 78 + request_count,
         "event_timestamp": (now - timedelta(hours=1)).isoformat(), "updated_at": now.isoformat()},
        {"event_id": "e-300", "customer_id": "c-2", "event_type": "malware",
         "severity": "medium", "risk_score": 50,
         "event_timestamp": (now - timedelta(minutes=30)).isoformat(), "updated_at": now.isoformat()},
    ]
    if request_count >= 2:
        rows.append({"event_id": "e-200", "customer_id": "c-1", "event_type": "credential_stuffing",
                     "severity": "low", "risk_score": 30,
                     "event_timestamp": (now - timedelta(days=3)).isoformat(),
                     "updated_at": (now - timedelta(days=3)).isoformat()})
    # Intentional duplicate: dedupe in dbt by event_id and timestamps.
    rows.append(dict(rows[0]))
    return rows

@app.get("/customers")
def customers():
    now = datetime.now(timezone.utc).isoformat()
    return [
        {"customer_id": "c-1", "plan_tier": "premium" if request_count % 2 == 0 else "standard",
         "risk_segment": "high", "updated_at": now},
        {"customer_id": "c-2", "plan_tier": "standard", "risk_segment": "medium", "updated_at": "2026-01-01T00:00:00+00:00"},
    ]
