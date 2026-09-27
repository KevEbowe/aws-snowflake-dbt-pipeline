"""Demo orchestration: API -> S3 NDJSON -> Snowflake COPY -> dbt -> snapshot -> marts."""
from __future__ import annotations
import json
import os
import subprocess
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

SQL_DIR = Path('/opt/airflow/snowflake')
DBT_DIR = '/opt/airflow/dbt'


def dbt(*args):
    subprocess.run(['dbt', *args, '--profiles-dir', DBT_DIR], cwd=DBT_DIR,
                   env=os.environ.copy(), check=True)


@dag(dag_id='cyber_api_to_snowflake', schedule='@hourly',
     start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
     catchup=False, max_active_runs=1,
     default_args={'retries': 2, 'retry_delay': timedelta(minutes=2)},
     tags=['portfolio', 'snowflake', 'dbt'])
def pipeline():
    @task
    def extract_and_land():
        api_url = os.environ.get('API_URL', 'http://mock-api:8000')
        bucket = os.environ['S3_BUCKET']
        hook = S3Hook(aws_conn_id='aws_default')
        now = datetime.now(timezone.utc)
        prefix = now.strftime('year=%Y/month=%m/day=%d')
        run_id = uuid.uuid4().hex
        for endpoint, s3_folder in [('events', 'security_events'), ('customers', 'customers')]:
            response = requests.get(f'{api_url}/{endpoint}', timeout=30)
            response.raise_for_status()
            rows = response.json()
            if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
                raise ValueError(f'Expected JSON array of objects from {endpoint}')
            if not rows:
                raise ValueError(f'No records returned by {endpoint}; refusing silent empty load')
            ndjson = '\n'.join(json.dumps(row) for row in rows) + '\n'
            key = f'raw/{s3_folder}/{prefix}/{run_id}.json'
            hook.load_string(string_data=ndjson, key=key,
                             bucket_name=bucket, replace=False)
            print(f'Uploaded {len(rows)} {endpoint} rows to s3://{bucket}/{key}')

    @task
    def copy_into_snowflake():
        sql = (SQL_DIR / 'copy_into.sql').read_text()
        SnowflakeHook(snowflake_conn_id='snowflake_default').run(sql, split_statements=True)

    @task
    def dbt_staging_and_intermediate():
        dbt('run', '--select', 'staging', 'intermediate')

    @task
    def dbt_customer_snapshot():
        dbt('snapshot')

    @task
    def dbt_marts_and_tests():
        dbt('run', '--select', 'marts')
        dbt('test')

    extract_and_land() >> copy_into_snowflake() >> dbt_staging_and_intermediate() >> dbt_customer_snapshot() >> dbt_marts_and_tests()

pipeline()
