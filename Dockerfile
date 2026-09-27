FROM apache/airflow:3.0.6
USER airflow
RUN pip install --no-cache-dir \
    'apache-airflow-providers-amazon>=9,<10' \
    'apache-airflow-providers-snowflake>=6,<8' \
    'dbt-snowflake>=1.9,<2' \
    'requests>=2.31,<3'
