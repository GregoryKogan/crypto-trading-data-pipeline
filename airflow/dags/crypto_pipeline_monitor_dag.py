from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.sql import SQLCheckOperator

with DAG(
    dag_id="crypto_pipeline_monitor_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule="*/5 * * * *",  # Run every 5 minutes
    tags=["crypto", "monitoring", "data_quality"],
    doc_md="""
    ### Crypto Pipeline Monitoring DAG

    This DAG runs periodically to check the freshness of the data in the PostgreSQL database.
    It fails if no new data has been written in the last 3 minutes, indicating a potential issue
    with the streaming pipeline.
    """,
) as dag:
    check_data_freshness = SQLCheckOperator(
        task_id="check_data_freshness",
        conn_id="postgres_default",  # Connection configured in Airflow UI
        sql="""
        SELECT COUNT(*) FROM trades_1min_agg WHERE window_start >= NOW() - INTERVAL '3 minutes';
        """,
    )
