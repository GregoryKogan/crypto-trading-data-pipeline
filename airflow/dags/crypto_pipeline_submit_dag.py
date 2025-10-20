from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="crypto_pipeline_submit_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["crypto", "spark", "streaming"],
    doc_md="""
    ### Crypto Pipeline Submit DAG

    This DAG is responsible for submitting the Spark Structured Streaming job to the Spark cluster.
    It should be triggered manually to start the real-time data processing pipeline.
    """,
) as dag:
    submit_spark_job = SparkSubmitOperator(
        task_id="submit_spark_streaming_job",
        application="/opt/spark/app/processor.py",
        conn_id="spark_default",
        verbose=True,
        conf={"spark.master": "spark://spark-master:7077"},
        env_vars={
            "KAFKA_BROKER_URL": "kafka:9092",
            "KAFKA_TOPIC": "raw_trades",
            "POSTGRES_HOST": "postgres",
            "POSTGRES_PORT": "5432",
            "POSTGRES_DB": "crypto_data",
            "POSTGRES_USER": "user",
            "POSTGRES_PASSWORD": "password",
            "CHECKPOINT_LOCATION": "/opt/spark/app/checkpoint",
        },
        jars="/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar,\
            /opt/spark/jars/kafka-clients-3.7.0.jar,\
            /opt/spark/jars/postgresql-42.7.3.jar,\
            /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar,\
            /opt/spark/jars/commons-pool2-2.12.0.jar",
    )
