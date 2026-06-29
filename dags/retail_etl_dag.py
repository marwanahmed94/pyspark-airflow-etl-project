from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "marwan",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="retail_etl_pipeline",
    default_args=default_args,
    description="Retail ETL pipeline using PySpark",
    start_date=datetime(2026, 6, 1),
    schedule_interval=None,
    catchup=False,
    tags=["retail", "etl", "pyspark"],
) as dag:

    extract_task = BashOperator(
        task_id="extract_data",
        bash_command="python /opt/airflow/project/scripts/extract.py",
    )

    transform_task = BashOperator(
        task_id="transform_data",
        bash_command="python /opt/airflow/project/scripts/transform.py",
    )

    load_task = BashOperator(
        task_id="load_data",
        bash_command="python /opt/airflow/project/scripts/load.py",
    )

    extract_task >> transform_task >> load_task