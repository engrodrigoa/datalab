import os, sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator  # type: ignore
from airflow.operators.empty import EmptyOperator  # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # type: ignore
from airflow.utils.trigger_rule import TriggerRule  # type: ignore

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from pipelines.commons.audit_logger import consolidate_dag_audit_logs

default_args = {
    'owner': 'datalab',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dag_rfb',
    default_args=default_args,
    description='pipeline for rfb webdav download and bronze parquet conversion',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['rfb', 'bronze', 's3', 'polars', 'parquet'],
    on_success_callback=consolidate_dag_audit_logs,
    on_failure_callback=consolidate_dag_audit_logs,
) as dag:

 
    trigger_infra_setup = TriggerDagRunOperator(
        task_id='trigger_infra_setup',
        trigger_dag_id='dag_setup_infrastructure',
        wait_for_completion=True,
        poke_interval=10,
    )

    task_download_files = BashOperator(
            task_id="task_download_files",
            bash_command="python3 -u /opt/airflow/pipelines/rfb/rfb01_download_files.py",
            execution_timeout=timedelta(minutes=260),
    )

    task_extract_to_bronze = BashOperator(
            task_id="task_extract_to_bronze",
            bash_command="python3 -u /opt/airflow/pipelines/rfb/rfb02_extract_zip.py",
            execution_timeout=timedelta(minutes=260),
    )

    trigger_infra_setup >> task_download_files >> task_extract_to_bronze