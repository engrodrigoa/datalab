from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
import sys, os
import os
import re
from datetime import datetime
from glob import glob


#sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from pipelines.commons.s3_client import ensure_buckets_exist, get_s3_client
from pipelines.commons.env_loader import REQUIRED_BUCKETS, BUCKET_AUDIT, validate_env, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
from pipelines.commons.audit_logger import consolidate_dag_audit_logs


#################
AUDIT_LOG_DIR = "/opt/airflow/audit_logs"
AIRFLOW_LOG_DIR = os.getenv("AIRFLOW__LOGGING__BASE_LOG_FOLDER", "/opt/airflow/logs")


def upload_audit_log_to_minio(local_path: str, dag_id: str, logical_date: datetime):
    validate_env({
        "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
        "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
    })
    s3 = get_s3_client()
    execution_ts = logical_date.strftime("%Y%m%d%H%M%S")
    object_key = f"airflow/{dag_id}_{execution_ts}.log"
    s3.upload_file(local_path, BUCKET_AUDIT, object_key)
    print(f"[AUDIT_MANAGER] log sent to MinIO: s3://{BUCKET_AUDIT}/{object_key}")


def consolidate_dag_audit_logs(context):
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    logical_date = context.get("logical_date") or context.get("execution_date")
    execution_ts = logical_date.strftime("%Y%m%d%H%M%S")

    dag_audit_dir = os.path.join(AUDIT_LOG_DIR, dag_id)
    os.makedirs(dag_audit_dir, exist_ok=True)
    target_audit_path = os.path.join(dag_audit_dir, f"log_{dag_id}_{execution_ts}.log")

    dag_log_path = os.path.join(AIRFLOW_LOG_DIR, f"dag_id={dag_id}")
    all_logs = glob(os.path.join(dag_log_path, "**", "*.log"), recursive=True)

    ts_nodash = context.get("ts_nodash", "")
    run_logs = sorted(
        [f for f in all_logs if run_id in f or (ts_nodash and ts_nodash in f)]
    )

    with open(target_audit_path, "w", encoding="utf-8") as audit_file:
        audit_file.write("=" * 80 + "\n")
        audit_file.write(f" AUDIT TRAIL LOG - DAG: {dag_id}\n")
        audit_file.write(f" EXECUTION RUN ID: {run_id}\n")
        audit_file.write(f" GENERATED AT: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        audit_file.write("=" * 80 + "\n\n")

        if not run_logs:
            audit_file.write("[WARN] Nenhum arquivo de log individual foi encontrado para esta execução.\n")

        for log_path in run_logs:
            task_match = re.search(r"task_id=([^/]+)", log_path)
            task_id = task_match.group(1) if task_match else "UNKNOWN_TASK"
            audit_file.write("\n" + "#" * 80 + "\n")
            audit_file.write(f"--- TASK EXECUTION: {task_id} ---\n")
            audit_file.write("#" * 80 + "\n\n")
            try:
                with open(log_path, "r", encoding="utf-8") as f:
                    audit_file.write(f.read())
            except Exception as e:
                audit_file.write(f"[ERROR] Falha ao ler log da task {task_id}: {e}\n")

    print(f"[AUDIT_MANAGER] Arquivo consolidado salvo em: {target_audit_path}")

    try:
        upload_audit_log_to_minio(target_audit_path, dag_id, logical_date)
    except Exception as e:
        print(f"[AUDIT_MANAGER][ERROR] Falha ao enviar log para MinIO: {e}")
################
default_args = {
    'owner': 'datalab',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def _create_buckets():
        ensure_buckets_exist(REQUIRED_BUCKETS)

with DAG(
    dag_id='dag_setup_infrastructure',
    default_args=default_args,
    description='infrastructure setup',
    schedule_interval=None,  
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['setup', 'polars', 'ddl', 'anp'],
    on_success_callback=consolidate_dag_audit_logs,
    on_failure_callback=consolidate_dag_audit_logs,
) as dag:

    # ==========================================
    # schemas
    # ==========================================
    create_schemas = PostgresOperator(
        task_id='create_schemas',
        postgres_conn_id='postgres_default', 
        sql="""
            CREATE SCHEMA IF NOT EXISTS ctrl;
            CREATE SCHEMA IF NOT EXISTS audit;
            CREATE SCHEMA IF NOT EXISTS bronze;
            CREATE SCHEMA IF NOT EXISTS silver;
            CREATE SCHEMA IF NOT EXISTS gold;
        """,
    )

    # ==========================================
    # object storage (MinIO / S3)
    # ==========================================
    create_buckets = PythonOperator(
        task_id='create_buckets',
        python_callable=_create_buckets,
    )

    # ==========================================
    # control tables
    # ==========================================
    create_ctrl_tables = PostgresOperator(
        task_id='create_ctrl_tables',
        postgres_conn_id='postgres_default',
        sql="""
            CREATE TABLE IF NOT EXISTS ctrl.anp_metadata_mensal (
                cat varchar(20) NOT NULL,
                "ref" varchar(6) NOT NULL,
                "month" int4 NOT NULL,
                "year" int4 NOT NULL,
                file_name varchar(255) NOT NULL,
                url_source text NOT NULL,
                download_date timestamp DEFAULT CURRENT_TIMESTAMP NULL,
                link_name varchar(30) NULL,
                CONSTRAINT pk_control_anp PRIMARY KEY (cat, ref)
            );

            CREATE TABLE IF NOT EXISTS ctrl.anp_metadata_semanal (
                data_ref date NULL,
                data_dag_run timestamp NULL,
                status varchar NULL
            );

            CREATE TABLE IF NOT EXISTS audit.dbt_runs (
                run_timestamp timestamp NULL,
                invocation_id text NOT NULL,
                node_id text NOT NULL,
                status text NULL,
                execution_time_sec float8 NULL,
                failures int4 NULL,
                message text NULL,
                resource_type text NULL,
                "database" text NULL,
                schema_layer text NULL,
                "materialized" text NULL,
                tags text NULL,
                data_carga timestamp NULL,
                CONSTRAINT dbt_runs_pkey PRIMARY KEY (invocation_id, node_id)
            );
        """,
    )

    # ==========================================
    # bronze tables
    # ==========================================
    create_bronze_tables = PostgresOperator(
        task_id='create_bronze_tables',
        postgres_conn_id='postgres_default',
        sql="""
            CREATE TABLE IF NOT EXISTS bronze.anp_landing_mensal (
                regiao_sigla text NULL,
                estado_sigla text NULL,
                municipio text NULL,
                revenda text NULL,
                cnpj_da_revenda text NULL,
                nome_da_rua text NULL,
                numero_rua text NULL,
                complemento text NULL,
                bairro text NULL,
                cep text NULL,
                produto text NULL,
                data_da_coleta text NULL,
                valor_de_venda text NULL,
                valor_de_compra text NULL,
                unidade_de_medida text NULL,
                bandeira text NULL,
                arquivo text NULL,
                ingestion_timestamp timestamp NULL
            );

            CREATE TABLE IF NOT EXISTS bronze.anp_landing_semanal (
                regiao_sigla text NULL,
                estado_sigla text NULL,
                municipio text NULL,
                revenda text NULL,
                cnpj_da_revenda text NULL,
                nome_da_rua text NULL,
                numero_rua text NULL,
                complemento text NULL,
                bairro text NULL,
                cep text NULL,
                produto text NULL,
                data_da_coleta text NULL,
                valor_de_venda text NULL,
                valor_de_compra text NULL,
                unidade_de_medida text NULL,
                bandeira text NULL,
                arquivo text NULL,
                ingestion_timestamp timestamp NULL
            );
        """,
    )

    # ==========================================
    # flow
    # ==========================================
    create_buckets >> create_schemas >> create_ctrl_tables >> create_bronze_tables