from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
import sys, os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from pipelines.commons.s3_client import ensure_buckets_exist
from pipelines.commons.env_loader import REQUIRED_BUCKETS

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
    tags=['setup', 'polars' 'ddl', 'anp'],
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