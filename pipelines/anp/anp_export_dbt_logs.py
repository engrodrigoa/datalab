import polars as pl
import json
import boto3
from datetime import datetime
from dotenv import load_dotenv
from warnings import filterwarnings
import os
import sys
import psycopg2

filterwarnings("ignore")
load_dotenv()

# ==========================================
# 1. VALIDAÇÃO DE CREDENCIAIS (Trava de Segurança)
# ==========================================
REQUIRED_VARS = ["MINIO_ENDPOINT", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY"]
missing_vars = [var for var in REQUIRED_VARS if not os.getenv(var)]
if missing_vars:
    print(f"[ERRO FATAL] Variáveis S3 ausentes no .env: {', '.join(missing_vars)}")
    sys.exit(1)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
endpoint_limpo = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
BUCKET_DATASOURCE = "audit"

REQUIRED_PG_VARS = ["DB_HOST", "DB_PORT", "DB_USER", "DB_PASS", "DB_NAME"]
missing_vars = [var for var in REQUIRED_PG_VARS if not os.getenv(var)]
if missing_vars:
    print(f"[ERRO FATAL] Variáveis PG ausentes no .env: {', '.join(missing_vars)}")
    sys.exit(1)

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")
CONSTRING = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Configurações de Paths
TARGET_DIR = "/opt/airflow/dags/pipelines/dbt_projects/target" #"/home/lambda2/lambda2/airflow/dags/pipelines/dbt_projects/target"  #
RUN_RESULTS_PATH = f"{TARGET_DIR}/run_results.json"
MANIFEST_PATH = f"{TARGET_DIR}/manifest.json"

# ==========================================
# 2. DEFINIÇÃO DAS FUNÇÕES MODULARES
# ==========================================
def backup_logs_to_s3(timestamp_str):
    """Copia os artefatos brutos para o MinIO (Camada Bronze)."""
    s3_client = boto3.client(
        's3',
        endpoint_url=f"http://{endpoint_limpo}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    
    s3_client.upload_file(
        Filename=RUN_RESULTS_PATH, 
        Bucket=BUCKET_DATASOURCE, 
        Key=f"dbt/run_results/run_results_{timestamp_str}.json"
    )
    s3_client.upload_file(
        Filename=MANIFEST_PATH, 
        Bucket=BUCKET_DATASOURCE, 
        Key=f"dbt/manifests/manifest_{timestamp_str}.json"
    )
    print(f"-> Backup S3 concluído (Sufixo: {timestamp_str})")


def process_dbt_artifacts(now):
    """Lê os JSONs usando Polars e retorna um DataFrame desnormalizado enriquecido."""
    # Parsing da Tabela Fato
    df_runs = (
        pl.read_json(RUN_RESULTS_PATH)
        .explode('results')
        .unnest('metadata')
        .with_columns(
            (pl.col('generated_at').cast(pl.Datetime) - pl.duration(hours=3)).alias('run_timestamp'),
            pl.col('results').struct.field('unique_id').alias('node_id'),
            pl.col('results').struct.field('status').alias('status'),
            pl.col('results').struct.field('execution_time').alias('execution_time_sec'),
            pl.col('results').struct.field('failures').alias('failures'),
            pl.col('results').struct.field('message').alias('message')
        )
        .select('run_timestamp', 'invocation_id', 'node_id', 'status', 'execution_time_sec', 'failures', 'message')
    )

    # Parsing da Tabela Dimensão
    with open(MANIFEST_PATH, 'r') as f:
        manifest_data = json.load(f)

    parsed_nodes = [
        {
            "node_id": node_id,
            "resource_type": info.get("resource_type"),
            "database": info.get("database"),
            "schema_layer": info.get("schema"),
            "materialized": info.get("config", {}).get("materialized"),
            "tags": info.get("tags", [])
        }
        for node_id, info in manifest_data.get('nodes', {}).items()
    ]
    df_manifest = pl.DataFrame(parsed_nodes)

    # Enriquecimento final
    df_final_log = (
        df_runs
        .join(df_manifest, on='node_id', how='left')
        .with_columns(
            pl.lit(now).cast(pl.Datetime('us')).alias('data_carga'),
            pl.col('tags').list.join(',').alias('tags') # Evita erro do psycopg2
        )
    )
    print("-> Processamento Polars concluído.")
    return df_final_log


def load_logs_to_postgres(df_final_log):
    """Grava na Staging temporária e faz o Upsert (Merge) para a tabela oficial."""
    # 1. Grava na Staging
    df_final_log.write_database(
        table_name="audit.dbt_runs_stg", 
        connection=CONSTRING, 
        if_table_exists="replace",
        engine="sqlalchemy"
    )

    # 2. Cláusula de Merge Idempotente
    merge_query = """
        INSERT INTO audit.dbt_runs (
            run_timestamp, invocation_id, node_id, status, execution_time_sec, 
            failures, message, resource_type, database, schema_layer, 
            materialized, tags, data_carga
        )
        SELECT 
            run_timestamp, invocation_id, node_id, status, execution_time_sec, 
            failures, message, resource_type, database, schema_layer, 
            materialized, tags, data_carga
        FROM audit.dbt_runs_stg
        ON CONFLICT (invocation_id, node_id) DO NOTHING;
        
        DROP TABLE audit.dbt_runs_stg;
    """

    # 3. Execução nativa no banco
    conn = psycopg2.connect(CONSTRING)
    cur = conn.cursor()
    cur.execute(merge_query)
    conn.commit()
    cur.close()
    conn.close()
    print("-> Upsert no PostgreSQL concluído com Idempotência.")

# ==========================================
# 3. ORQUESTRAÇÃO PRINCIPAL (Ponto de Entrada)
# ==========================================
def main():
    print("Iniciando rotina de Observabilidade do dbt...")
    
    # Congela o tempo global da execução
    now = datetime.now()
    timestamp_str = now.strftime('%Y%m%d_%H%M%S')
    
    # 1. Backup Físico
    backup_logs_to_s3(timestamp_str)
    
    # 2. Processamento Analítico
    df_final_log = process_dbt_artifacts(now)
    
    # 3. Carga no Banco (Data Warehouse)
    load_logs_to_postgres(df_final_log)
    
    print("Rotina finalizada com sucesso.")

if __name__ == "__main__":
    main()