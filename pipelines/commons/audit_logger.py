import os
import re
from datetime import datetime
from glob import glob

from pipelines.commons.s3_client import get_s3_client
from pipelines.commons.env_loader import BUCKET_AUDIT, validate_env, MINIO_ACCESS_KEY, MINIO_SECRET_KEY

AUDIT_LOG_DIR = "/opt/airflow/audit_logs"
AIRFLOW_LOG_DIR = os.getenv("AIRFLOW__LOGGING__BASE_LOG_FOLDER", "/opt/airflow/logs")


def upload_audit_log_to_minio(local_path: str, dag_id: str, logical_date: datetime):
    """Envia o log consolidado para o bucket de auditoria no MinIO (S3-compatible)."""
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
    """Consolida os logs de todas as tasks de uma DAG run num único arquivo local,
    e tenta enviar esse arquivo para o MinIO (best-effort, não derruba o callback)."""
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