import os
import re
from datetime import datetime
from glob import glob
from zoneinfo import ZoneInfo

from pipelines.commons.s3_client import get_s3_client
from pipelines.commons.env_loader import BUCKET_AUDIT, validate_env, MINIO_ACCESS_KEY, MINIO_SECRET_KEY

AUDIT_LOG_DIR = "/opt/airflow/audit_logs"
AIRFLOW_LOG_DIR = os.getenv("AIRFLOW__LOGGING__BASE_LOG_FOLDER", "/opt/airflow/logs")


LOCAL_TZ = ZoneInfo("America/Sao_Paulo")


def _format_local_ts(logical_date: datetime) -> str:
    return logical_date.astimezone(LOCAL_TZ).strftime("%Y%m%d%H%M%S")


def upload_audit_log_to_minio(local_path: str, dag_id: str, logical_date: datetime):
    """Envia o log consolidado para o bucket de auditoria no MinIO (S3-compatible)."""
    validate_env({
        "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
        "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
    })
    s3 = get_s3_client()
    execution_ts = _format_local_ts(logical_date)
    object_key = f"airflow/{dag_id}_{execution_ts}.log"
    s3.upload_file(local_path, BUCKET_AUDIT, object_key)
    print(f"[AUDIT_MANAGER] log sent to MinIO: s3://{BUCKET_AUDIT}/{object_key}")
    return f"s3://{BUCKET_AUDIT}/{object_key}"


def _build_run_summary(context) -> str:
    """Monta um resumo de outcome (status geral + contagem de tasks + duração)
    a partir das task instances da dag_run atual."""
    dag_run = context.get("dag_run")

    if dag_run is None:
        return " STATUS: UNKNOWN (dag_run não disponível no contexto)\n"

    try:
        task_instances = dag_run.get_task_instances()
    except Exception:
        task_instances = []

    total = len(task_instances)
    by_state = {}
    for ti in task_instances:
        by_state[ti.state] = by_state.get(ti.state, 0) + 1

    success_count = by_state.get("success", 0)
    failed_count = by_state.get("failed", 0)
    skipped_count = by_state.get("skipped", 0)
    other_count = total - success_count - failed_count - skipped_count

    overall_status = "SUCCESS" if failed_count == 0 and total > 0 else "FAILED"

    start_date = dag_run.start_date
    end_date = dag_run.end_date or datetime.now(start_date.tzinfo if start_date else None)
    duration = "N/A"
    if start_date and end_date:
        total_seconds = int((end_date - start_date).total_seconds())
        minutes, seconds = divmod(total_seconds, 60)
        duration = f"{minutes}m{seconds}s"

    lines = [
        f" STATUS: {overall_status}",
        f" TASKS: {total} total, {success_count} success, {failed_count} failed, "
        f"{skipped_count} skipped, {other_count} other",
        f" DURATION: {duration}",
    ]
    return "\n".join(lines) + "\n"


def consolidate_dag_audit_logs(context):
    """Consolida os logs de todas as tasks de uma DAG run num único arquivo local,
    em ordem cronológica real, com um resumo de outcome no topo, e tenta enviar
    esse arquivo para o MinIO (best-effort, não derruba o callback)."""
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    logical_date = context.get("logical_date") or context.get("execution_date")
    execution_ts = _format_local_ts(logical_date)

    dag_audit_dir = os.path.join(AUDIT_LOG_DIR, dag_id)
    os.makedirs(dag_audit_dir, exist_ok=True)
    target_audit_path = os.path.join(dag_audit_dir, f"log_{dag_id}_{execution_ts}.log")

    dag_log_path = os.path.join(AIRFLOW_LOG_DIR, f"dag_id={dag_id}")
    all_logs = glob(os.path.join(dag_log_path, "**", "*.log"), recursive=True)

    ts_nodash = context.get("ts_nodash", "")
    matching_logs = [
        f for f in all_logs if run_id in f or (ts_nodash and ts_nodash in f)
    ]
    # Ordena por ordem real de execução (mtime), não por nome de arquivo/task_id.
    run_logs = sorted(matching_logs, key=os.path.getmtime)

    with open(target_audit_path, "w", encoding="utf-8") as audit_file:
        audit_file.write("=" * 80 + "\n")
        audit_file.write(f" AUDIT TRAIL LOG - DAG: {dag_id}\n")
        audit_file.write(f" EXECUTION RUN ID: {run_id}\n")
        generated_at_local = datetime.now(ZoneInfo("UTC")).astimezone(LOCAL_TZ)
        audit_file.write(f" GENERATED AT: {generated_at_local.strftime('%Y-%m-%d %H:%M:%S')} (GMT-3)\n")
        audit_file.write(_build_run_summary(context))
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

    # Upload para o object storage (best-effort: falha aqui não deve derrubar o callback).
    try:
        object_uri = upload_audit_log_to_minio(target_audit_path, dag_id, logical_date)
        upload_status_line = f"[AUDIT_MANAGER] log successfuly sento to : {object_uri}\n"
    except Exception as e:
        upload_status_line = f"[AUDIT_MANAGER][ERROR] fail sending log to s3 {e}\n"
        print(upload_status_line.strip())

    # Registra o resultado do upload de volta no arquivo local, para que o
    # arquivo consolidado seja autocontido (não dependa do stdout do scheduler
    # para saber se o envio ao S3 funcionou).
    with open(target_audit_path, "a", encoding="utf-8") as audit_file:
        audit_file.write("\n" + "=" * 80 + "\n")
        audit_file.write(upload_status_line)
        audit_file.write("=" * 80 + "\n")

    # Reenvia a versão final (já com o rodapé) para sobrescrever, no MinIO, a
    # cópia que foi enviada antes do rodapé existir. Falha aqui é apenas
    # cosmética (a cópia local já está completa) e não deve gerar novo erro.
    try:
        upload_audit_log_to_minio(target_audit_path, dag_id, logical_date)
    except Exception:
        pass
