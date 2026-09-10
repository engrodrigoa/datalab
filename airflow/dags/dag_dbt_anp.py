import os
import re
from datetime import datetime, timedelta
from glob import glob

from airflow import DAG
from airflow.operators.bash import BashOperator  # type: ignore
from airflow.operators.empty import EmptyOperator  # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # type: ignore
from airflow.utils.trigger_rule import TriggerRule  # type: ignore

# Importações do Astronomer Cosmos
from cosmos import (  # type: ignore
    DbtTaskGroup,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)

# Caminhos do ambiente
DBT_PROJECT_DIR = "/opt/airflow/pipelines/dbt_projects"
DBT_EXECUTABLE_PATH = "/opt/airflow/dbt_venv/bin/dbt"
EDR_EXECUTABLE_PATH = "/opt/airflow/dbt_venv/bin/edr"

# Configurações de Auditoria de Logs da DAG
AUDIT_LOG_DIR = "/opt/airflow/audit_logs/anp"
AIRFLOW_LOG_DIR = os.getenv("AIRFLOW__LOGGING__BASE_LOG_FOLDER", "/opt/airflow/logs")


def consolidate_dag_audit_logs(context):

    dag_id = context["dag"].dag_id
    run_id = context["run_id"]

    # Formata a data/hora para o padrão log_anp20260910180000.log
    logical_date = context.get("logical_date") or context.get("execution_date")
    execution_ts = logical_date.strftime("%Y%m%d%H%M%S")
    audit_filename = f"log_anp_{execution_ts}.log"

    os.makedirs(AUDIT_LOG_DIR, exist_ok=True)
    target_audit_path = os.path.join(AUDIT_LOG_DIR, audit_filename)

    # Localiza todos os logs gerados na pasta desta DAG
    dag_log_path = os.path.join(AIRFLOW_LOG_DIR, f"dag_id={dag_id}")
    all_logs = glob(os.path.join(dag_log_path, "**", "*.log"), recursive=True)

    # Filtra logs específicos do run_id/timestamp atual
    ts_nodash = context.get("ts_nodash", "")
    run_logs = sorted(
        [
            f
            for f in all_logs
            if run_id in f or (ts_nodash and ts_nodash in f)
        ]
    )

    with open(target_audit_path, "w", encoding="utf-8") as audit_file:
        audit_file.write("=" * 80 + "\n")
        audit_file.write(f" AUDIT TRAIL LOG - DAG: {dag_id}\n")
        audit_file.write(f" EXECUTION RUN ID: {run_id}\n")
        audit_file.write(
            f" GENERATED AT: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        )
        audit_file.write("=" * 80 + "\n\n")

        if not run_logs:
            audit_file.write(
                "[WARN] Nenhum arquivo de log individual foi encontrado para esta execução.\n"
            )

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
                audit_file.write(
                    f"[ERROR] Falha ao ler log da task {task_id}: {e}\n"
                )

    print(
        f"[AUDIT_MANAGER] Arquivo consolidado salvo em: {target_audit_path}"
    )


# dbt/cosmos
profile_config = ProfileConfig(
    profile_name="datalab",
    target_name="dev",
    profiles_yml_filepath=f"{DBT_PROJECT_DIR}/profiles.yml",
)

default_args = {
    "owner": "datalab",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag_dbt_anp",
    doc_md="""
    ### DAG ANP - Pipeline de Dados
    
    **Camadas:**
    1. **Landing:** Scraping dos dados semanais e mensais da ANP
    2. **Bronze/Silver/Gold:** Transformações via dbt + Cosmos
    3. **Observabilidade:** Report do Elementary e consolidação de logs
    
    **Owner:** datalab  
    **Schedule:** Segundas 06:00  
    **SLA:** 2 horas
    """,
    default_args=default_args,
    description="Orquestração em camadas da ANP com Cosmos e Elementary",
    schedule_interval="0 6 * * 1",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=[
        "anp",
        "bronze",
        "silver",
        "gold",
        "postgres",
        "dbt",
        "cosmos",
        "elementary",
    ],
    # REGISTRO DOS CALLBACKS DE AUDITORIA
    on_success_callback=consolidate_dag_audit_logs,
    on_failure_callback=consolidate_dag_audit_logs,
) as dag:

    # ==========================================
    # 0. SETUP DE INFRAESTRUTURA (DDL)
    # ==========================================
    t_setup_infra = TriggerDagRunOperator(
        task_id="trigger_setup_infra",
        trigger_dag_id="dag_setup_infrastructure",
        wait_for_completion=True,
        poke_interval=10,
    )

    # ==========================================
    # 1. SCRAPING E INGESTÃO (LANDING)
    # ==========================================
    t_scrap_semanal = BashOperator(
        task_id="scrap_semanal",
        bash_command="python3 -u /opt/airflow/pipelines/anp/anp_01_anp_week_file_scrap.py",
        execution_timeout=timedelta(minutes=10),
    )

    t_etl_semanal = BashOperator(
        task_id="landing_semanal",
        bash_command="python3 -u /opt/airflow/pipelines/anp/anp_03_anp_landing_semanal.py",
        execution_timeout=timedelta(minutes=10),
    )

    t_scrap_mensal = BashOperator(
        task_id="scrap_mensal",
        bash_command="python3 -u /opt/airflow/pipelines/anp/anp_02_anp_month_file_scrap.py",
        execution_timeout=timedelta(minutes=15),
    )

    t_etl_mensal = BashOperator(
        task_id="landing_mensal",
        bash_command="python3 -u /opt/airflow/pipelines/anp/anp_04_anp_landing_mensal.py",
        execution_timeout=timedelta(minutes=30),
    )

    # ==========================================
    # 2. TASK GROUP: ASTRONOMER COSMOS (DBT)
    # ==========================================
    dbt_transformations = DbtTaskGroup(
        group_id="dbt_transformations",
        project_config=ProjectConfig(DBT_PROJECT_DIR),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=DBT_EXECUTABLE_PATH
        ),
        render_config=RenderConfig(exclude=["package:elementary"]),
    )

    # ==========================================
    # 3. CONVERGÊNCIA DAS CARGAS LANDING
    # ==========================================
    join_landing = EmptyOperator(
        task_id="join_landing",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ==========================================
    # 4. ELEMENTARY: SETUP + GERAÇÃO DE RELATÓRIO
    # ==========================================
    run_elementary_models = BashOperator(
        task_id="run_elementary_models",
        bash_command=(
            f"{DBT_EXECUTABLE_PATH} run --select elementary "
            f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} "
            f"--profile datalab --target dev"
        ),
        execution_timeout=timedelta(minutes=10),
        trigger_rule=TriggerRule.ALL_DONE,
    )

    generate_elementary_report = BashOperator(
        task_id="generate_elementary_report",
        bash_command=f"DBT_PACKAGES_DIR=/tmp {EDR_EXECUTABLE_PATH} report --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}",
        execution_timeout=timedelta(minutes=10),
    )

    # ==========================================
    # 5. EXPORTAÇÃO DE LOGS DE AUDITORIA
    # ==========================================
    export_dbt_logs = BashOperator(
        task_id="export_dbt_logs",
        bash_command="python3 -u /opt/airflow/pipelines/anp/anp_export_dbt_logs.py",
        execution_timeout=timedelta(minutes=5),
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ==========================================
    # DEFINIÇÃO DAS DEPENDÊNCIAS GERAIS
    # ==========================================
    t_setup_infra >> [t_scrap_semanal, t_scrap_mensal]

    t_scrap_semanal >> t_etl_semanal
    t_scrap_mensal >> t_etl_mensal

    [t_etl_semanal, t_etl_mensal] >> join_landing

    (
        join_landing
        >> dbt_transformations
        >> run_elementary_models
        >> generate_elementary_report
        >> export_dbt_logs
    )