from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Importações do Astronomer Cosmos
from cosmos import (
    DbtTaskGroup,
    ProjectConfig,
    ProfileConfig,
    ExecutionConfig,
    RenderConfig
)

# Caminhos do ambiente
DBT_PROJECT_DIR = "/opt/airflow/pipelines/dbt_projects"
DBT_EXECUTABLE_PATH = "/opt/airflow/dbt_venv/bin/dbt"
EDR_EXECUTABLE_PATH = "/opt/airflow/dbt_venv/bin/edr" 

# dbt/cosmos 
profile_config = ProfileConfig(
    profile_name="datalab", 
    target_name="dev",             
    profiles_yml_filepath=f"{DBT_PROJECT_DIR}/profiles.yml"
)

default_args = {
    'owner': 'datalab',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dag_dbt_anp',
    doc_md="""
    ### DAG ANP - Pipeline de Dados
    
    **Camadas:**
    1. **Landing:** Scraping dos dados semanais e mensais da ANP
    2. **Bronze/Silver/Gold:** Transformações via dbt + Cosmos
    3. **Observabilidade:** Report do Elementary
    
    **Owner:** datalab  
    **Schedule:** Segundas 06:00  
    **SLA:** 2 horas
    """,
    default_args=default_args,
    description='Orquestração em camadas da ANP com Cosmos e Elementary',
    schedule_interval='0 6 * * 1',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['anp', 'bronze', 'silver', 'gold', 'postgres', 'dbt', 'cosmos', 'elementary'],
) as dag:

    # ==========================================
    # 0. SETUP DE INFRAESTRUTURA (DDL)
    # ==========================================
    t_setup_infra = TriggerDagRunOperator(
        task_id='trigger_setup_infra',
        trigger_dag_id='dag_setup_infrastructure',
        wait_for_completion=True,
        poke_interval=10,        
    )

    # ==========================================
    # 1. SCRAPING E INGESTÃO (LANDING)
    # ==========================================
    t_scrap_semanal = BashOperator(
        task_id='scrap_semanal',
        bash_command='python3 -u /opt/airflow/pipelines/anp/anp_01_anp_week_file_scrap.py',
        execution_timeout=timedelta(minutes=10),
    )

    t_etl_semanal = BashOperator(
        task_id='landing_semanal',
        bash_command='python3 -u /opt/airflow/pipelines/anp/anp_03_anp_landing_semanal.py',
        execution_timeout=timedelta(minutes=10),
    )

    t_scrap_mensal = BashOperator(
        task_id='scrap_mensal',
        bash_command='python3 -u /opt/airflow/pipelines/anp/anp_02_anp_month_file_scrap.py',
        execution_timeout=timedelta(minutes=15),
    )

    t_etl_mensal = BashOperator(
        task_id='landing_mensal',
        bash_command='python3 -u /opt/airflow/pipelines/anp/anp_04_anp_landing_mensal.py',
        execution_timeout=timedelta(minutes=30),
    )

    # ==========================================
    # 2. TASK GROUP: ASTRONOMER COSMOS (DBT)
    # ==========================================
    # O Cosmos vai varrer o projeto automaticamente. 
    # Você não precisa mais separar por pastas manualmente, ele lê o grafo de dependências 
    # (refs) e orquestra Bronze -> Silver -> Gold modelo por modelo.
    dbt_transformations = DbtTaskGroup(
        group_id="dbt_transformations",
        project_config=ProjectConfig(DBT_PROJECT_DIR),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=DBT_EXECUTABLE_PATH
        ),
        # Adicione o RenderConfig para excluir o pacote do elementary do Airflow
        render_config=RenderConfig(
            exclude=["package:elementary"] 
        ),
    )

    # ==========================================
    # 3. CONVERGÊNCIA DAS CARGAS LANDING
    # ==========================================
    join_landing = EmptyOperator(
        task_id='join_landing',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    # ==========================================
    # 4. ELEMENTARY: GERAÇÃO DE RELATÓRIO
    # ==========================================
    # Gera o dashboard HTML do Elementary para visualização do workflow e testes do dbt
    generate_elementary_report = BashOperator(
        task_id='generate_elementary_report',
        bash_command=f'DBT_PACKAGES_DIR=/tmp {EDR_EXECUTABLE_PATH} report --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}',
        execution_timeout=timedelta(minutes=10),
    )

    # ==========================================
    # 5. EXPORTAÇÃO DE LOGS DE AUDITORIA (NOVO)
    # ==========================================
    # Extrai os metadados do dbt (run_results e manifest), faz o backup no MinIO
    # e grava a tabela de observabilidade (audit.dbt_runs) no PostgreSQL.
    export_dbt_logs = BashOperator(
        task_id='export_dbt_logs',
        bash_command='python3 -u /opt/airflow/pipelines/anp/anp_export_dbt_logs.py',
        execution_timeout=timedelta(minutes=5),
        # IMPORTANTE: Garante que os logs sejam salvos mesmo se algum modelo do dbt falhar
        trigger_rule=TriggerRule.ALL_DONE
    )

    # ==========================================
    # DEFINIÇÃO DAS DEPENDÊNCIAS GERAIS
    # ==========================================
    t_setup_infra >> [t_scrap_semanal, t_scrap_mensal]
    
    t_scrap_semanal >> t_etl_semanal
    t_scrap_mensal >> t_etl_mensal

    # O fluxo consolida as saídas e tolera o "skip" da rotina mensal ou semanal
    [t_etl_semanal, t_etl_mensal] >> join_landing
    
    # O dbt roda em seguida, depois o Elementary gera o report, 
    # e por último, o log de execução é salvo.
    join_landing >> dbt_transformations >> generate_elementary_report >> export_dbt_logs