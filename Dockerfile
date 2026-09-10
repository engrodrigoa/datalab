FROM apache/airflow:2.9.0
####
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

#cosmos
RUN pip install --no-cache-dir \
    polars \
    connectorx \
    pyarrow \
    astronomer-cosmos

# venv

RUN python -m venv /opt/airflow/dbt_venv && \
    /opt/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-core dbt-postgres 'elementary-data[postgres]' && \
    chmod -R 775 /opt/airflow/dbt_venv

RUN python3 -m venv /opt/airflow/dbt_venv && \
    /opt/airflow/dbt_venv/bin/pip install --no-cache-dir -r requirements-dbt.txt