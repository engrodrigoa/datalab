# %%
import os
import glob
import gc
import io
import polars as pl
import logging
import json
import datetime
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert
from warnings import filterwarnings

filterwarnings("ignore")
#-------------------------------------------------------------------------------------------------------------------
# %%
# DB CONNECTION ON LAMBDA3
DB_HOST = "192.168.100.80"
DB_PORT = "5432"
DB_USER = "lambda2"
DB_PASS = "lambda2"
DB_NAME = "lambda3"
TB_ANO_ATUAL = "bronze.anp_ano_atual"
TB_SEMANAL = "bronze.anp_semanal"
SILVER_ANO_CORRENTE = "silver.anp_ano_corrente"

# STRING FORMAT 'postgresql://USER:PASSWORD@HOST:PORT/DATABASE'
CONNSTRING = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# CONECTION ENGINE
engine = create_engine(CONNSTRING)
#-------------------------------------------------------------------------------------------------------------------
print('-' * 50)
print('------- running query on bronze layer, month and week tables. -------')
print('-' * 50)


# sql insert with cte for grab month and week data, deduped by unique key (chave) 
# insert with 'on conflict' clause operations, working as incremental pipeline
sql_upsert = f"""
INSERT INTO {SILVER_ANO_CORRENTE} (
    produto, valor_venda, valor_compra, unidade_de_medida, 
    data_coleta, regiao_sigla, estado_sigla, municipio, 
    revenda, cnpj, nome_da_rua, numero_rua, complemento, 
    bairro, cep, arquivo, id_produto, chave, bandeira
)
WITH uniao AS (
    SELECT 
        produto, 
        valor_venda::DOUBLE PRECISION as valor_venda, 
        valor_compra::DOUBLE PRECISION as valor_compra, 
        unidade_de_medida, 
        data_coleta, 
        regiao_sigla, 
        estado_sigla, 
        municipio, 
        revenda, 
        cnpj, 
        nome_da_rua, 
        numero_rua, 
        complemento, 
        bairro, 
        cep, 
        arquivo, 
        CAST(id_produto as INTEGER) AS id_produto, 
        chave, 
        bandeira
    FROM {TB_ANO_ATUAL} 

    UNION ALL

    SELECT 
        produto, 
        valor_venda::DOUBLE PRECISION as valor_venda, 
        valor_compra::DOUBLE PRECISION as valor_compra, 
        unidade_de_medida, 
        data_coleta, 
        regiao_sigla, 
        estado_sigla, 
        municipio, 
        revenda, 
        cnpj, 
        nome_da_rua, 
        numero_rua, 
        complemento, 
        bairro, 
        cep, 
        arquivo, 
        CAST(id_produto as INTEGER) AS id_produto, 
        chave, 
        bandeira
    FROM {TB_SEMANAL}
)
SELECT DISTINCT ON (chave) * 
FROM uniao
ORDER BY chave, data_coleta DESC

ON CONFLICT (chave) DO NOTHING;
"""

with engine.begin() as conn:
    conn.execute(text(sql_upsert))
    print('-' * 50)
    print('upsert complete on silver')
    print('-' * 50)
#-------------------------------------------------------------------------------------------------------------------

# read silver loaded
silver = pl.read_database_uri(f'''
    SELECT 
        produto, 
        valor_venda::DOUBLE PRECISION as valor_venda, 
        valor_compra::DOUBLE PRECISION as valor_compra, 
        unidade_de_medida, 
        data_coleta, 
        regiao_sigla, 
        estado_sigla, 
        municipio, 
        revenda, 
        cnpj, 
        nome_da_rua, 
        numero_rua, 
        complemento, 
        bairro, 
        cep, 
        arquivo, 
        CAST(id_produto as INTEGER) AS id_produto, 
        chave, 
        bandeira
    FROM silver.anp_ano_corrente''', uri= CONNSTRING) 
#-------------------------------------------------------------------------------------------------------------------
#silver.write_parquet('/home/lambda2/lambda2/airflow/dags/dashboard/data/gold_combustiveis.parquet')
#print('parquet salvo em dashboard/data para consumo do dashboard de controle')


              
#-------------------------------------------------------------------------------------------------------------------
# statistics
resumo = silver.select(
    total_registros=pl.len(),
    produtos_unicos=pl.col("produto").n_unique(),
    lista_produtos= pl.col('produto').unique().sort().str.join(', '),
    estados_atendidos=pl.col("estado_sigla").n_unique(),
    preco_venda_medio=pl.col("valor_venda").filter(pl.col('id_produto') != 8).mean().round(2),
    preco_venda_min=pl.col("valor_venda").min(),
    preco_venda_max=pl.col("valor_venda").filter(pl.col('id_produto') != 8).max(),
    data_inicio=pl.col("data_coleta").min(),
    data_fim=pl.col("data_coleta").max(),
).to_dicts()[0]

#  statistics print out
print("=" * 50)
print(" ----- RESUMO ESTATÍSTICO DA CARGA ----- ")
print("=" * 50)
print(f"Total de Registros : {resumo['total_registros']:,}".replace(",", "."))
print(f"Período Coberto    : {resumo['data_inicio']} a {resumo['data_fim']}")
print(f"Produtos Distintos : {resumo['produtos_unicos']}")
print(f"Produtos descritos : {resumo['lista_produtos']}")
print(f"Estados Presentes  : {resumo['estados_atendidos']}")
print(f"Média Preço Venda  : R$ {resumo['preco_venda_medio']}")
print(f"Amplitude de Preço : R$ {resumo['preco_venda_min']} - R$ {resumo['preco_venda_max']}")
print("=" * 50)

#-------------------------------------------------------------------------------------------------------------------
#AIRFLOW XCOM
resumo_dados = {
    "status": "sucesso",
    "registros_processados": len(silver),
    "data_processamento": datetime.now().isoformat()
}

with open("/tmp/resumo_silver.json", "w", encoding="utf-8") as f:
    json.dump(resumo, f, default=str)


