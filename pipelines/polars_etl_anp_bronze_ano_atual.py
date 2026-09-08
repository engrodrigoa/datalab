# %%
import os
from datetime import datetime
import glob
import gc
import polars as pl
import io
import traceback
from sqlalchemy import create_engine, text
from warnings import filterwarnings

filterwarnings("ignore")

# %%
DB_HOST = "192.168.100.80"
DB_PORT = "5432"
DB_USER = "lambda2"
DB_PASS = "lambda2"
DB_NAME = "lambda3"
BRONZE = "bronze"
TB_ANO_ATUAL = "anp_ano_atual"
CURADORIA = "bronze"
COMBUSTIVEL = "combustivel"
LANDING = "bronze.combustivel"
TARGET = "bronze.anp_ano_atual"
PDATA = f"{datetime.now().year}-01-01"

# 2. Crie a URL de Conexão
# Formato: 'postgresql://USER:PASSWORD@HOST:PORT/DATABASE'
CONNSTRING = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# 3. Crie o motor (engine) de conexão
engine = create_engine(CONNSTRING)

print(f'=' *60)
print(f'Iniciando upsert por ON CONFLICT (chave)')
print(f'=' *60)
sql_upsert = f"""
INSERT INTO {TARGET} (
    produto, valor_venda, valor_compra, unidade_de_medida, 
    data_coleta, regiao_sigla, estado_sigla, municipio, 
    revenda, cnpj, nome_da_rua, numero_rua, complemento, 
    bairro, cep, arquivo, id_produto, chave, bandeira
)
SELECT 
    produto 
, valor_venda::DOUBLE PRECISION as valor_venda
, valor_compra::DOUBLE PRECISION as valor_compra	
, unidade_de_medida	
, data_coleta	
, regiao_sigla	
, estado_sigla	
, municipio	
, revenda	
, cnpj	
, nome_da_rua	
, numero_rua	
, complemento	
, bairro	
, cep	
, arquivo	
, CAST(id_produto as INTEGER) AS id_produto	
, chave 
, bandeira
FROM {LANDING}
WHERE data_coleta >= '{PDATA}'
ON CONFLICT (chave) DO NOTHING
"""

with engine.begin() as conn:
    conn.execute(text(sql_upsert))

print("---> Upsert executado com sucesso")

