import sys
import os
import re
from datetime import datetime
from warnings import filterwarnings

import requests
from bs4 import BeautifulSoup
import polars as pl
from sqlalchemy import create_engine
from dotenv import load_dotenv


load_dotenv()
filterwarnings("ignore")

#####################################
# SETUP INIT
#####################################
URL_ANP = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis"
HEADERS_WEB = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

LINKS_DOWNLOAD = {
    "ultimas-4-semanas-diesel-gnv.csv": "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/qus/ultimas-4-semanas-diesel-gnv.csv",
    "ultimas-4-semanas-gasolina-etanol.csv": "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/qus/ultimas-4-semanas-gasolina-etanol.csv",
    "ultimas-4-semanas-glp.csv": "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/qus/ultimas-4-semanas-glp.csv"
}

# FILES LANDING
DIR_LANDING = "/mnt/datasource/anp/ult4"

# FAIL FAST CREDENCIALS
REQUIRED_PG_VARS = ["DB_HOST", "DB_PORT", "DB_USER", "DB_PASS", "DB_NAME"]
missing_vars = [var for var in REQUIRED_PG_VARS if not os.getenv(var)]
if missing_vars:
    print(f"[FAIL] no credencials vars on .env {', '.join(missing_vars)}")
    sys.exit(1)

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")
SCHEMA = "ctrl"
TABELA = "anp_metadata_semanal"

CONSTRING = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

#####################################
# PROCESSING FUNCTIONS
#####################################
def ler_ultima_data_banco(string_conexao, schema, tabela):
    try:
        df_metadata = pl.read_database_uri(f"SELECT * FROM {schema}.{tabela}", uri=string_conexao)
        if not df_metadata.is_empty() and "data_ref" in df_metadata.columns:
            return df_metadata.select(pl.col('data_ref').max()).item()
        return None
    except Exception:
        return None

def obter_data_atualizacao_site(url, headers):
    response = requests.get(url, headers=headers, timeout=15)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')
    
    padrao = re.search(
        r"Quatro\s+últimas\s+semanas.*?atualizado\s+em\s*(\d{1,2}/\d{1,2}/\d{4})", 
        soup.get_text(), 
        re.IGNORECASE | re.DOTALL
    )
    if padrao:
        return datetime.strptime(padrao.group(1), "%d/%m/%Y").date()
    raise ValueError("data não encontrada.")

def download_csv_local(links, diretorio_destino):
    os.makedirs(diretorio_destino, exist_ok=True)
    
    for nome_arquivo, link_url in links.items():
        caminho_local = os.path.join(diretorio_destino, nome_arquivo)
        print(f" -> Baixando {nome_arquivo} para {caminho_local}...")
        
        res_file = requests.get(link_url, headers=HEADERS_WEB, stream=True, timeout=30)
        res_file.raise_for_status()
        
        with open(caminho_local, 'wb') as out_file:
            for chunk in res_file.iter_content(chunk_size=8192):
                out_file.write(chunk)

def registrar_metadado_banco(data_site, status, schema, tabela, string_conexao):
    engine = create_engine(string_conexao)
    df_resultado = pl.DataFrame([{
        'data_ref': data_site,
        'data_dag_run': datetime.now(),
        'status': status
    }])
    df_resultado.write_database(
        table_name=f"{schema}.{tabela}",
        connection=string_conexao,
        if_table_exists="append",
        engine="sqlalchemy"
    )
    engine.dispose()

#####################################
# DATA FLOW
#####################################
def main():
    print(f">>> --- Iniciando Extrator ANP (Web -> Local): {datetime.now()} ---")
    
    try:
        data_site = obter_data_atualizacao_site(URL_ANP, HEADERS_WEB)
        print(f" >>>[GET] date recovered from anp {data_site}")
    except Exception as e:
        print(f">>>[FAIL] scrapping fail: {e}")
        sys.exit(1)
        
    ultima_data_banco = ler_ultima_data_banco(CONSTRING, SCHEMA, TABELA)
    print(f" >>> lasdate run {ultima_data_banco}")

    # Condição de idempotência
    if ultima_data_banco is not None and data_site <= ultima_data_banco:
        print(">>> [SKIP] already up-to-date.")
        sys.exit(99) 
        
    try:
        print(f"\n>>> [STEP 1/2] downloading files on {DIR_LANDING}...")
        download_csv_local(LINKS_DOWNLOAD, DIR_LANDING)
        
        print("\n>>>[STEP 2/2] writing successs")
        registrar_metadado_banco(data_site, 'SUCESSO', SCHEMA, TABELA, CONSTRING)
        
        print("\n>>> --- db updated ---")
        sys.exit(0)
        
    except Exception as e:
        print(f"\n[FAIL] PIPELINE FAIL: {e}")
        try:
            registrar_metadado_banco(data_site, 'FALHOU', SCHEMA, TABELA, CONSTRING)
            print(">>> insert fail status")
        except Exception as db_e:
            print(f"[FAIL] LOG INSERT FAIL: {db_e}")
            
        sys.exit(1)

if __name__ == "__main__":
    main()