# %%
import datetime as dt
from datetime import datetime
import os
import re
import sys  
from warnings import filterwarnings

from bs4 import BeautifulSoup
import polars as pl
import requests
from sqlalchemy import create_engine, text

filterwarnings("ignore")

# %%
#####################################
# Parâmetros
#####################################
def main():
    url = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }

    links_anp = {
        "ultimas-4-semanas-diesel-gnv.csv": "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/qus/ultimas-4-semanas-diesel-gnv.csv",
        "ultimas-4-semanas-gasolina-etanol.csv": "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/qus/ultimas-4-semanas-gasolina-etanol.csv",
        "ultimas-4-semanas-glp.csv": "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/qus/ultimas-4-semanas-glp.csv"
    }

    # download dir
    diretorio_destino = "/mnt/datasource/anp/ult4"
    os.makedirs(diretorio_destino, exist_ok=True)

    # %%
    # DB con
    DB_HOST = "192.168.100.80"
    DB_PORT = "5432"
    DB_USER = "lambda2"
    DB_PASS = "lambda2"
    DB_NAME = "lambda3"
    SCHEMA = "bronze"
    TABELA = "anp_metadata" 
    TARGET = f"{SCHEMA}.{TABELA}"

    CONSTRING = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(CONSTRING)

    # %%
    # Consulta metadata existente
    try:
        df_metadata = pl.read_database_uri(
            f"SELECT * FROM {SCHEMA}.{TABELA}", uri=CONSTRING
        )
    except Exception:
        df_metadata = pl.DataFrame(
            schema={"data_ref": pl.Date, "data_dag_run": pl.Datetime, "status": pl.Utf8}
        )

    # %%
    ######################################
    # 1. Requisição e Scraping da Data de Atualização
    ######################################
    response = requests.get(url, headers=headers, timeout=15)
    print(f"Status Code: {response.status_code}")

    soup = BeautifulSoup(response.text, 'html.parser')
    texto_da_pagina = soup.get_text()
    padrao = re.search(r"Quatro\s+últimas\s+semanas.*?atualizado\s+em\s*(\d{1,2}/\d{1,2}/\d{4})", texto_da_pagina, re.IGNORECASE | re.DOTALL)

    if padrao:
        data_str = padrao.group(1)
        data_dt = datetime.strptime(data_str, "%d/%m/%Y").date()
        print(f"Data de referência no site: {data_dt}")
    else:
        print("[ERRO] Falha ao capturar a data de referência no portal da ANP.")
        sys.exit(1)  # <--- FALA NO AIRFLOW DEVIDO À MUDANÇA NA ESTRUTURA DO SITE

    # %%
    # 2. Comparação de datas
    df_controle = pl.DataFrame([data_dt], schema=['data_atualizacao_site'])
    df_controle = df_controle.with_columns(pl.lit(datetime.now()).alias('data_atualizacao_dag'))

    data_site = df_controle.select('data_atualizacao_site').item()

    if not df_metadata.is_empty() and "data_ref" in df_metadata.columns:
        ultima_data_processada = df_metadata.select(pl.col('data_ref').max()).item()
    else:
        ultima_data_processada = None

    # %%
    # 3. Execução condicional
    if ultima_data_processada is None or data_site > ultima_data_processada:
        print("Iniciando download de novos arquivos semanais...")
        
        try:
            headers_download = {"User-Agent": "Mozilla/5.0"}
            for nome_arquivo, link_url in links_anp.items():
                caminho_final = os.path.join(diretorio_destino, nome_arquivo)
                print(f" -> downloading {nome_arquivo}...")
                
                res_file = requests.get(link_url, headers=headers_download, timeout=30)
                res_file.raise_for_status()
                
                with open(caminho_final, 'wb') as f:
                    f.write(res_file.content)
                    
            print(f"\nArquivos descarregados com sucesso no diretório: {diretorio_destino}")
            
            # Cria registro de sucesso no Polars
            df_resultado_final = pl.DataFrame([{
                'data_ref': data_site,
                'data_dag_run': datetime.now(),
                'status': 'SUCESSO'
            }])
            
            # Registra metadados no banco
            df_resultado_final.write_database(
                table_name=f"{SCHEMA}.{TABELA}",
                connection=engine,
                if_table_exists="append",
                engine="sqlalchemy"
            )
            print(f" ---> Metadados atualizados em {TARGET}.")
            
            # Encerra com SUCESSO (Exit code 0) para prosseguir para o ETL Semanal
            sys.exit(0)

        except Exception as e:
            print(f"\n[ERRO] Falha no download dos arquivos: {e}")
            
            df_resultado_final = pl.DataFrame([{
                'data_ref': data_site,
                'data_dag_run': datetime.now(),
                'status': 'FALHOU'
            }])
            
            try:
                df_resultado_final.write_database(
                    table_name=f"{SCHEMA}.{TABELA}",
                    connection=engine,
                    if_table_exists="append",
                    engine="sqlalchemy"
                )
            except Exception as db_err:
                print(f"Erro ao registrar falha no banco: {db_err}")
                
            # SINALIZA ERRO NO AIRFLOW
            sys.exit(1)

    else:
        print(f"Nenhuma nova atualização no portal. Data do site ({data_site}) <= ÚLTIMA PROCESSADA ({ultima_data_processada}).")
        print("[SKIP] Encerrando execução com exit code 99...")
        
        # Encerra com EXIT CODE 99 para o Airflow ignorar (SKIP) a task semanal e ir para o 'end'
        sys.exit(99)
        pass 
if __name__ == "__main__":
    main()