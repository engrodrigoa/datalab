import os
import sys
import gc
import io
import traceback
import glob
from datetime import datetime

import polars as pl
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from warnings import filterwarnings

filterwarnings("ignore")
load_dotenv()

# ==========================================
# 1. TRAVA DE SEGURANÇA E CONFIGURAÇÕES POSTGRESQL (DW)
# ==========================================
REQUIRED_PG_VARS = ["DB_HOST", "DB_PORT", "DB_USER", "DB_PASS", "DB_NAME"]

missing_vars = [var for var in REQUIRED_PG_VARS if not os.getenv(var)]
if missing_vars:
    print(f"[ERRO FATAL] Variáveis de banco ausentes no .env: {', '.join(missing_vars)}")
    sys.exit(1)

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DB_URL)

SCHEMA = "bronze"
TABELA = "anp_landing_mensal"

# Atualizado com o seu padrão de busca (Wildcard)
DIR_INPUT = '/mnt/datasource/anp/arquivos_fechados/**/*.csv'

# ==========================================
# 2. FUNÇÃO DE INGESTÃO MASSIVA (COPY)
# ==========================================
def copy_to_postgres(df: pl.DataFrame, engine_db, schema: str, tabela: str):
    """Realiza o bulk insert utilizando o comando COPY do PostgreSQL."""
    buffer = io.StringIO()
    df.write_csv(buffer)
    buffer.seek(0)
    colunas = ", ".join(df.columns)

    sql = f"""
        COPY {schema}.{tabela}
        ({colunas})
        FROM STDIN
        WITH (
            FORMAT CSV,
            HEADER TRUE
        )
    """
    conn = engine_db.raw_connection()
    try:
        cur = conn.cursor()
        cur.copy_expert(sql, buffer)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

# ==========================================
# 3. EXECUÇÃO DO PIPELINE
# ==========================================
def run_pipeline():
    print(f"=== Iniciando Ingestão Landing Mensal (Raw): {datetime.now()} ===")
    
    # 3.1 Busca recursiva utilizando diretamente a string com o wildcard
    todos_arquivos = glob.glob(DIR_INPUT, recursive=True)
    
    # Filtra arquivos que já foram processados (prefixo OK_)
    arquivos_pendentes = sorted([
        f for f in todos_arquivos 
        if not os.path.basename(f).startswith("OK_")
    ])

    if not arquivos_pendentes:
        print("[OK] Nenhum arquivo pendente para processar na origem mensal.")
        sys.exit(0)

    print(f" -> {len(arquivos_pendentes)} arquivo(s) encontrado(s). Iniciando processamento...")
    timestamp_ingestao = datetime.now()

    # 3.2 Loop de processamento arquivo a arquivo
    for caminho_completo in arquivos_pendentes:
        nome_base = os.path.basename(caminho_completo)
        print(f"\n- Lendo: {nome_base}")

        try:
            # Leitura Raw: tudo como string, ignorando erros de parse
            df_bruto = pl.read_csv(
                caminho_completo,
                separator=";",
                encoding="utf8",
                infer_schema_length=0,
                ignore_errors=True
            )
            
            # Padronização de Colunas (snake_case)
            novas_colunas = [c.lower().replace(" - ", "_").replace(" ", "_") for c in df_bruto.columns]
            df_padronizado = df_bruto.rename(dict(zip(df_bruto.columns, novas_colunas)))
            
            # Adição de Metadados de Linhagem
            df_padronizado = df_padronizado.with_columns(
                pl.lit(nome_base).alias("arquivo"),
                pl.lit(timestamp_ingestao).alias("ingestion_timestamp")
            )

            # Idempotência: Limpeza de processamentos anteriores do mesmo arquivo
            with engine.begin() as conn:
                registros_deletados = conn.execute(
                    text(f"DELETE FROM {SCHEMA}.{TABELA} WHERE arquivo = :arquivo"),
                    {"arquivo": nome_base}
                ).rowcount
                if registros_deletados > 0:
                    print(f"  -> {registros_deletados} registros antigos removidos por idempotência.")

            # Bulk Insert via COPY
            copy_to_postgres(df_padronizado, engine, SCHEMA, TABELA)
            print("  -> Dados gravados no PostgreSQL com sucesso.")

            # Renomeia o arquivo no seu diretório original para marcar como OK
            novo_nome = os.path.join(os.path.dirname(caminho_completo), f"OK_{nome_base}")
            os.rename(caminho_completo, novo_nome)
            print(f"  -> Arquivo renomeado para OK_{nome_base}")

        except Exception as e:
            print(f"  [ERRO] Falha ao processar {nome_base}: {e}")
            traceback.print_exc()
        finally:
            gc.collect()

    print("\n============================================================")
    print("Pipeline da Camada Landing Mensal executado com sucesso.")

if __name__ == "__main__":
    run_pipeline()