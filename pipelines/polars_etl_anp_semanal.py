# %%
import polars as pl
import datetime as dt 
import os
import glob
import gc
import io
import traceback
import sys

import psycopg2
from sqlalchemy import text

from sqlalchemy import create_engine, text
from warnings import filterwarnings


filterwarnings("ignore")

# %%
#######
# db con
DB_HOST = "192.168.100.80"
DB_PORT = "5432"
DB_USER = "lambda2"
DB_PASS = "lambda2"
DB_NAME = "lambda3"
SCHEMA = "bronze"
TABELA = "anp_semanal" 

# Formato: 'postgresql://USER:PASSWORD@HOST:PORT/DATABASE'
DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DB_URL)
###############

#####
# 

# %%
###########################################################
dir_input = '/mnt/datasource/anp/ult4'
diretorio_recursivo = '/mnt/datasource/anp/ult4/**/*.csv'
todos_arquivos = glob.glob(diretorio_recursivo, recursive=True)

# %%
arquivos_pendentes = []
for f in todos_arquivos:
    nome_arquivo = os.path.basename(f)
    caminho_formatado = f.replace('\\', '/')
    
    if not nome_arquivo.startswith("OK_") and ".ipynb_checkpoints" not in caminho_formatado:
        arquivos_pendentes.append(f)

# 
arquivos_pendentes = sorted(arquivos_pendentes)

print(f"=== {len(arquivos_pendentes)} arquivos para processamento ===")

# %%
###########################################################

#CHECAGEM DE ARQUIVOS EXISTENTES, VÁLIDOS E JÁ INSERIDOS
if len(arquivos_pendentes) == 0:
    print("nenhum arquivo pendente para processar")
    print("O site da ANP não trouxe atualizações ou os dados já foram processados.")
    print("pipeline abortado")
    
    sys.exit(0) 

else:
    if os.path.exists(dir_input):
        arquivos_na_pasta = os.listdir(dir_input)
        contador_removidos = 0
    
    for arquivo in arquivos_na_pasta:
        if arquivo.startswith("OK_"):
            caminho_completo = os.path.join(dir_input, arquivo)
            os.remove(caminho_completo)
            print(f" -> arquivo: {arquivo}")
            contador_removidos += 1
            
    if contador_removidos == 0:
        print(" -> arquivos válidos")
    else:
        print(f" ->  {contador_removidos} arquivos removidos")

    print("iniciando truncate")
    engine = create_engine(DB_URL)
    with engine.connect() as conexao:
        conexao.execute(text("TRUNCATE TABLE bronze.anp_semanal"))
    print("truncate bronze.anp_semanal executado")
    print("percorrendo pipeline...")

# %%
def tratamento_df(df_input: pl.DataFrame, nome_arquivo: str) -> pl.DataFrame:
    mapeamento_produtos = {
        "GASOLINA": "1",
        "ETANOL": "2",
        "DIESEL": "3",
        "DIESEL S10": "4",
        "GNV": "5",
        "GASOLINA ADITIVADA": "6",
        "DIESEL S50": "7",
        "GLP": "8",
    }

    novas_colunas = (df_input.columns)
    novas_colunas = [
        c.lower()
         .replace(" - ", "_")
         .replace(" ", "_")
        for c in novas_colunas
    ]
        
    df_local = (
            df_input
            .rename(dict(zip(df_input.columns, novas_colunas)))
            .rename({
            "regiao_sigla": "regiao_sigla",
            "estado_sigla": "estado_sigla"
        })
        .with_columns(
        pl.col('data_da_coleta').str.to_date('%d/%m/%Y').alias('data_coleta')

        , pl.col('valor_de_venda')
            .str.replace(',', '.')
            .cast(pl.Float64)
            .alias('valor_venda')
    
        , pl.col('valor_de_compra')
            .str.replace(',', '.')
            .cast(pl.Float64)
            .alias('valor_compra')

        , pl.col('produto')
            .replace_strict(mapeamento_produtos, default=None)
            .alias('id_produto')

        # CEP
        , pl.col("cep")
            .str.replace_all(r"[-./ ]", "")
            .alias('cep')
        
                    # CNPJ limpo
        , pl.col("cnpj_da_revenda")
            .str.replace_all(r"[-./ ]", "")
            .alias("cnpj")

        # Nome do arquivo
        , pl.lit(nome_arquivo).alias("arquivo")

            )
    .with_columns(
        (
        pl.col('data_coleta').dt.strftime('%Y%m%d')
        + pl.col('id_produto')
        + pl.col('cnpj')
        ).alias('chave')
    
    )
    .select(
            "produto",
            "id_produto",
            "valor_venda",
            "valor_compra",
            "unidade_de_medida",
            "data_coleta",
            "regiao_sigla",
            "estado_sigla",
            "municipio",
            "revenda",
            "cnpj",
            "nome_da_rua",
            "numero_rua",
            "complemento",
            "bairro",
            "cep",
            "arquivo",
            "chave",
            "bandeira",
        )
    )
    
    return df_local

# %%
def copy_to_postgres(df: pl.DataFrame, engine, schema: str, tabela: str):

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
    conn = engine.raw_connection()

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

# %%
for caminho_completo in arquivos_pendentes:

    nome_base = os.path.basename(caminho_completo)
    nome_arquivo = os.path.splitext(nome_base)[0]

    print(f"\nIniciando processamento do arquivo: {nome_base}")

    try:

        # ==================================================
        # Leitura
        # ==================================================

        df_bruto = (
            pl.scan_csv(
                caminho_completo,
                separator=";",
                encoding="utf8"
            )
            .with_columns(
                pl.lit(nome_arquivo).alias("arquivo")
            )
        )

        #print(f"   -> Arquivo carregado ({df_bruto.height:,} linhas brutas).")

        # ==================================================
        # Tratamento
        # ==================================================

        #df_final = tratamento_df(df_bruto, nome_arquivo)
        df_final= (tratamento_df(df_bruto, nome_arquivo). collect(engine='streaming'))

        #print(f"   -> Tratamento concluído ({df_final.height:,} linhas).")

        # ==================================================
        # Idempotência
        # ==================================================

        with engine.begin() as conn:

            tabela_existe = conn.execute(
                text("""
                    SELECT EXISTS (
                        SELECT
                        FROM information_schema.tables
                        WHERE table_schema = :schema
                          AND table_name = :tabela
                    )
                """),
                {
                    "schema": SCHEMA,
                    "tabela": TABELA
                }
            ).scalar()

            if tabela_existe:

                registros_deletados = conn.execute(
                    text(f"""
                        DELETE
                        FROM {SCHEMA}.{TABELA}
                        WHERE arquivo = :arquivo
                    """),
                    {
                        "arquivo": nome_arquivo
                    }
                ).rowcount

                if registros_deletados:
                    print(f"   -> {registros_deletados:,} registros antigos removidos.")

        # ==================================================
        # Carga
        # ==================================================

        copy_to_postgres(
            df=df_final,
            engine=engine,
            schema=SCHEMA,
            tabela=TABELA
        )

        print("   -> Dados gravados no PostgreSQL.")

        # ==================================================
        # Renomeia arquivo
        # ==================================================

        novo_nome = os.path.join(
            os.path.dirname(caminho_completo),
            f"OK_{nome_base}"
        )
        os.rename(
            caminho_completo,
            novo_nome
        )
        print(f"   -> Arquivo renomeado para OK_{nome_base}")

    except Exception:
        print(f"\n[ERRO] Arquivo: {nome_base}\n")
        traceback.print_exc()

    finally:
        gc.collect()
        print("   -> Memória liberada.\n")

print("=" * 60)
print("Pipeline executado com sucesso.")


