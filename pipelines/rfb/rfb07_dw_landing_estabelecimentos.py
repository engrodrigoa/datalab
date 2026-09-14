# import os
# import sys
# import gc
# import io
# import polars as pl
# import pyarrow as pa
# import pyarrow.parquet as pq
# from botocore.exceptions import ClientError
# from warnings import filterwarnings

# filterwarnings("ignore")

# os.environ["POLARS_MAX_THREADS"] = "2"
# os.environ["RAYON_NUM_THREADS"] = "2"

# # Importando do seu ecossistema Lambda existente
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

# # Modificado para usar os conectores padronizados do DW e S3
# from pipelines.commons.env_loader import validate_env
# from pipelines.commons.s3_client import get_s3_client, test_s3_connection
# from pipelines.commons.dw_client import get_sqla_engine, test_pg_connection
# from pipelines.commons.logger import get_logger
# from sqlalchemy import text

# logger = get_logger("rfb_landing_estabelecimentos")

# REFERENCIA = "2026-08"
# REF_MES_INT = int(REFERENCIA.replace("-", ""))
# BUCKET_SILVER = "silver"
# SCHEMA_LANDING = "landing_rfb"
# TABELA_LANDING = "estabelecimentos"
# PASTA_TMP = "/mnt/datasource/tmp_landing"
# TAMANHO_LOTE = 500_000

# os.makedirs(PASTA_TMP, exist_ok=True)

# DDL_ESTABELECIMENTOS = f"""
# CREATE SCHEMA IF NOT EXISTS {SCHEMA_LANDING};

# CREATE TABLE IF NOT EXISTS {SCHEMA_LANDING}.{TABELA_LANDING} (
#     cnpj_basico TEXT,
#     cnpj_ordem TEXT,
#     cnpj_dv TEXT,
#     cnpj_completo TEXT,
#     identificador_matriz_filial SMALLINT,
#     nome_fantasia TEXT,
#     situacao_cadastral SMALLINT,
#     data_situacao_cadastral DATE,
#     motivo_situacao_cadastral INTEGER,
#     nome_cidade_exterior TEXT,
#     pais INTEGER,
#     data_inicio_atividade DATE,
#     cnae_fiscal_principal TEXT,
#     cnae_fiscal_secundaria TEXT,
#     tipo_logradouro TEXT,
#     logradouro TEXT,
#     numero TEXT,
#     complemento TEXT,
#     bairro TEXT,
#     cep TEXT,
#     uf TEXT,
#     municipio INTEGER,
#     ddd_1 TEXT,
#     telefone_1 TEXT,
#     ddd_2 TEXT,
#     telefone_2 TEXT,
#     ddd_fax TEXT,
#     fax TEXT,
#     correio_eletronico TEXT,
#     situacao_especial TEXT,
#     data_situacao_especial DATE,
#     referencia_mes INTEGER,
#     _source_file TEXT,
#     _inserted_at TIMESTAMPTZ,
#     _suspeita_deslocamento BOOLEAN DEFAULT FALSE
# );
# """

# COLUNAS_PARA_TEXT = [
#     "cnpj_basico", "cnpj_ordem", "cnpj_dv", "cnpj_completo",
#     "cnae_fiscal_principal", "cep", "uf",
#     "ddd_1", "telefone_1", "ddd_2", "telefone_2", "ddd_fax", "fax",
# ]

# def preparar_banco(engine):
#     logger.info(f"validating DDL and cleaning partition {REF_MES_INT} in dw")
#     with engine.begin() as conn:
#         conn.execute(text(DDL_ESTABELECIMENTOS))

#         # A tabela pode já existir de execuções anteriores com os tipos antigos
#         # (VARCHAR(n)) e sem a coluna de flag. Alarga para TEXT (operação segura e
#         # instantânea no Postgres) e garante a coluna de flag, sem perder dados.
#         for coluna in COLUNAS_PARA_TEXT:
#             conn.execute(text(
#                 f"ALTER TABLE {SCHEMA_LANDING}.{TABELA_LANDING} "
#                 f"ALTER COLUMN {coluna} TYPE TEXT"
#             ))
#         conn.execute(text(
#             f"ALTER TABLE {SCHEMA_LANDING}.{TABELA_LANDING} "
#             f"ADD COLUMN IF NOT EXISTS _suspeita_deslocamento BOOLEAN DEFAULT FALSE"
#         ))

#         # Idempotência: Garante que não haverá duplicatas se o script rodar 2 vezes
#         registros_deletados = conn.execute(
#             text(f"DELETE FROM {SCHEMA_LANDING}.{TABELA_LANDING} WHERE referencia_mes = :ref_mes"),
#             {"ref_mes": REF_MES_INT}
#         ).rowcount
#         if registros_deletados > 0:
#             logger.info(f"Removido(s) {registros_deletados} registro(s) antigo(s) da partição {REF_MES_INT}.")

# # Limites de tamanho esperados dos campos "código" da RFB (não são mais usados para
# # DESCARTAR linhas — apenas para MARCAR linhas suspeitas de deslocamento de coluna,
# # preservando 100% dos dados. A correção definitiva é no processo upstream que gera
# # o parquet da Silver; aqui só sinalizamos para permitir auditoria/filtro posterior.
# LIMITES_ESPERADOS = {
#     "cnpj_basico": 8,
#     "cnpj_ordem": 4,
#     "cnpj_dv": 2,
#     "cnpj_completo": 14,
#     "cnae_fiscal_principal": 7,
#     "cep": 8,
#     "uf": 2,
#     "ddd_1": 4,
#     "ddd_2": 4,
#     "ddd_fax": 4,
#     "telefone_1": 15,
#     "telefone_2": 15,
#     "fax": 15,
# }

# def batch_para_large_string(batch: pa.RecordBatch) -> pa.RecordBatch:
#     """
#     Recasta colunas string/binary do RecordBatch para large_string/large_binary
#     (offsets de 64 bits) ANTES de converter para polars. O Arrow Utf8/Binary padrão
#     usa offset i32 (limite de ~2GB de dado de texto acumulado no batch); quando um
#     registro corrompido/deslocado traz um campo anormalmente grande, o total do
#     batch pode estourar esse limite e o polars.from_arrow entra em panic (Rust,
#     não vira exceção Python capturável). Usando large_string, esse teto deixa de
#     existir na prática.
#     """
#     novos_arrays = []
#     novos_campos = []
#     for i, campo in enumerate(batch.schema):
#         arr = batch.column(i)
#         if pa.types.is_string(campo.type):
#             arr = arr.cast(pa.large_string())
#             campo = pa.field(campo.name, pa.large_string(), nullable=campo.nullable)
#         elif pa.types.is_binary(campo.type):
#             arr = arr.cast(pa.large_binary())
#             campo = pa.field(campo.name, pa.large_binary(), nullable=campo.nullable)
#         novos_arrays.append(arr)
#         novos_campos.append(campo)
#     novo_schema = pa.schema(novos_campos)
#     return pa.RecordBatch.from_arrays(novos_arrays, schema=novo_schema)


# def marcar_linhas_suspeitas(df: pl.DataFrame) -> pl.DataFrame:
#     """
#     Adiciona a coluna booleana _suspeita_deslocamento, TRUE quando algum campo
#     "código" excede o tamanho esperado pela RFB (indício de deslocamento de coluna
#     herdado da Silver). NÃO remove nenhuma linha — só sinaliza para tratamento
#     posterior (ex: WHERE _suspeita_deslocamento = false nas camadas seguintes).
#     """
#     condicao_suspeita = pl.lit(False)
#     for coluna, limite in LIMITES_ESPERADOS.items():
#         if coluna in df.columns:
#             condicao_suspeita = condicao_suspeita | (
#                 pl.col(coluna).str.len_chars().fill_null(0) > limite
#             )
#     return df.with_columns(condicao_suspeita.alias("_suspeita_deslocamento"))


# def inserir_dataframe_bulk_copy(df: pl.DataFrame, engine):
#     """
#     Motor de ingestão otimizado: Converte Polars para um buffer de memória TSV (Tab-Separated) 
#     e injeta direto na API COPY do Postgres via SQLAlchemy raw_connection.
#     """
#     buffer = io.BytesIO()
    
#     # Usamos TAB (\t) como separador para evitar colisão com vírgulas nos textos da RFB.
#     # NOTA: quote_style="necessary" (default) é o correto aqui. Já confirmamos via diagnóstico
#     # que a corrupção de colunas vem de dados JÁ deslocados na origem (Silver), não de um
#     # problema de escaping do polars — por isso a marcação (marcar_linhas_suspeitas) é quem
#     # sinaliza isso, não o quoting. Forçar quote_style="always" quebra o reconhecimento de NULL:
#     # o Postgres só trata um campo vazio como NULL quando ele NÃO está entre aspas; com "always"
#     # todo NULL vira '""' (citado) e é lido como valor literal, quebrando colunas numéricas/data.
#     df.write_csv(
#         buffer,
#         separator='\t',
#         null_value='',
#     )
#     buffer.seek(0)
    
#     colunas_str = ", ".join(df.columns)
    
#     copy_sql = f"""
#         COPY {SCHEMA_LANDING}.{TABELA_LANDING} ({colunas_str})
#         FROM STDIN WITH (FORMAT CSV, HEADER TRUE, NULL '', DELIMITER '\t', QUOTE '"', ESCAPE '"')
#     """
    
#     conn = engine.raw_connection()
#     try:
#         with conn.cursor() as cur:
#             cur.copy_expert(copy_sql, buffer)
#         conn.commit()
#     except Exception as e:
#         conn.rollback()
#         logger.error(f"Erro durante o COPY no banco: {e}")
#         # Dump do lote problemático para inspeção manual (ex: encontrar a linha/valor sujo)
#         try:
#             debug_path = os.path.join(PASTA_TMP, "lote_com_erro.tsv")
#             buffer.seek(0)
#             with open(debug_path, "wb") as f:
#                 f.write(buffer.read())
#             logger.error(f"Lote com erro salvo em: {debug_path} (para inspecionar a linha problemática)")
#         except Exception as dump_err:
#             logger.error(f"Falha ao salvar lote de debug: {dump_err}")
#         raise e
#     finally:
#         conn.close()

# def processar_landing_estabelecimentos():
#     # Valida as conexões usando os módulos padronizados do Lab
#     test_s3_connection()
#     test_pg_connection()

#     s3_client = get_s3_client()
#     engine = get_sqla_engine()

#     prefixo_silver = f"rfb/{TABELA_LANDING}/ref_month={REF_MES_INT}/"
    
#     res = s3_client.list_objects_v2(Bucket=BUCKET_SILVER, Prefix=prefixo_silver)
#     arquivos_s3 = [obj["Key"] for obj in res.get("Contents", []) if obj["Key"].endswith(".parquet")]

#     if not arquivos_s3:
#         logger.warning(f"no files found on s3://{BUCKET_SILVER}/{prefixo_silver}")
#         return

#     preparar_banco(engine)

#     total_linhas_geral = 0

#     for chave_s3 in arquivos_s3:
#         nome_arq = os.path.basename(chave_s3)
#         path_local = os.path.join(PASTA_TMP, f"landing_{nome_arq}")
        
#         logger.info(f">>> downloading REV 4{nome_arq} from s3 for local processing")
#         s3_client.download_file(BUCKET_SILVER, chave_s3, path_local)
        
#         parquet_file = pq.ParquetFile(path_local)
#         total_linhas_arquivo = parquet_file.metadata.num_rows
        
#         logger.info(f">>> >>> copy iniciated {nome_arq} para o Postgres (Total: {total_linhas_arquivo} linhas)")
        
#         linhas_processadas = 0
#         total_linhas_suspeitas = 0
#         for batch in parquet_file.iter_batches(batch_size=TAMANHO_LOTE):
#             batch = batch_para_large_string(batch)
#             df_lote = pl.from_arrow(batch)

#             df_lote = marcar_linhas_suspeitas(df_lote)
#             qtd_suspeitas = df_lote.select(pl.col("_suspeita_deslocamento").sum()).item()
#             if qtd_suspeitas > 0:
#                 total_linhas_suspeitas += qtd_suspeitas
#                 logger.warning(
#                     f">>> >>> >>> {qtd_suspeitas} linha(s) suspeita(s) de deslocamento de coluna "
#                     f"neste lote (inseridas mesmo assim, com _suspeita_deslocamento = TRUE)."
#                 )

#             inserir_dataframe_bulk_copy(df_lote, engine)

#             linhas_processadas += df_lote.height
#             logger.info(f">>> >>> >>> inserted batch {linhas_processadas}/{total_linhas_arquivo} rows")
            
#             del df_lote
#             gc.collect()

#         if total_linhas_suspeitas > 0:
#             logger.warning(
#                 f">>> >>> {total_linhas_suspeitas} linha(s) de {nome_arq} marcadas como suspeitas "
#                 f"no total (todas inseridas; filtrar com _suspeita_deslocamento = false se necessário "
#                 f"até a causa raiz ser corrigida na Silver)."
#             )
            
#         total_linhas_geral += linhas_processadas
#         os.remove(path_local)
#         logger.info(f">>> >>> >>> >>> file {nome_arq} done")

#     logger.info("==================================================================")
#     logger.info(f"RUN COMPLETE {total_linhas_geral} total rows inserted {SCHEMA_LANDING}.{TABELA_LANDING}.")
#     logger.info("==================================================================")

# if __name__ == "__main__":
#     processar_landing_estabelecimentos()


import os
import sys
from warnings import filterwarnings

filterwarnings("ignore")

os.environ["POLARS_MAX_THREADS"] = "2"
os.environ["RAYON_NUM_THREADS"] = "2"

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pipelines.commons.s3_client import get_s3_client, test_s3_connection
from pipelines.commons.dw_client import get_sqla_engine, test_pg_connection
from pipelines.commons.logger import get_logger
from pipelines.commons.dw_landing_rfb import processar_arquivos_landing

logger = get_logger("rfb_landing_estabelecimentos")

REFERENCIA = "2026-08"
REF_MES_INT = int(REFERENCIA.replace("-", ""))
BUCKET_SILVER = "silver"
SCHEMA_LANDING = "landing_rfb"
TABELA_LANDING = "estabelecimentos"
ENTIDADE_SILVER = "estabelecimentos"
PASTA_TMP = "/mnt/datasource/tmp_landing"
TAMANHO_LOTE = 500_000

DDL_ESTABELECIMENTOS = f"""
CREATE SCHEMA IF NOT EXISTS {SCHEMA_LANDING};

CREATE TABLE IF NOT EXISTS {SCHEMA_LANDING}.{TABELA_LANDING} (
    cnpj_basico TEXT,
    cnpj_ordem TEXT,
    cnpj_dv TEXT,
    cnpj_completo TEXT,
    identificador_matriz_filial SMALLINT,
    nome_fantasia TEXT,
    situacao_cadastral SMALLINT,
    data_situacao_cadastral DATE,
    motivo_situacao_cadastral INTEGER,
    nome_cidade_exterior TEXT,
    pais INTEGER,
    data_inicio_atividade DATE,
    cnae_fiscal_principal TEXT,
    cnae_fiscal_secundaria TEXT,
    tipo_logradouro TEXT,
    logradouro TEXT,
    numero TEXT,
    complemento TEXT,
    bairro TEXT,
    cep TEXT,
    uf TEXT,
    municipio INTEGER,
    ddd_1 TEXT,
    telefone_1 TEXT,
    ddd_2 TEXT,
    telefone_2 TEXT,
    ddd_fax TEXT,
    fax TEXT,
    correio_eletronico TEXT,
    situacao_especial TEXT,
    data_situacao_especial DATE,
    referencia_mes INTEGER,
    _source_file TEXT,
    _inserted_at TIMESTAMPTZ,
    _suspeita_deslocamento BOOLEAN DEFAULT FALSE
);
"""

# "Code" columns that must be widened to TEXT if the table already exists
# from a previous run with stricter VARCHAR(n) types.
COLUNAS_PARA_TEXT = [
    "cnpj_basico", "cnpj_ordem", "cnpj_dv", "cnpj_completo",
    "cnae_fiscal_principal", "cep", "uf",
    "ddd_1", "telefone_1", "ddd_2", "telefone_2", "ddd_fax", "fax",
]

# Expected RFB field lengths, used only to flag suspicious rows (column-shift
# detection) -- never to drop them.
LIMITES_SUSPEITA = {
    "cnpj_basico": 8,
    "cnpj_ordem": 4,
    "cnpj_dv": 2,
    "cnpj_completo": 14,
    "cnae_fiscal_principal": 7,
    "cep": 8,
    "uf": 2,
    "ddd_1": 4,
    "ddd_2": 4,
    "ddd_fax": 4,
    "telefone_1": 15,
    "telefone_2": 15,
    "fax": 15,
}


def processar_landing_estabelecimentos():
    test_s3_connection()
    test_pg_connection()

    s3_client = get_s3_client()
    engine = get_sqla_engine()

    total_linhas, total_suspeitas = processar_arquivos_landing(
        s3_client=s3_client,
        engine=engine,
        bucket_silver=BUCKET_SILVER,
        entidade=ENTIDADE_SILVER,
        ref_mes_int=REF_MES_INT,
        schema=SCHEMA_LANDING,
        tabela=TABELA_LANDING,
        ddl=DDL_ESTABELECIMENTOS,
        colunas_para_text=COLUNAS_PARA_TEXT,
        limites_suspeita=LIMITES_SUSPEITA,
        pasta_tmp=PASTA_TMP,
        tamanho_lote=TAMANHO_LOTE,
        logger=logger,
    )

    logger.info("==================================================================")
    logger.info(
        f"RUN COMPLETE: {total_linhas} total row(s) inserted into "
        f"{SCHEMA_LANDING}.{TABELA_LANDING} ({total_suspeitas} flagged as suspicious)."
    )
    logger.info("==================================================================")


if __name__ == "__main__":
    processar_landing_estabelecimentos()
