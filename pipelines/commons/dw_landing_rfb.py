"""
pipelines/commons/dw_landing_rfb.py

Shared module for the Silver -> DW landing pipelines of every RFB entity
(Estabelecimentos, Empresas, Socios, Simples, reference/domain tables).

Centralizes the logic that was hardened while building the Estabelecimentos
pipeline, so new entity scripts don't have to rediscover the same bugs:

  1. batch_para_large_string: avoids the Arrow/polars panic
     ("max string/binary length exceeded") that occurs when a batch
     accumulates more than ~2GB of text under the default i32-offset Utf8 type.
  2. marcar_linhas_suspeitas: never drops rows; flags _suspeita_deslocamento=True
     when a "code" field exceeds its expected RFB length (a symptom of a
     column-shift bug inherited from Bronze-layer parsing), so no data is lost
     while still being auditable/filterable downstream.
  3. inserir_dataframe_bulk_copy: serializes the DataFrame to TSV and uses
     COPY FROM STDIN with correct quoting/NULL handling. IMPORTANT:
     quote_style="always" must NOT be used — Postgres only recognizes an
     empty field as NULL when it is NOT quoted; forcing quotes on every field
     turns every NULL into a literal '""' value and breaks numeric/date columns.
  4. preparar_schema_tabela: creates schema/table, widens "code" columns to
     TEXT (idempotent, needed if the table already exists with stricter
     types), ensures the suspicious-row flag column exists, and clears the
     target partition (referencia_mes) for idempotent reprocessing.
  5. processar_arquivos_landing: orchestrates the full per-entity flow --
     lists the Silver parquet files, downloads them, iterates in batches
     applying the steps above, and performs the COPY.
"""

import os
import gc
import io
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import text


def batch_para_large_string(batch: pa.RecordBatch) -> pa.RecordBatch:
    """
    Recasts string/binary columns of the RecordBatch to large_string/large_binary
    (64-bit offsets) before converting to polars. The default Arrow Utf8/Binary
    type uses i32 offsets (~2GB limit of accumulated text per batch); a single
    corrupted/oversized field can push a batch over that limit and cause
    polars.from_arrow to panic (a Rust panic, not a catchable Python exception).
    Using large_string removes this ceiling in practice.
    """
    novos_arrays = []
    novos_campos = []
    for i, campo in enumerate(batch.schema):
        arr = batch.column(i)
        if pa.types.is_string(campo.type):
            arr = arr.cast(pa.large_string())
            campo = pa.field(campo.name, pa.large_string(), nullable=campo.nullable)
        elif pa.types.is_binary(campo.type):
            arr = arr.cast(pa.large_binary())
            campo = pa.field(campo.name, pa.large_binary(), nullable=campo.nullable)
        novos_arrays.append(arr)
        novos_campos.append(campo)
    novo_schema = pa.schema(novos_campos)
    return pa.RecordBatch.from_arrays(novos_arrays, schema=novo_schema)


def marcar_linhas_suspeitas(df: pl.DataFrame, limites: dict) -> pl.DataFrame:
    """
    Adds the boolean column _suspeita_deslocamento, True when some "code" field
    exceeds the max length expected by the RFB layout (a symptom of a column
    shift inherited from Bronze/Silver parsing). Never removes any row --
    correction/exclusion is a decision for downstream (dbt) layers, not landing.
    """
    if not limites:
        return df.with_columns(pl.lit(False).alias("_suspeita_deslocamento"))

    condicao_suspeita = pl.lit(False)
    for coluna, limite in limites.items():
        if coluna in df.columns:
            condicao_suspeita = condicao_suspeita | (
                pl.col(coluna).str.len_chars().fill_null(0) > limite
            )
    return df.with_columns(condicao_suspeita.alias("_suspeita_deslocamento"))


def preparar_schema_tabela(engine, schema, tabela, ddl, colunas_para_text, ref_mes_int, logger):
    """
    Runs the DDL (CREATE SCHEMA/TABLE IF NOT EXISTS), widens existing "code"
    columns to TEXT (idempotent -- needed if the table already exists from a
    previous run with stricter types), ensures the flag column exists, and
    clears the target partition (referencia_mes) so reprocessing is idempotent.
    """
    logger.info(f"Validating schema/table and cleaning partition {ref_mes_int} for {schema}.{tabela}")
    with engine.begin() as conn:
        conn.execute(text(ddl))

        for coluna in colunas_para_text:
            conn.execute(text(
                f"ALTER TABLE {schema}.{tabela} ALTER COLUMN {coluna} TYPE TEXT"
            ))
        conn.execute(text(
            f"ALTER TABLE {schema}.{tabela} "
            f"ADD COLUMN IF NOT EXISTS _suspeita_deslocamento BOOLEAN DEFAULT FALSE"
        ))

        registros_deletados = conn.execute(
            text(f"DELETE FROM {schema}.{tabela} WHERE referencia_mes = :ref_mes"),
            {"ref_mes": ref_mes_int}
        ).rowcount
        if registros_deletados > 0:
            logger.info(f"Removed {registros_deletados} existing row(s) from partition {ref_mes_int}.")


def sanitizar_nul_bytes(df: pl.DataFrame) -> pl.DataFrame:
    """
    Removes literal NUL bytes (0x00) from every string column. Postgres rejects
    0x00 in any text column regardless of encoding -- it's a low-level limitation
    of its internal string representation, not a UTF-8 validity issue. Control
    characters like this occasionally survive RFB's raw files (often introduced
    by the errors='replace' decoding step in the Bronze layer) and would
    otherwise abort the whole COPY batch with "invalid byte sequence".
    """
    colunas_string = [nome for nome, dtype in df.schema.items() if dtype == pl.Utf8]
    if not colunas_string:
        return df
    return df.with_columns([
        pl.col(c).str.replace_all("\x00", "", literal=True) for c in colunas_string
    ])


def inserir_dataframe_bulk_copy(df: pl.DataFrame, engine, schema, tabela, pasta_tmp, logger):
    """
    Serializes the DataFrame to TSV (in-memory buffer) and streams it via
    COPY FROM STDIN. Do NOT use quote_style="always": Postgres only treats an
    empty field as NULL when it is unquoted; forcing quotes on every field
    turns each NULL into a literal '""' value, which breaks numeric/date columns.
    """
    df = sanitizar_nul_bytes(df)

    buffer = io.BytesIO()
    df.write_csv(buffer, separator='\t', null_value='')
    buffer.seek(0)

    colunas_str = ", ".join(df.columns)
    copy_sql = f"""
        COPY {schema}.{tabela} ({colunas_str})
        FROM STDIN WITH (FORMAT CSV, HEADER TRUE, NULL '', DELIMITER '\t', QUOTE '"', ESCAPE '"')
    """

    conn = engine.raw_connection()
    try:
        with conn.cursor() as cur:
            cur.copy_expert(copy_sql, buffer)
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Error during COPY into {schema}.{tabela}: {e}")
        try:
            debug_path = os.path.join(pasta_tmp, f"failed_batch_{schema}_{tabela}.tsv")
            buffer.seek(0)
            with open(debug_path, "wb") as f:
                f.write(buffer.read())
            logger.error(f"Failed batch saved to: {debug_path} for manual inspection.")
        except Exception as dump_err:
            logger.error(f"Failed to save debug batch: {dump_err}")
        raise e
    finally:
        conn.close()


def processar_arquivos_landing(
    s3_client, engine, bucket_silver, entidade, ref_mes_int,
    schema, tabela, ddl, colunas_para_text, limites_suspeita,
    pasta_tmp, tamanho_lote, logger,
):
    """
    Orchestrates the full landing load for one entity: lists the Silver parquet
    files, downloads each one, iterates in batches applying the hardening steps
    above, and inserts via COPY.

    Returns (total_rows_inserted, total_rows_flagged_suspicious).
    """
    os.makedirs(pasta_tmp, exist_ok=True)
    prefixo_silver = f"rfb/{entidade}/ref_month={ref_mes_int}/"

    res = s3_client.list_objects_v2(Bucket=bucket_silver, Prefix=prefixo_silver)
    arquivos_s3 = sorted([
        obj["Key"] for obj in res.get("Contents", []) if obj["Key"].endswith(".parquet")
    ])

    if not arquivos_s3:
        logger.warning(f"No files found at s3://{bucket_silver}/{prefixo_silver}")
        return 0, 0

    preparar_schema_tabela(engine, schema, tabela, ddl, colunas_para_text, ref_mes_int, logger)

    total_linhas_geral = 0
    total_suspeitas_geral = 0

    for chave_s3 in arquivos_s3:
        nome_arq = os.path.basename(chave_s3)
        path_local = os.path.join(pasta_tmp, f"landing_{nome_arq}")

        logger.info(f">>> downloading {nome_arq} from S3 for local processing")
        s3_client.download_file(bucket_silver, chave_s3, path_local)

        parquet_file = pq.ParquetFile(path_local)
        total_linhas_arquivo = parquet_file.metadata.num_rows
        logger.info(f">>> starting copy of {nome_arq} into Postgres (total: {total_linhas_arquivo} rows)")

        linhas_processadas = 0
        for batch in parquet_file.iter_batches(batch_size=tamanho_lote):
            batch = batch_para_large_string(batch)
            df_lote = pl.from_arrow(batch)

            df_lote = marcar_linhas_suspeitas(df_lote, limites_suspeita)

            qtd_suspeitas = df_lote.select(pl.col("_suspeita_deslocamento").sum()).item()
            if qtd_suspeitas > 0:
                total_suspeitas_geral += qtd_suspeitas
                logger.warning(
                    f">>> >>> >>> {qtd_suspeitas} suspicious row(s) in this batch "
                    f"(inserted anyway, flagged _suspeita_deslocamento = TRUE)"
                )

            inserir_dataframe_bulk_copy(df_lote, engine, schema, tabela, pasta_tmp, logger)

            linhas_processadas += df_lote.height
            logger.info(f">>> inserted batch {linhas_processadas}/{total_linhas_arquivo} rows")

            del df_lote
            gc.collect()

        total_linhas_geral += linhas_processadas
        os.remove(path_local)
        logger.info(f">>> file {nome_arq} done")

    if total_suspeitas_geral > 0:
        logger.warning(
            f">>> {total_suspeitas_geral} suspicious row(s) total for entity '{entidade}' "
            f"(all inserted; filter with _suspeita_deslocamento = false downstream if needed)"
        )

    return total_linhas_geral, total_suspeitas_geral