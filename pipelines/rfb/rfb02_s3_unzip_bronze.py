# import os
# import sys
# import zipfile
# import pyarrow.csv as pv
# import pyarrow.parquet as pq
# import pyarrow as pa
# from warnings import filterwarnings

# filterwarnings("ignore")

# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

# from pipelines.commons.env_loader import (
#     MINIO_ACCESS_KEY, MINIO_SECRET_KEY, validate_env,
# )
# from pipelines.commons.s3_client import get_s3_client
# from pipelines.commons.logger import get_logger

# logger = get_logger("rfb_extract")

# validate_env({
#     "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
#     "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
# })

# REFERENCIA = "2026-08"
# SOURCE_DIR = f"/mnt/datasource/rfb/ref{REFERENCIA.replace('-', '')}"
# BUCKET_BRONZE = "bronze"

# def process_zips_to_parquet():
#     s3_client = get_s3_client()
    
#     if not os.path.exists(SOURCE_DIR):
#         logger.error(f"Source directory does not exist: {SOURCE_DIR}")
#         return

#     all_zip_files = sorted([f for f in os.listdir(SOURCE_DIR) if f.endswith('.zip')])
#     total_files = len(all_zip_files)
    
#     if not total_files:
#         logger.info(f"No ZIP files found in {SOURCE_DIR} to process.")
#         return

#     pending_files = []
#     skipped_count = 0
    
#     for zip_file in all_zip_files:
#         base_name = zip_file.replace('.zip', '')
#         s3_prefix = f"rfb/ref{REFERENCIA.replace('-', '')}/{base_name}.parquet"
        
#         try:
#             s3_client.head_object(Bucket=BUCKET_BRONZE, Key=s3_prefix)
#             logger.info(f"Skipping {zip_file}: already exists in s3://{BUCKET_BRONZE}/{s3_prefix}")
#             skipped_count += 1
#         except Exception:
#             pending_files.append(zip_file)

#     if not pending_files:
#         logger.info(f"Pipeline execution completed. Total ZIPs: {total_files} | Already in S3: {skipped_count} | Processed this run: 0")
#         return

#     logger.info(f"Found {len(pending_files)} pending ZIP files for bronze ingestion out of {total_files} total ({skipped_count} already present).")

#     N_COLUNAS = 30
#     column_names = [f"f{i}" for i in range(N_COLUNAS)]
#     base_schema = pa.schema([pa.field(col, pa.string()) for col in column_names] + [pa.field("referencia_mes", pa.string())])

#     processed_count = 0

#     for zip_file in pending_files:
#         zip_path = os.path.join(SOURCE_DIR, zip_file)
#         base_name = zip_file.replace('.zip', '')
#         local_parquet = os.path.join(SOURCE_DIR, f"{base_name}.parquet")
#         extracted_file = None
#         s3_prefix = f"rfb/ref{REFERENCIA.replace('-', '')}/{base_name}.parquet"
        
#         logger.info(f"Processing archive: {zip_file}")
        
#         try:
#             with zipfile.ZipFile(zip_path, 'r') as z:
#                 internal_name = z.namelist()[0]
#                 z.extract(internal_name, path=SOURCE_DIR)
#                 extracted_file = os.path.join(SOURCE_DIR, internal_name)

#             writer = pq.ParquetWriter(local_parquet, base_schema, compression='snappy')
#             chunk_size = 100_000
#             batch_data = {col: [] for col in column_names}
#             batch_data["referencia_mes"] = []
#             row_count = 0

#             with open(extracted_file, 'r', encoding='iso-8859-1', errors='replace') as file:
#                 for line in file:
#                     line_clean = line.strip().rstrip('\r\n')
#                     if not line_clean:
#                         continue
                        
#                     fields = line_clean.split(';')
#                     fields = [c.strip('"') for c in fields]
                    
#                     if len(fields) > N_COLUNAS:
#                         adjusted_fields = fields[:N_COLUNAS-1]
#                         adjusted_fields.append(" ".join(fields[N_COLUNAS-1:]))
#                         fields = adjusted_fields
#                     elif len(fields) < N_COLUNAS:
#                         fields.extend([""] * (N_COLUNAS - len(fields)))

#                     for idx, col in enumerate(column_names):
#                         batch_data[col].append(fields[idx])
#                     batch_data["referencia_mes"].append(REFERENCIA)

#                     row_count += 1

#                     if row_count % chunk_size == 0:
#                         table_batch = pa.Table.from_pydict(batch_data, schema=base_schema)
#                         writer.write_table(table_batch)
#                         batch_data = {col: [] for col in column_names}
#                         batch_data["referencia_mes"] = []

#             if batch_data[column_names[0]]:
#                 table_batch = pa.Table.from_pydict(batch_data, schema=base_schema)
#                 writer.write_table(table_batch)

#             writer.close()

#             logger.info(f"Uploading to s3://{BUCKET_BRONZE}/{s3_prefix} ({row_count} rows)")
#             s3_client.upload_file(local_parquet, BUCKET_BRONZE, s3_prefix)
#             logger.info(f"Upload completed successfully for {base_name}")
            
#             processed_count += 1

#         except Exception as e:
#             logger.error(f"Unrecoverable error processing {zip_file}: {e}")
            
#         finally:
#             if extracted_file and os.path.exists(extracted_file):
#                 os.remove(extracted_file)
#             if os.path.exists(local_parquet):
#                 os.remove(local_parquet)

#     logger.info(f"Pipeline execution completed. Total ZIPs: {total_files} | Already in S3: {skipped_count} | Processed this run: {processed_count}")

# if __name__ == "__main__":
#     process_zips_to_parquet()

# #V2
# import os
# import sys
# import zipfile
# import re
# import pyarrow.csv as pv
# import pyarrow.parquet as pq
# import pyarrow as pa
# from warnings import filterwarnings

# filterwarnings("ignore")

# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

# from pipelines.commons.env_loader import (
#     MINIO_ACCESS_KEY, MINIO_SECRET_KEY, validate_env,
# )
# from pipelines.commons.s3_client import get_s3_client
# from pipelines.commons.logger import get_logger

# logger = get_logger("rfb_extract")

# validate_env({
#     "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
#     "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
# })

# REFERENCIA = "2026-08"
# SOURCE_DIR = f"/mnt/datasource/rfb/ref{REFERENCIA.replace('-', '')}"
# BUCKET_BRONZE = "bronze"

# def to_snake_case(text):
#     """
#     Converts PascalCase or CamelCase to snake_case and makes it lowercase.
#     Example: 'Estabelecimentos0' -> 'estabelecimentos0'
#     """
#     text = re.sub(r'(?<!^)(?=[A-Z])', '_', text).lower()
#     return text

# def get_entity_name(base_name):
#     """
#     Extracts entity name by stripping trailing digits and converting to snake_case.
#     Example: 'Estabelecimentos0' -> 'estabelecimentos'
#     """
#     clean_name = re.sub(r'\d+$', '', base_name)
#     return to_snake_case(clean_name)

# def process_zips_to_parquet():
#     s3_client = get_s3_client()
    
#     if not os.path.exists(SOURCE_DIR):
#         logger.error(f"Source directory does not exist: {SOURCE_DIR}")
#         return

#     all_zip_files = sorted([f for f in os.listdir(SOURCE_DIR) if f.endswith('.zip')])
#     total_files = len(all_zip_files)
    
#     if not total_files:
#         logger.info(f"No ZIP files found in {SOURCE_DIR} to process.")
#         return

#     pending_files = []
#     skipped_count = 0
#     ref_partition = REFERENCIA.replace('-', '')
    
#     for zip_file in all_zip_files:
#         base_name = zip_file.replace('.zip', '')
#         file_name_clean = to_snake_case(base_name)
#         entity = get_entity_name(base_name)
        
#         s3_prefix = f"rfb/{entity}/ref_month={ref_partition}/{file_name_clean}.parquet"
        
#         try:
#             s3_client.head_object(Bucket=BUCKET_BRONZE, Key=s3_prefix)
#             logger.info(f"Skipping {zip_file}: already exists in s3://{BUCKET_BRONZE}/{s3_prefix}")
#             skipped_count += 1
#         except Exception:
#             pending_files.append(zip_file)

#     if not pending_files:
#         logger.info(f"Pipeline execution completed. Total ZIPs: {total_files} | Already in S3: {skipped_count} | Processed this run: 0")
#         return

#     logger.info(f"Found {len(pending_files)} pending ZIP files for bronze ingestion out of {total_files} total ({skipped_count} already present).")

#     N_COLUNAS = 30
#     column_names = [f"f{i}" for i in range(N_COLUNAS)]
#     base_schema = pa.schema([pa.field(col, pa.string()) for col in column_names] + [pa.field("referencia_mes", pa.string())])

#     processed_count = 0

#     for zip_file in pending_files:
#         zip_path = os.path.join(SOURCE_DIR, zip_file)
#         base_name = zip_file.replace('.zip', '')
#         file_name_clean = to_snake_case(base_name)
#         entity = get_entity_name(base_name)
        
#         local_parquet = os.path.join(SOURCE_DIR, f"{file_name_clean}.parquet")
#         s3_prefix = f"rfb/{entity}/ref_month={ref_partition}/{file_name_clean}.parquet"
#         extracted_file = None
        
#         logger.info(f"Processing archive: {zip_file} into entity: {entity} as {file_name_clean}.parquet")
        
#         try:
#             with zipfile.ZipFile(zip_path, 'r') as z:
#                 internal_name = z.namelist()[0]
#                 z.extract(internal_name, path=SOURCE_DIR)
#                 extracted_file = os.path.join(SOURCE_DIR, internal_name)

#             writer = pq.ParquetWriter(local_parquet, base_schema, compression='snappy')
#             chunk_size = 100_000
#             batch_data = {col: [] for col in column_names}
#             batch_data["referencia_mes"] = []
#             row_count = 0

#             with open(extracted_file, 'r', encoding='iso-8859-1', errors='replace') as file:
#                 for line in file:
#                     line_clean = line.strip().rstrip('\r\n')
#                     if not line_clean:
#                         continue
                        
#                     fields = line_clean.split(';')
#                     fields = [c.strip('"') for c in fields]
                    
#                     if len(fields) > N_COLUNAS:
#                         adjusted_fields = fields[:N_COLUNAS-1]
#                         adjusted_fields.append(" ".join(fields[N_COLUNAS-1:]))
#                         fields = adjusted_fields
#                     elif len(fields) < N_COLUNAS:
#                         fields.extend([""] * (N_COLUNAS - len(fields)))

#                     for idx, col in enumerate(column_names):
#                         batch_data[col].append(fields[idx])
#                     batch_data["referencia_mes"].append(REFERENCIA)

#                     row_count += 1

#                     if row_count % chunk_size == 0:
#                         table_batch = pa.Table.from_pydict(batch_data, schema=base_schema)
#                         writer.write_table(table_batch)
#                         batch_data = {col: [] for col in column_names}
#                         batch_data["referencia_mes"] = []

#             if batch_data[column_names[0]]:
#                 table_batch = pa.Table.from_pydict(batch_data, schema=base_schema)
#                 writer.write_table(table_batch)

#             writer.close()

#             logger.info(f">>> deploying on s3://{BUCKET_BRONZE}/{s3_prefix} ({row_count} rows)")
#             s3_client.upload_file(local_parquet, BUCKET_BRONZE, s3_prefix)
#             logger.info(f">>> >>> deployment complete for {base_name}")
            
#             processed_count += 1

#         except Exception as e:
#             logger.error(f"Unrecoverable error processing {zip_file}: {e}")
            
#         finally:
#             if extracted_file and os.path.exists(extracted_file):
#                 os.remove(extracted_file)
#             if os.path.exists(local_parquet):
#                 os.remove(local_parquet)

#         logger.info('=' * 120)
#         logger.info(f"pipeline run complete. total zip files: {total_files} | already in s3: {skipped_count} | loaded on this run: {processed_count}")
#         logger.info('=' * 120)
#         logger.info('#' * 120)
#         logger.info('#' * 120)

# if __name__ == "__main__":
#     process_zips_to_parquet()

# import os
# import sys
# import zipfile
# import pyarrow.csv as pv
# import pyarrow.parquet as pq
# import pyarrow as pa
# from warnings import filterwarnings

# filterwarnings("ignore")

# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

# from pipelines.commons.env_loader import (
#     MINIO_ACCESS_KEY, MINIO_SECRET_KEY, validate_env,
# )
# from pipelines.commons.s3_client import get_s3_client
# from pipelines.commons.logger import get_logger

# logger = get_logger("rfb_extract")

# validate_env({
#     "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
#     "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
# })

# REFERENCIA = "2026-08"
# SOURCE_DIR = f"/mnt/datasource/rfb/ref{REFERENCIA.replace('-', '')}"
# BUCKET_BRONZE = "bronze"

# def process_zips_to_parquet():
#     s3_client = get_s3_client()
    
#     if not os.path.exists(SOURCE_DIR):
#         logger.error(f"Source directory does not exist: {SOURCE_DIR}")
#         return

#     all_zip_files = sorted([f for f in os.listdir(SOURCE_DIR) if f.endswith('.zip')])
#     total_files = len(all_zip_files)
    
#     if not total_files:
#         logger.info(f"No ZIP files found in {SOURCE_DIR} to process.")
#         return

#     pending_files = []
#     skipped_count = 0
    
#     for zip_file in all_zip_files:
#         base_name = zip_file.replace('.zip', '')
#         s3_prefix = f"rfb/ref{REFERENCIA.replace('-', '')}/{base_name}.parquet"
        
#         try:
#             s3_client.head_object(Bucket=BUCKET_BRONZE, Key=s3_prefix)
#             logger.info(f"Skipping {zip_file}: already exists in s3://{BUCKET_BRONZE}/{s3_prefix}")
#             skipped_count += 1
#         except Exception:
#             pending_files.append(zip_file)

#     if not pending_files:
#         logger.info(f"Pipeline execution completed. Total ZIPs: {total_files} | Already in S3: {skipped_count} | Processed this run: 0")
#         return

#     logger.info(f"Found {len(pending_files)} pending ZIP files for bronze ingestion out of {total_files} total ({skipped_count} already present).")

#     N_COLUNAS = 30
#     column_names = [f"f{i}" for i in range(N_COLUNAS)]
#     base_schema = pa.schema([pa.field(col, pa.string()) for col in column_names] + [pa.field("referencia_mes", pa.string())])

#     processed_count = 0

#     for zip_file in pending_files:
#         zip_path = os.path.join(SOURCE_DIR, zip_file)
#         base_name = zip_file.replace('.zip', '')
#         local_parquet = os.path.join(SOURCE_DIR, f"{base_name}.parquet")
#         extracted_file = None
#         s3_prefix = f"rfb/ref{REFERENCIA.replace('-', '')}/{base_name}.parquet"
        
#         logger.info(f"Processing archive: {zip_file}")
        
#         try:
#             with zipfile.ZipFile(zip_path, 'r') as z:
#                 internal_name = z.namelist()[0]
#                 z.extract(internal_name, path=SOURCE_DIR)
#                 extracted_file = os.path.join(SOURCE_DIR, internal_name)

#             writer = pq.ParquetWriter(local_parquet, base_schema, compression='snappy')
#             chunk_size = 100_000
#             batch_data = {col: [] for col in column_names}
#             batch_data["referencia_mes"] = []
#             row_count = 0

#             with open(extracted_file, 'r', encoding='iso-8859-1', errors='replace') as file:
#                 for line in file:
#                     line_clean = line.strip().rstrip('\r\n')
#                     if not line_clean:
#                         continue
                        
#                     fields = line_clean.split(';')
#                     fields = [c.strip('"') for c in fields]
                    
#                     if len(fields) > N_COLUNAS:
#                         adjusted_fields = fields[:N_COLUNAS-1]
#                         adjusted_fields.append(" ".join(fields[N_COLUNAS-1:]))
#                         fields = adjusted_fields
#                     elif len(fields) < N_COLUNAS:
#                         fields.extend([""] * (N_COLUNAS - len(fields)))

#                     for idx, col in enumerate(column_names):
#                         batch_data[col].append(fields[idx])
#                     batch_data["referencia_mes"].append(REFERENCIA)

#                     row_count += 1

#                     if row_count % chunk_size == 0:
#                         table_batch = pa.Table.from_pydict(batch_data, schema=base_schema)
#                         writer.write_table(table_batch)
#                         batch_data = {col: [] for col in column_names}
#                         batch_data["referencia_mes"] = []

#             if batch_data[column_names[0]]:
#                 table_batch = pa.Table.from_pydict(batch_data, schema=base_schema)
#                 writer.write_table(table_batch)

#             writer.close()

#             logger.info(f"Uploading to s3://{BUCKET_BRONZE}/{s3_prefix} ({row_count} rows)")
#             s3_client.upload_file(local_parquet, BUCKET_BRONZE, s3_prefix)
#             logger.info(f"Upload completed successfully for {base_name}")
            
#             processed_count += 1

#         except Exception as e:
#             logger.error(f"Unrecoverable error processing {zip_file}: {e}")
            
#         finally:
#             if extracted_file and os.path.exists(extracted_file):
#                 os.remove(extracted_file)
#             if os.path.exists(local_parquet):
#                 os.remove(local_parquet)

#     logger.info(f"Pipeline execution completed. Total ZIPs: {total_files} | Already in S3: {skipped_count} | Processed this run: {processed_count}")

# if __name__ == "__main__":
#     process_zips_to_parquet()

import os
import sys
import zipfile
import re
import csv
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow as pa
from warnings import filterwarnings

filterwarnings("ignore")

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pipelines.commons.env_loader import (
    MINIO_ACCESS_KEY, MINIO_SECRET_KEY, validate_env,
)
from pipelines.commons.s3_client import get_s3_client
from pipelines.commons.logger import get_logger

logger = get_logger("rfb_extract")

validate_env({
    "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
    "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
})

REFERENCIA = "2026-08"
SOURCE_DIR = f"/mnt/datasource/rfb/ref{REFERENCIA.replace('-', '')}"
BUCKET_BRONZE = "bronze"

def to_snake_case(text):
    """
    Converts PascalCase or CamelCase to snake_case and makes it lowercase.
    Example: 'Estabelecimentos0' -> 'estabelecimentos0'
    """
    text = re.sub(r'(?<!^)(?=[A-Z])', '_', text).lower()
    return text

def get_entity_name(base_name):
    """
    Extracts entity name by stripping trailing digits and converting to snake_case.
    Example: 'Estabelecimentos0' -> 'estabelecimentos'
    """
    clean_name = re.sub(r'\d+$', '', base_name)
    return to_snake_case(clean_name)

def process_zips_to_parquet():
    s3_client = get_s3_client()
    
    if not os.path.exists(SOURCE_DIR):
        logger.error(f"Source directory does not exist: {SOURCE_DIR}")
        return

    all_zip_files = sorted([f for f in os.listdir(SOURCE_DIR) if f.endswith('.zip')])
    total_files = len(all_zip_files)
    
    if not total_files:
        logger.info(f"No ZIP files found in {SOURCE_DIR} to process.")
        return

    pending_files = []
    skipped_count = 0
    ref_partition = REFERENCIA.replace('-', '')
    
    for zip_file in all_zip_files:
        base_name = zip_file.replace('.zip', '')
        file_name_clean = to_snake_case(base_name)
        entity = get_entity_name(base_name)
        
        s3_prefix = f"rfb/{entity}/ref_month={ref_partition}/{file_name_clean}.parquet"
        
        try:
            s3_client.head_object(Bucket=BUCKET_BRONZE, Key=s3_prefix)
            logger.info(f"Skipping {zip_file}: already exists in s3://{BUCKET_BRONZE}/{s3_prefix}")
            skipped_count += 1
        except Exception:
            pending_files.append(zip_file)

    if not pending_files:
        logger.info(f"Pipeline execution completed. Total ZIPs: {total_files} | Already in S3: {skipped_count} | Processed this run: 0")
        return

    logger.info(f"Found {len(pending_files)} pending ZIP files for bronze ingestion out of {total_files} total ({skipped_count} already present).")

    N_COLUNAS = 30
    column_names = [f"f{i}" for i in range(N_COLUNAS)]
    base_schema = pa.schema([pa.field(col, pa.string()) for col in column_names] + [pa.field("referencia_mes", pa.string())])

    processed_count = 0

    for zip_file in pending_files:
        zip_path = os.path.join(SOURCE_DIR, zip_file)
        base_name = zip_file.replace('.zip', '')
        file_name_clean = to_snake_case(base_name)
        entity = get_entity_name(base_name)
        
        local_parquet = os.path.join(SOURCE_DIR, f"{file_name_clean}.parquet")
        s3_prefix = f"rfb/{entity}/ref_month={ref_partition}/{file_name_clean}.parquet"
        extracted_file = None
        
        logger.info(f"Processing archive: {zip_file} into entity: {entity} as {file_name_clean}.parquet")
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as z:
                internal_name = z.namelist()[0]
                z.extract(internal_name, path=SOURCE_DIR)
                extracted_file = os.path.join(SOURCE_DIR, internal_name)

            writer = pq.ParquetWriter(local_parquet, base_schema, compression='snappy')
            chunk_size = 100_000
            batch_data = {col: [] for col in column_names}
            batch_data["referencia_mes"] = []
            row_count = 0

            with open(extracted_file, 'r', encoding='iso-8859-1', errors='replace', newline='') as file:
                # IMPORTANTE: usamos csv.reader (respeitando quotechar='"') em vez de um
                # split(';') manual. Os arquivos brutos da RFB trazem campos de texto livre
                # (nome_fantasia, logradouro, complemento, bairro...) entre aspas, e esses
                # campos podem conter um ';' literal DENTRO das aspas. Um split(';') ingênuo
                # quebra esse campo em dois, fazendo TODAS as colunas seguintes daquela linha
                # deslizarem uma posição (ex: cep vazando para uf, bairro sumindo). O csv.reader
                # trata corretamente ';' dentro de aspas como dado, não como delimitador.
                leitor = csv.reader(file, delimiter=';', quotechar='"')
                for fields in leitor:
                    if not fields or (len(fields) == 1 and fields[0].strip() == ''):
                        continue

                    fields = [c.strip() for c in fields]

                    if len(fields) > N_COLUNAS:
                        # Rede de segurança: com o parser correto isso deve ser raríssimo
                        # (ou indicar linha genuinamente malformada na fonte), mas ainda assim
                        # não perdemos dado: concatenamos o excedente no último campo.
                        logger.warning(
                            f"Linha com {len(fields)} campos (esperado {N_COLUNAS}) em {zip_file}, "
                            f"mesmo após parsing correto de aspas. Concatenando excedente no último campo."
                        )
                        adjusted_fields = fields[:N_COLUNAS-1]
                        adjusted_fields.append(" ".join(fields[N_COLUNAS-1:]))
                        fields = adjusted_fields
                    elif len(fields) < N_COLUNAS:
                        fields.extend([""] * (N_COLUNAS - len(fields)))

                    for idx, col in enumerate(column_names):
                        batch_data[col].append(fields[idx])
                    batch_data["referencia_mes"].append(REFERENCIA)

                    row_count += 1

                    if row_count % chunk_size == 0:
                        table_batch = pa.Table.from_pydict(batch_data, schema=base_schema)
                        writer.write_table(table_batch)
                        batch_data = {col: [] for col in column_names}
                        batch_data["referencia_mes"] = []

            if batch_data[column_names[0]]:
                table_batch = pa.Table.from_pydict(batch_data, schema=base_schema)
                writer.write_table(table_batch)

            writer.close()

            logger.info(f">>> deploying on with REV3 s3://{BUCKET_BRONZE}/{s3_prefix} ({row_count} rows)")
            s3_client.upload_file(local_parquet, BUCKET_BRONZE, s3_prefix)
            logger.info(f">>> >>> deployment complete for {base_name} WITH REV 3")
            
            processed_count += 1

        except Exception as e:
            logger.error(f"Unrecoverable error processing {zip_file}: {e}")
            
        finally:
            if extracted_file and os.path.exists(extracted_file):
                os.remove(extracted_file)
            if os.path.exists(local_parquet):
                os.remove(local_parquet)

        logger.info('=' * 120)
        logger.info(f"pipeline run complete. total zip files: {total_files} | already in s3: {skipped_count} | loaded on this run: {processed_count}")
        logger.info('=' * 120)
        logger.info('#' * 120)
        logger.info('#' * 120)

if __name__ == "__main__":
    process_zips_to_parquet()