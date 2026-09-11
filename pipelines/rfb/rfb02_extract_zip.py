
import os
import sys
import zipfile
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

def process_zips_to_parquet():
    s3_client = get_s3_client()
    
    if not os.path.exists(SOURCE_DIR):
        logger.error(f"Source directory does not exist: {SOURCE_DIR}")
        return

    pending_files = sorted([f for f in os.listdir(SOURCE_DIR) if f.endswith('.zip')])
    logger.info(f"Found {len(pending_files)} ZIP files for bronze ingestion.")

    N_COLUNAS = 30
    column_names = [f"f{i}" for i in range(N_COLUNAS)]
    base_schema = pa.schema([pa.field(col, pa.string()) for col in column_names] + [pa.field("referencia_mes", pa.string())])

    for zip_file in pending_files:
        zip_path = os.path.join(SOURCE_DIR, zip_file)
        base_name = zip_file.replace('.zip', '')
        local_parquet = os.path.join(SOURCE_DIR, f"{base_name}.parquet")
        extracted_file = None
        s3_prefix = f"rfb/ref{REFERENCIA.replace('-', '')}/{base_name}.parquet"
        
        logger.info(f"Processing archive: {zip_file}")
        
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

            with open(extracted_file, 'r', encoding='iso-8859-1', errors='replace') as file:
                for line in file:
                    line_clean = line.strip().rstrip('\r\n')
                    if not line_clean:
                        continue
                        
                    fields = line_clean.split(';')
                    fields = [c.strip('"') for c in fields]
                    
                    if len(fields) > N_COLUNAS:
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

            logger.info(f"Uploading to s3://{BUCKET_BRONZE}/{s3_prefix} ({row_count} rows)")
            s3_client.upload_file(local_parquet, BUCKET_BRONZE, s3_prefix)
            logger.info(f"Upload completed successfully for {base_name}")

        except Exception as e:
            logger.error(f"Unrecoverable error processing {zip_file}: {e}")
            
        finally:
            if extracted_file and os.path.exists(extracted_file):
                os.remove(extracted_file)
            if os.path.exists(local_parquet):
                os.remove(local_parquet)

if __name__ == "__main__":
    process_zips_to_parquet()