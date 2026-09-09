import os
import re
import sys
import time
import urllib.parse
from datetime import datetime
from warnings import filterwarnings

import requests
from bs4 import BeautifulSoup
import polars as pl
from sqlalchemy import create_engine
from dotenv import load_dotenv

filterwarnings("ignore")
load_dotenv()

# ==========================================
# 0. TRAVA DE SEGURANÇA (FAIL-FAST)
# ==========================================
REQUIRED_VARS = [
    "DB_HOST", "DB_USER", "DB_PASS", "DB_NAME"
]

missing_vars = [var for var in REQUIRED_VARS if not os.getenv(var)]
if missing_vars:
    print(f"[ERRO FATAL] Variáveis de ambiente obrigatórias ausentes no .env: {', '.join(missing_vars)}")
    sys.exit(1)

# ==========================================
# 1. CONFIGURAÇÕES DE DIRETÓRIO E BANCO
# ==========================================
# Diretório raiz local mapeado no Docker
DIR_LANDING = "/mnt/datasource/anp/arquivos_fechados"

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")
TARGET = "ctrl.anp_metadata_mensal"

CONSTRING = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(CONSTRING)

# ==========================================
# 2. CONFIGURAÇÕES DE SCRAPING
# ==========================================
PAGE_URL = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

CATEGORY_FOLDER_MAP = {
    "GLP": "glp",
    "GASOLINA": "combustivel",
    "DIESEL": "combustivel",
}

MONTHS_MAP = {
    "janeiro": 1, "fevereiro": 2, "março": 3, "marco": 3,
    "abril": 4, "maio": 5, "junho": 6, "julho": 7,
    "agosto": 8, "setembro": 9, "outubro": 10,
    "novembro": 11, "dezembro": 12,
}

PRODUCT_SLUGS = {
    "óleo diesel": "DIESEL", "diesel": "DIESEL",
    "etanol hidratado + gasolina c": "GASOLINA",
    "gasolina": "GASOLINA",
    "glp p13": "GLP", "glp": "GLP",
}

# ==========================================
# 3. FUNÇÕES DE PIPELINE
# ==========================================
def parse_portal_items() -> pl.DataFrame:
    """Faz o scraping da página da ANP e retorna um DataFrame Polars com os links mapeados."""
    response = requests.get(PAGE_URL, headers=HEADERS)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.content, "html.parser")
    portal_items = []
    content_area = soup.find("div", {"id": "content"}) or soup
    current_cat = None

    for element in content_area.find_all(["h1", "h2", "h3", "h4", "p", "strong", "ul"]):
        if element.name in ["h1", "h2", "h3", "h4", "p", "strong"]:
            text_clean = element.get_text(strip=True).lower()
            for key, slug_val in PRODUCT_SLUGS.items():
                if key in text_clean:
                    current_cat = slug_val
                    break

        elif element.name == "ul" and current_cat:
            for a_tag in element.find_all("a", href=True):
                link_name = a_tag.get_text(strip=True)
                match = re.search(r"([a-zA-ZçÇ]+)\s+de\s+(\d{4})", link_name, re.IGNORECASE)
                
                if match:
                    month_str = match.group(1).lower()
                    year_num = int(match.group(2))
                    month_num = MONTHS_MAP.get(month_str)

                    if month_num:
                        ref = f"{year_num}{month_num:02d}"
                        download_url = urllib.parse.urljoin(PAGE_URL, a_tag["href"])
                        portal_items.append({
                            "cat": current_cat, "ref": ref, "month": month_num,
                            "year": year_num, "link_name": link_name, "url_source": download_url,
                        })

    schema = {
        "cat": pl.Utf8, "ref": pl.Utf8, "month": pl.Int32,
        "year": pl.Int32, "link_name": pl.Utf8, "url_source": pl.Utf8,
    }

    if not portal_items:
        return pl.DataFrame(schema=schema)

    return pl.DataFrame(portal_items, schema=schema).unique(subset=["cat", "ref", "url_source"], keep="first")

def get_monit_db_dataframe() -> pl.DataFrame:
    query = f"SELECT cat, ref FROM {TARGET};"
    try:
        return pl.read_database_uri(query=query, uri=CONSTRING)
    except Exception:
        return pl.DataFrame({"cat": [], "ref": []}, schema={"cat": pl.Utf8, "ref": pl.Utf8})

def insert_monitoring_record(record: dict):
    """Insere o registro baixado na tabela de monitoramento."""
    df_record = pl.DataFrame([record])
    df_record.write_database(
        table_name=TARGET,
        connection=engine,
        if_table_exists="append",
        engine="sqlalchemy",
    )

# ==========================================
# 4. EXECUÇÃO PRINCIPAL
# ==========================================
def run_pipeline():
    # CHAVE DE TESTE: Mude para False para rodar em produção (todos os anos)
    TEST_ONLY_2026 = True 

    print(f"--- Iniciando Scraping ANP Mensal: {datetime.now()} ---")
    
    df_portal = parse_portal_items()
    if df_portal.is_empty():
        print("[SKIP] Estrutura inválida ou nenhum item mapeado no portal.")
        sys.exit(99)

    print(" -> Lendo tabela de monitoramento para comparação...")
    df_db = get_monit_db_dataframe()

    # Identifica apenas os arquivos pendentes via Anti-Join
    df_pending = df_portal.join(df_db, on=["cat", "ref"], how="anti")
    
    # ---------------------------------------------------------
    # APLICAÇÃO DO FILTRO DE TESTE
    # ---------------------------------------------------------
    if TEST_ONLY_2026:
        import polars as pl # Garantindo a importação para o filtro
        print("\n[MODO TESTE ATIVADO] Filtrando carga apenas para arquivos de 2026!\n")
        df_pending = df_pending.filter(pl.col("year") == 2026)
    # ---------------------------------------------------------

    pending_count = df_pending.height

    if pending_count == 0:
        print("[OK] Nenhum novo arquivo encontrado. O Lakehouse já está 100% atualizado.")
        sys.exit(99)

    print(f"\n[DIFERENÇA DETECTADA] Encontrado(s) {pending_count} arquivo(s) pendente(s).\n")
    downloaded_count = 0

    for row in df_pending.iter_rows(named=True):
        cat = row["cat"]
        print(f"-> Processando: {cat} | {row['link_name']} (REF: {row['ref']})")

        # Define a subpasta principal (combustivel ou glp)
        subfolder = CATEGORY_FOLDER_MAP.get(cat, "combustivel")
        
        extracted_filename = os.path.basename(urllib.parse.urlparse(row["url_source"]).path)
        if not extracted_filename or not extracted_filename.endswith(".csv"):
            extracted_filename = f"{cat.lower()}_{row['ref']}.csv"

        # Aponta direto para a pasta fixa 'mes' exigida pela árvore de diretórios
        destination_folder = os.path.join(DIR_LANDING, subfolder, "mes")
        os.makedirs(destination_folder, exist_ok=True)
        
        destination_local_path = os.path.join(destination_folder, extracted_filename)

        try:
            res = requests.get(row["url_source"], headers=HEADERS, stream=True)
            res.raise_for_status()

            # Streaming direto para a pasta 'mes' local
            with open(destination_local_path, 'wb') as out_file:
                for chunk in res.iter_content(chunk_size=8192):
                    out_file.write(chunk)

            monitoring_record = {
                "cat": cat,
                "ref": row["ref"],
                "month": row["month"],
                "year": row["year"],
                "file_name": f"{subfolder}/mes/{extracted_filename}",
                "url_source": row["url_source"],
                "download_date": datetime.now(),
                "link_name": row["link_name"],
            }

            insert_monitoring_record(monitoring_record)
            print(f"   [SUCESSO] Salvo localmente: {destination_local_path} | Registrado no DB.")
            downloaded_count += 1

        except Exception as e:
            print(f"   [ERRO] Falha no download de {row['url_source']}: {e}")

        time.sleep(2)

    print("\n" + "=" * 100)
    print(" -----> Pipeline de Ingestão executado com sucesso")
    print(f" Total processado: {downloaded_count}/{pending_count}")
    print(f" Destino principal: {DIR_LANDING}")
    print("=" * 100 + "\n")
    sys.exit(0)

if __name__ == "__main__":
    run_pipeline()