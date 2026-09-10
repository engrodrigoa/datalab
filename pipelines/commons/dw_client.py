import psycopg2
from pipelines.commons.env_loader import CONSTRING

def get_pg_connection():
    return psycopg2.connect(CONSTRING)