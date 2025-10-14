import pandas as pd
import psycopg2
import os
from psycopg2.extras import RealDictCursor


from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

def db_conn():
    return psycopg2.connect(**DB_CONFIG)

def fetch_adres(nipy) -> pd.DataFrame:
    if isinstance(nipy,pd.Series):
        nipy = nipy.dropna().to_frame().astype(str).str.strip().unique().tolist()
    elif isinstance(nipy, pd.DataFrame):
        nipy = nipy["NIP"].dropna().astype(str).str.strip().unique().tolist()
    elif isinstance(nipy, (list, tuple)):
        nipy = [str(n).strip() for n in nipy if n]
    else:
        nipy = [str(nipy).strip()]

    if not nipy:
        print("Brak nipów w typie dataseries")
    query = "Select adres from merchanci WHere nip = Any(%s:bigint[])"
    with db_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, (nipy,))
        rows = cur.fetchall()
    print("Emaile z bazy danych:")
    print(rows)

def main():
    d = {'nip': [8911641630,1130471313,5211776929,
                 7722100192,5140247559,5862386810,
                 6581991980,5372433637,8210007076,
                 5791480222]}
    df = pd.DataFrame(data=d)
    print(df)

main()