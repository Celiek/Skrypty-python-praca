import psycopg2
import re
import pandas as pd
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import os

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    database=os.getenv("DB_NAME"),
    port = os.getenv("PORT"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)


def pobierz_dane_z_bazy_danych():
    df = pd.read_sql("SELECT * FROM merchanci;",conn)
    duplikaty = df[df.duplicated(subset=["nip"], keep=False)]
    df_braki = df[
    (df["adres"].isna() | (df["adres"] == "")) &
    (df["nr_konta_sm"].isna() | (df["nr_konta_sm"] == ""))
]

    with pd.ExcelWriter("wynik.xlsx") as writer:
        df.to_excel(writer, sheet_name="Dane", index=False)
        duplikaty.to_excel(writer, sheet_name="Duplikaty", index=False)
        df_braki.to_excel(writer,sheet_name="Braki", index = False)

pobierz_dane_z_bazy_danych()