import pandas as pd
from sqlalchemy import create_engine
import re
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert
from dotenv import load_dotenv
import os

# Kod na podstawie różnic pomiędzy bazą danych a plikiem xlsx 
# różnice zapisuje w pliku i przechowuje w pamięci
# tą "różnicę" następnie zapisuje w bazie danych     
# kod wymaga pliku z clickupa do działania ze zmienioną nazwą 2 giej kolumy
# status na : relation_status

load_dotenv()

DB_URL = os.getenv("DB_URL")
EXCEL_PATH = "clickup_tasks_clean.xlsx"
def clean_nip(x) -> str | None:
    if pd.isna(x):
        return None
    s = re.sub(r"\D", "", str(x))  # tylko cyfry
    return s if len(s) == 10 else None

def clean_status(x) -> str | None:
    if pd.isna(x):
        return None
    return str(x).strip().lower()

# --- Baza: tylko status=merchant ---
engine = create_engine(DB_URL)
with engine.begin() as con:
    df_db = pd.read_sql(
        "SELECT nip::text AS nip FROM merchanci WHERE status = 'merchant' AND nip IS NOT NULL",
        con
    )
df_db["nip_norm"] = df_db["nip"].map(clean_nip)
nipy_db = set(df_db["nip_norm"].dropna().unique())

# --- Excel: wczytaj NIP + Status i przefiltruj jak w bazie ---
df_xlsx = pd.read_excel(EXCEL_PATH, usecols=["NIP", "Status"], dtype={"NIP": "string", "Status": "string"})
df_xlsx["Status_norm"] = df_xlsx["Status"].map(clean_status)
df_xlsx = df_xlsx[df_xlsx["Status_norm"] == "merchant"].copy()

# znormalizuj NIP-y
df_xlsx["NIP_norm"] = df_xlsx["NIP"].map(clean_nip)
df_xlsx = df_xlsx[df_xlsx["NIP_norm"].notna()].drop_duplicates(subset=["NIP_norm"])

# różnica: co jest w pliku, a nie ma w bazie
missing = set(df_xlsx["NIP_norm"]) - nipy_db

# raport 1: same unikalne NIP-y
pd.DataFrame({'NIP': sorted(missing)}) \
  .to_csv("brakujace_nipy.csv", index=False, encoding="utf-8-sig")

# raport 2: pełne wiersze z Excela dla brakujących NIP-ów
df_missing_rows = df_xlsx[df_xlsx["NIP_norm"].isin(missing)]
df_missing_rows.to_csv("brakujace_nipy_wiersze.csv", index=False, encoding="utf-8-sig")

print("Kolumny w pliku:", df_xlsx.columns.tolist())

col_map = {
    "ID": "id",
    "NIP": "nip",
    "Nazwa": "nazwa",
    "Status": "status",
    "📧 Merchant mail": "email"
}

available = [c for c in col_map.keys() if c in df_missing_rows.columns]
df_to_insert = df_missing_rows.rename(columns=col_map)[[col_map[c] for c in available]]


meta = MetaData()
meta.reflect(bind=engine)
t_merchanci = Table("merchanci", meta, autoload_with=engine)

records = df_to_insert.to_dict(orient="records")

with engine.begin() as conn:
    stmt = insert(t_merchanci).values(records)
    stmt = stmt.on_conflict_do_nothing()
    conn.execute(stmt)

print(f"✅ Dodano {len(records)} nowych rekordów do tabeli merchanci")

print(f"Znaleziono {len(missing)} brakujących NIP-ów, zapisano do CSV.")