import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# === KONFIGURACJA ===
load_dotenv()
DB_URL = os.getenv("DB_URL")  # np. postgresql+psycopg2://user:pass@localhost:5432/db
EXCEL_PATH = "lista kontrahentów wraz z nr_konta.xlsx"

# Kolumny z Excela
cols = ["NIP", "nr_konta"]

# === POŁĄCZENIE Z BAZĄ ===
engine = create_engine(DB_URL)

# === WCZYTANIE DANYCH ===
df = pd.read_excel(EXCEL_PATH, usecols=cols)
df = df.dropna(subset=["NIP", "nr_konta"])  # usuń wiersze bez NIP lub konta
df["NIP"] = df["NIP"].astype(str).str.replace(r"\D", "", regex=True)  # tylko cyfry w NIP
df["nr_konta"] = df["nr_konta"].astype(str).str.replace(r"\s", "", regex=True)  # usuń spacje

print("🔹 Dane wczytane:")
print(df.head())

# === AKTUALIZACJA BAZY ===
updated = 0
with engine.begin() as conn:
    for _, row in df.iterrows():
        nip = row["NIP"]
        konto = row["nr_konta"]

        result = conn.execute(
            text("""
                UPDATE merchanci
                SET nr_konta_sm = :konto
                WHERE nip = :nip
            """),
            {"konto": konto, "nip": nip}
        )
        updated += result.rowcount  # liczba zaktualizowanych rekordów

print(f"✅ Zaktualizowano {updated} rekordów w tabeli merchanci.")
