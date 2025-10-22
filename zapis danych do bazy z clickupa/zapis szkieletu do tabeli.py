import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# skrypt do odbudowy bazy danych (merchanci) 
# zapisuje jednorazowo dane z clickupa do tabeli 
# zapisuje podstawowe dane tj.  
# ["NIP", "Nazwa", "Status", "Merchant mail", "Regulations accept date"]

load_dotenv()
DB_URL = os.getenv("DB_URL")


EXCEL_PATH = "dane kontrahentów.xlsx"
TABLE_NAME = "merchanci"

cols = ["NIP", "Nazwa", "Status", "Merchant mail", "Regulations accept date"]

mappings = {
    "NIP": "nip",
    "Nazwa": "nazwa",
    "Status": "status",
    "Merchant mail": "email",
    "Regulations accept date": "data_akceptacji_regulaminu"
}

engine = create_engine(DB_URL)

df = pd.read_excel(EXCEL_PATH, usecols=cols)

df = df.rename(columns=mappings)
df["data_akceptacji_regulaminu"] = pd.to_datetime(df["data_akceptacji_regulaminu"], unit="ms").dt.date

df = df.dropna(subset=["nip"])
df["nip"] = df["nip"].astype(str).str.replace(r"\D", "", regex=True)
df["data_akceptacji_regulaminu"] = pd.to_datetime(df["data_akceptacji_regulaminu"], errors="coerce").dt.date

print("Dane wczytane i przygotowane:")
print(df.head())


df.to_sql(TABLE_NAME, engine, if_exists="append", index=False)
print(f"Zapisano rekordów: ({len(df)}) do tabeli")
