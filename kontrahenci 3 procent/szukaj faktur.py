import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

# === konfiguracja połączenia z bazą ===
load_dotenv()
DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)

# === zapytanie SQL łączące wszystkie potrzebne tabele ===
query = """
SELECT 
    fp.numer_faktury AS nr_faktury_prowizyjnej,
    f.numer_faktury AS nr_faktury_zrodlowej,
    f.data_wystawienia AS data_faktury_zrodlowej,
    f.kwota_netto AS netto,
    f.kwota_brutto AS brutto,
    fp.nip AS nip_kontrahenta,
    fp.kontrahent AS nazwa_kontrahenta
FROM powiazania_faktur pf
JOIN faktury_prowizje fp ON pf.id_faktury_prowizji = fp.id_faktury_prowizji
JOIN faktury f ON pf.id_faktury_zrodlowej = f.id_faktury
ORDER BY fp.numer_faktury, f.data_wystawienia;
"""

# === pobranie danych do DataFrame ===
df = pd.read_sql(query, engine)

# === grupowanie po fakturze prowizyjnej ===
raport = (
    df.groupby(
        ["nr_faktury_prowizyjnej", "nip_kontrahenta", "nazwa_kontrahenta"]
    )["nr_faktury_zrodlowej"]
    .apply(lambda x: ", ".join(sorted(x)))
    .reset_index()
    .rename(columns={"nr_faktury_zrodlowej": "powiazane_faktury"})
)

# === wyświetlenie raportu ===
print("\n=== RAPORT POWIĄZAŃ FAKTUR ===")
print(raport)

# === zapis do Excela (opcjonalnie) ===
raport.to_excel("raport_powiazania_faktur.xlsx", index=False)
print("\nZapisano raport do pliku: raport_powiazania_faktur.xlsx")
