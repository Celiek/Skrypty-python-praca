import pandas as pd

# === KONFIGURACJA ===
plik_wejsciowy = "test shumee 17.11.xlsx"
plik_wyjsciowy = "test shumee 17.11 - bez dupl.xlsx"

# Kolumny, po których ma wykrywać duplikaty
kolumny_duplikat = ["Netto", "Numer dokumentu", "Brutto","VAT"]  # <-- zmień według potrzeby

# === 1. Wczytanie pliku ===
df = pd.read_excel(plik_wejsciowy)

# === 2. Usuwanie duplikatów po wskazanych 3 kolumnach ===
df_clean = df.drop_duplicates(subset=kolumny_duplikat, keep="first")

# Opcje:
# keep="first"  → zostawia pierwszy rekord
# keep="last"   → zostawia ostatni
# keep=False    → usuwa wszystkie duplikaty z grupy

# === 3. Zapis wyniku ===
df_clean.to_excel(plik_wyjsciowy, index=False)

print(f"✅ Zakończono! Zapisano plik: {plik_wyjsciowy}")
print(f"📉 Usunięto {len(df) - len(df_clean)} duplikatów.")
