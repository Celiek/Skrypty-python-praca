import pandas as pd

# === Ścieżki plików ===
plik_wejsciowy = "shumee_02_10.xlsx"
plik_wynikowy = "shumee 02.10 bez dubli.xlsx"


kolumny_kluczowe =['Netto', 'Brutto', 'Vat', 'Nip', 'Numer dokumentu','Data wystawienia']

df = pd.read_excel(plik_wejsciowy)

# Normalizacja nazw kolumn (na wypadek spacji / wielkich liter)
df.columns = df.columns.str.strip().str.lower()
kolumny_kluczowe = [k.lower() for k in kolumny_kluczowe]

# === 2. Usuwanie duplikatów ===
# keep='first' → zachowuje pierwszy występujący wiersz, usuwa kolejne identyczne
df_bez_duplikatow = df.drop_duplicates(subset=kolumny_kluczowe, keep='first')

# === 3. Zapis do nowego pliku ===
df_bez_duplikatow.to_excel(plik_wynikowy, index=False)

print(f"Usunięto {len(df) - len(df_bez_duplikatow)} duplikatów.")
print(f"Czyste dane zapisano do pliku: {plik_wynikowy}")
