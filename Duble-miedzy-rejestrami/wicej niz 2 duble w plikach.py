import pandas as pd

# === Ścieżka do pliku ===
plik = "zcsdgsdfb_test.xlsx"
plik_wynik = "shumee_02_10.xlsx"

# === Kolumny, po których identyfikujemy duplikaty ===
kolumny_kluczowe = ['Netto', 'Brutto', 'Vat', 'Nip', 'Numer dokumentu','Data wystawienia']

# === 1. Wczytanie pliku ===
df = pd.read_excel(plik)

# Normalizacja nazw kolumn (żeby uniknąć błędów przez spacje lub wielkie litery)
df.columns = df.columns.str.strip().str.lower()
kolumny_kluczowe = [k.lower() for k in kolumny_kluczowe]

# Ujednolicenie typów danych (wszystko jako string dla pewności)
for col in kolumny_kluczowe:
    df[col] = df[col].astype(str).str.strip()

# === 2. Grupowanie i liczenie powtórek ===
grupa = (
    df.groupby(kolumny_kluczowe)
    .size()
    .reset_index(name="liczba_powtorzen")
)

# === 3. Filtrowanie duplikatów (2x i więcej) ===
duplikaty = grupa[grupa["liczba_powtorzen"] >= 2]

print(f"Znaleziono {len(duplikaty)} unikalnych kombinacji faktur występujących ≥2 razy.")

# === 4. Połączenie z oryginalnym plikiem, żeby zobaczyć pełne dane ===
duplikaty_full = df.merge(duplikaty[kolumny_kluczowe], on=kolumny_kluczowe, how="inner")

# === 5. Zapis do pliku ===
duplikaty_full.to_excel(plik_wynik, index=False)
print(f"Zapisano szczegóły duplikatów do pliku: {plik_wynik}")
