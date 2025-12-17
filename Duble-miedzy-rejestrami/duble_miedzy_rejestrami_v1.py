import pandas as pd

# === KONFIGURACJA ===
plik2 = r"zakup test shumee 17.11 folder.xlsx"
plik1 = r"zrzut optima 01.07-20.11 shumee"
plik_wynikowy = "duplikaty shumee 20.11.25.xlsx"

# Kolumny po których porównujemy
kolumny_kluczowe = ['Netto', 'Brutto', 'Nip', 'Numer dokumentu','Data wystawienia']

#kolumny_kluczowe = ['Numer dokumentu']

# === 1. Wczytanie danych ===
df1 = pd.read_excel(plik1)
df2 = pd.read_excel(plik2)

# === 2. Normalizacja nazw kolumn (na wszelki wypadek różne formaty) ===
df1.columns = df1.columns.str.strip().str.lower()
df2.columns = df2.columns.str.strip().str.lower()

# Dopasowanie nazw do formatu porównawczego
mapowanie = {
    'netto': 'Netto',
    'brutto': 'Brutto',
    'nip': 'Nip',
    'numer dokumentu': 'Numer dokumentu',
    "numer dokumentu": 'Numer dokumentu'  # czasem Excel daje cudzysłów
}

df1 = df1.rename(columns=mapowanie)
df2 = df2.rename(columns=mapowanie)

# === 3. Usuwanie wierszy z brakami w kolumnach kluczowych ===
df1 = df1.dropna(subset=kolumny_kluczowe)
df2 = df2.dropna(subset=kolumny_kluczowe)

# === 4. Konwersja typów i czyszczenie wartości ===
def normalizuj(df):
    df = df.copy()
    df['Nip'] = df['Nip'].astype(str).str.replace(r'\D', '', regex=True)  # tylko cyfry

    for col in ['Netto', 'Brutto']:
        df[col] = (
            df[col].astype(str)
            .str.replace(',', '.', regex=False)
            .str.replace(r'\s+', '', regex=True)  # usuń spacje i taby z liczb
            .replace('', '0')
            .astype(float)
        )

    # czyszczenie numeru dokumentu
    df['Numer dokumentu'] = (
        df['Numer dokumentu']
        .astype(str)
        .str.strip()                           # usuwa spacje z początku i końca
        .str.replace(r'\s+', '', regex=True)   # usuwa spacje wewnątrz
        .str.replace('\xa0', '', regex=False)  # usuwa niełamliwe spacje
        .str.replace('\t', '', regex=False)    # usuwa tabulatory
        .str.replace('\n', '', regex=False)    # usuwa nowe linie
        #.str.upper()                           # (opcjonalnie) ujednolica wielkość liter
    )

    return df


df1 = normalizuj(df1)
df2 = normalizuj(df2)

duplikaty = pd.merge(df1, df2, on=kolumny_kluczowe, how='inner')

# === 6. Zapis do Excela ===
if not duplikaty.empty:
    duplikaty.to_excel(plik_wynikowy, index=False)
    print(f"✅ Zapisano {len(duplikaty)} duplikatów do pliku: {plik_wynikowy}")
else:
    print("✅ Brak duplikatów między plikami.")