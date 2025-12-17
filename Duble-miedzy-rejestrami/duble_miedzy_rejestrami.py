import pandas as pd
from rapidfuzz import distance

# TODO
# sprawdzać po kombinacjach kolumn nie po pojedyńczych

plik1 = r"C:\Users\DELL\Downloads\GI_SHumee.xlsx"
plik2 = r"C:\Users\DELL\Sm Dropbox\Faktury 3 %\shumee\10.12.2025\raporty\raport_9730408592_Global_Service_Group_Pawe_Kapustka.xlsx"
plik_wynikowy = "duplikaty sm zaległe.xlsx"

THRESHOLD_DIST = 2   # maksymalna różnica znaków w nazwie dokumentu
TOLERANCJA_PLN = 1   # dopuszczalna różnica kwot w PLN

# === 1. Wczytanie danych ===FV
df1 = pd.read_excel(plik1)
df2 = pd.read_excel(plik2)

# === 2. Normalizacja minimalna ===
def normalizuj_minimalnie(df):
    df = df.copy()
    df['NIP'] = df['NIP'].astype(str).str.replace(r'\D', '', regex=True)
    for col in ['Netto', 'Brutto', 'VAT']:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(',', '.', regex=False)
            .str.replace(r'\s+', '', regex=True)
            .replace('', '0')
            .astype(float)
        )
    return df

df1 = normalizuj_minimalnie(df1)
df2 = normalizuj_minimalnie(df2)

# === 3. ETAP 1 — identyczne rekordy 1:1 (z tolerancją ±1 PLN) ===
mask = (
    (df1['NIP'].isin(df2['NIP'])) &
    (df1.apply(lambda r: any(abs(df2['Netto'] - r['Netto']) <= TOLERANCJA_PLN), axis=1)) &
    (df1.apply(lambda r: any(abs(df2['Brutto'] - r['Brutto']) <= TOLERANCJA_PLN), axis=1)) &
    (df1.apply(lambda r: any(abs(df2['VAT'] - r['VAT']) <= TOLERANCJA_PLN), axis=1)) &
    (df1['Numer dokumentu'].isin(df2['Numer dokumentu']))
)

duplikaty_oczywiste = df1[mask].merge(
    df2,
    on=['NIP', 'Numer dokumentu'],
    how='inner',
    suffixes=('_plik1', '_plik2')
)

if not duplikaty_oczywiste.empty:
    print(f"✅ Znaleziono {len(duplikaty_oczywiste)} oczywistych duplikatów 1:1.")
    df1 = df1[~mask]
    df2 = df2[~df2['Numer dokumentu'].isin(duplikaty_oczywiste['Numer dokumentu'])]
else:
    print("ℹ️ Brak oczywistych duplikatów 1:1.")

# === 4. ETAP 2 — nieoczywiste duplikaty (fuzzy matching, ±1 PLN) ===
wyniki = []

for i, r1 in df1.iterrows():
    kandydaci = df2[
        (df2['NIP'] == r1['NIP']) &
        (abs(df2['Netto'] - r1['Netto']) <= TOLERANCJA_PLN) &
        (abs(df2['Brutto'] - r1['Brutto']) <= TOLERANCJA_PLN) &
        (abs(df2['VAT'] - r1['VAT']) <= TOLERANCJA_PLN)
    ]

    if kandydaci.empty:
        continue

    for j, r2 in kandydaci.iterrows():
        dist = distance.Levenshtein.distance(
            str(r1['Numer dokumentu']), str(r2['Numer dokumentu'])
        )

        if 0 < dist <= THRESHOLD_DIST:
            wyniki.append({
                "NIP": r1['NIP'],
                "Numer dokumentu_plik1": r1['Numer dokumentu'],
                "Numer dokumentu_plik2": r2['Numer dokumentu'],
                "różnica_znaków": dist,
                "Netto_plik1": r1['Netto'],
                "Netto_plik2": r2['Netto'],
                "Brutto_plik1": r1['Brutto'],
                "Brutto_plik2": r2['Brutto'],
                "VAT_plik1": r1['VAT'],
                "VAT_plik2": r2['VAT']
            })

duplikaty_nieoczywiste = pd.DataFrame(wyniki)

# === 5. Usuwanie symetrycznych duplikatów (A-B == B-A) ===
if not duplikaty_nieoczywiste.empty:
    duplikaty_nieoczywiste['para_klucz'] = duplikaty_nieoczywiste.apply(
        lambda x: tuple(sorted([
            str(x['Numer dokumentu_plik1']).strip(),
            str(x['Numer dokumentu_plik2']).strip()
        ])),
        axis=1
    )
    duplikaty_nieoczywiste = duplikaty_nieoczywiste.drop_duplicates(
        subset=['NIP', 'para_klucz'], keep='first'
    )
    duplikaty_nieoczywiste = duplikaty_nieoczywiste.drop(columns=['para_klucz'])

# === 6. Zapis do Excela w dwóch arkuszach ===
with pd.ExcelWriter(plik_wynikowy) as writer:
    if not duplikaty_oczywiste.empty:
        duplikaty_oczywiste.to_excel(writer, sheet_name='duplikaty_1_do_1', index=False)
    if not duplikaty_nieoczywiste.empty:
        duplikaty_nieoczywiste.to_excel(writer, sheet_name='duplikaty_nieoczywiste', index=False)

print("✅ Wyniki zapisano do pliku:", plik_wynikowy)
