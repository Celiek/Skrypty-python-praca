import pandas as pd
from rapidfuzz import distance

# TODO
# sprawdzać po kombinacjach kolumn nie po pojedyńczych

# === KONFIGURACJA ===
plik1 = "rejestrtestgreatstore.xlsx"
plik2 = "zakupkrajgreatstore.xlsx"
plik_wynikowy = "duplikaty_po_nip_i_kwotach_07.11.2025.xlsx"

THRESHOLD_DIST = 4   # maksymalna różnica znaków w nazwie dokumentu

# === 1. Wczytanie danych ===
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

# === 3. ETAP 1 — identyczne rekordy 1:1 ===
mask = (
    (df1['NIP'].isin(df2['NIP'])) &
    (df1['Netto'].isin(df2['Netto'])) &
    (df1['Brutto'].isin(df2['Brutto'])) &
    (df1['VAT'].isin(df2['VAT'])) &
    (df1['Numer dokumentu'].isin(df2['Numer dokumentu']))
)

duplikaty_oczywiste = df1[mask].merge(
    df2,
    on=['NIP', 'Netto', 'Brutto', 'VAT', 'Numer dokumentu'],
    how='inner',
    suffixes=('_plik1', '_plik2')
)

# Usuń te rekordy z dalszego porównywania
if not duplikaty_oczywiste.empty:
    print(f"✅ Znaleziono {len(duplikaty_oczywiste)} oczywistych duplikatów 1:1.")
    df1 = df1[~mask]
    # Usuń z df2 również rekordy, które już wystąpiły
    df2 = df2[~df2['Numer dokumentu'].isin(duplikaty_oczywiste['Numer dokumentu'])]

else:
    print("ℹ️ Brak oczywistych duplikatów 1:1.")

# === 4. ETAP 2 — nieoczywiste duplikaty (fuzzy matching) ===
wyniki = []

for i, r1 in df1.iterrows():
    kandydaci = df2[
        (df2['NIP'] == r1['NIP']) &
        (df2['Netto'] == r1['Netto']) &
        (df2['Brutto'] == r1['Brutto']) &
        (df2['VAT'] == r1['VAT'])
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
                "Netto": r1['Netto'],
                "Brutto": r1['Brutto'],
                "VAT": r1['VAT']
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

    duplikaty_nieoczywiste = duplikaty_nieoczywiste.drop_duplicates(subset=['NIP', 'para_klucz'], keep='first')
    duplikaty_nieoczywiste = duplikaty_nieoczywiste.drop(columns=['para_klucz'])

# === 6. Zapis do Excela w dwóch arkuszach ===
with pd.ExcelWriter(plik_wynikowy) as writer:
    if not duplikaty_oczywiste.empty:
        duplikaty_oczywiste.to_excel(writer, sheet_name='duplikaty_1_do_1', index=False)
    if not duplikaty_nieoczywiste.empty:
        duplikaty_nieoczywiste.to_excel(writer, sheet_name='duplikaty_nieoczywiste', index=False)

print("✅ Wyniki zapisano do pliku:", plik_wynikowy)
