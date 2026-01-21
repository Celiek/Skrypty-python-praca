import pandas as pd
import os
import time
from rapidfuzz import distance

TOL_PLN = 1
THRESHOLD_FUZZY = 2  # max różnica znaków w numerze dokumentu

# ==============================
# Wczytanie pliku
# ==============================
input_file = r"Zakup test shumee folder 15.01.2026.xlsx"
df = pd.read_excel(input_file, dtype=str)

print("Kolumny w pliku:", df.columns.tolist())

# ==============================
# Czyszczenie danych
# ==============================
for col in ["NIP", "Numer dokumentu"]:
    df[col] = (
        df[col]
        .astype(str)
        .str.lstrip("'")
        .str.strip()
        .str.replace("\xa0", "", regex=False)
        .str.replace(r"\s+", "", regex=True)
    )

df["NIP"] = df["NIP"].str.replace(r"\D", "", regex=True)

# Zamiana kwot na float
for col in ["Brutto", "Netto"]:
    df[col] = (
        df[col]
        .astype(str)
        .str.replace(",", ".", regex=False)
        .str.extract(r"([0-9]+\.[0-9]+|[0-9]+)", expand=False)
        .fillna("0")
        .astype(float)
    )

# ==============================
# 1. Duplikaty dokładne 1:1
# ==============================
dups_exact = df[df.duplicated(
    subset=["NIP", "Numer dokumentu", "Brutto", "Netto"], keep=False
)]

# ==============================
# 2. Duplikaty kwotowe ±1 PLN
# ==============================
dups_amount = []

df_sorted = df.sort_values(["NIP", "Numer dokumentu"])

for idx in range(len(df_sorted) - 1):
    r1 = df_sorted.iloc[idx]
    r2 = df_sorted.iloc[idx + 1]

    if r1["NIP"] == r2["NIP"] and r1["Numer dokumentu"] == r2["Numer dokumentu"]:
        if abs(r1["Brutto"] - r2["Brutto"]) <= TOL_PLN:
            dups_amount.append(r1)
            dups_amount.append(r2)

dups_amount = pd.DataFrame(dups_amount).drop_duplicates()

# usuń rekordy już w exact
dups_amount = dups_amount[~dups_amount.index.isin(dups_exact.index)]

# ==============================
# 3. Fuzzy matching
# ==============================
dups_fuzzy = []

for idx in range(len(df_sorted) - 1):
    r1 = df_sorted.iloc[idx]
    r2 = df_sorted.iloc[idx + 1]

    if r1["NIP"] != r2["NIP"]:
        continue

    if abs(r1["Brutto"] - r2["Brutto"]) > TOL_PLN:
        continue

    dist = distance.Levenshtein.distance(
        r1["Numer dokumentu"], r2["Numer dokumentu"]
    )

    if 0 < dist <= THRESHOLD_FUZZY:
        dups_fuzzy.append(r1)
        dups_fuzzy.append(r2)

dups_fuzzy = pd.DataFrame(dups_fuzzy).drop_duplicates()

# usuń rekordy już znalezione wcześniej
dups_fuzzy = dups_fuzzy[
    ~dups_fuzzy.index.isin(dups_exact.index)
]
dups_fuzzy = dups_fuzzy[
    ~dups_fuzzy.index.isin(dups_amount.index)
]

# ==============================
# FINALNE DWA ARKUSZE
# ==============================

# Arkusz "dokładne" = exact + kwotowe
df_dokladne = pd.concat([dups_exact, dups_amount]).drop_duplicates()

print("\n[DUP] Dokładne (1:1 + różnica ±1 PLN):", len(df_dokladne))
print("[DUP] Fuzzy (numer podobny, kwota ±1 PLN):", len(dups_fuzzy))

# ==============================
# Zapis do Excela
# ==============================
os.makedirs("dup", exist_ok=True)
ts = time.strftime("%Y%m%d_%H%M%S")
out_xlsx = os.path.join("dup", f"duplikaty_{ts}.xlsx")

with pd.ExcelWriter(out_xlsx) as writer:
    df_dokladne.to_excel(writer, sheet_name="dokladne", index=False)
    dups_fuzzy.to_excel(writer, sheet_name="fuzzy", index=False)

print("\n[DUP] Raport zapisany:", out_xlsx)
