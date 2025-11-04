import pandas as pd
import os
import time

input_file = "shumee przelewy problemy.xlsx"

# === Wczytanie pliku ===
df = pd.read_excel(input_file, dtype=str)
print(df.head())
print("Kolumny w pliku:", df.columns.tolist())

# === Czyszczenie pól ===
for col in ["NIP", "Numer dokumentu"]:
    if col in df.columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.lstrip("'")
            .str.strip()
            .str.replace("\xa0", "", regex=False)
            .str.replace(r"\s+", "", regex=True)
        )

if "NIP" in df.columns:
    df["NIP"] = df["NIP"].str.replace(r"\D", "", regex=True)

# === Znajdowanie duplikatów ===
subset_cols = ["NIP", "Numer dokumentu", "Brutto", "Netto"]
subset_cols = [c for c in subset_cols if c in df.columns]

dups = df[df.duplicated(subset=subset_cols, keep=False)].copy()
uniques = df.drop_duplicates(subset=subset_cols, keep=False).copy()

# === Dodanie liczby wystąpień ===
if not dups.empty:
    dups["Liczba_wystapien"] = dups.groupby(subset_cols)[subset_cols[0]].transform("count")

# === Raporty ===
os.makedirs("dup", exist_ok=True)
ts = time.strftime("%Y%m%d_%H%M%S")

if dups.empty:
    print("[DUP] Brak duplikatów!")
else:
    print(f"[DUP] Znaleziono {len(dups)} duplikatów!")
    print(f"[DUP] Liczba unikalnych zestawów duplikatów: {dups[subset_cols].drop_duplicates().shape[0]}")

    out_csv = os.path.join("dup", f"duplikaty_{ts}.csv")
    out_xlsx = os.path.join("dup", f"duplikaty_{ts}.xlsx")
    dups.to_csv(out_csv, index=False, encoding="utf-8")
    dups.to_excel(out_xlsx, index=False)

    print(f"[DUP] Zapisano duplikaty:")
    print(f" ├─ CSV : {out_csv}")
    print(f" └─ XLSX: {out_xlsx}")

if not uniques.empty:
    out_unique_xlsx = os.path.join("dup", f"unikalne_{ts}.xlsx")
    uniques.to_excel(out_unique_xlsx, index=False)
    print(f"[OK] Zapisano unikalne rekordy: {out_unique_xlsx}")
