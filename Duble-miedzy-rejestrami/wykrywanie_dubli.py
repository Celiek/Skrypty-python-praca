import pandas as pd
import os
import time
#"shumee przelewy problemy.xlsx"
input_file = "zakup test folder 27.10 great (1).xlsx"

# wczytanie pliku
df = pd.read_excel(input_file, dtype=str)
# print(df.head())
print("Kolumny w pliku:", df.columns.tolist())

# czyszczenie pól
for col in ["NIP", "Numer dokumentu"]:
    df[col] = (
        df[col]
        .astype(str)
        .str.lstrip("'")
        .str.strip()
        .str.replace("\xa0", "", regex=False)  # usuwa niełamliwe spacje
        .str.replace(r"\s+", "", regex=True)   # usuwa wszystkie spacje/tabulatory
    )

df["NIP"] = df["NIP"].str.replace(r"\D", "", regex=True)


# sprawdzanie duplikatów
dups = df[df.duplicated(subset=["NIP", "Numer dokumentu", "Brutto", "Netto"], keep=False)]
df_clean = df.drop_duplicates(subset=["NIP", "Numer dokumentu", "Brutto", "Netto"])


if dups.empty:
    print("[DUP] Brak duplikatów!")
else:
    print(f"[DUP] Znaleziono {len(dups)} duplikatów w pliku!")
    print(dups[["NIP", "Numer dokumentu"]].to_string(index=False))

    # folder wyjściowy
    os.makedirs("dup", exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")

    # zapis do CSV
    out_csv = os.path.join("dup", f"duplikaty_{ts}.csv")
    dups.to_csv(out_csv, index=False, encoding="utf-8")

    # zapis do XLSX
    out_xlsx = os.path.join("dup", f"duplikaty_{ts}.xlsx")
    dups.to_excel(out_xlsx, index=False)

    #czysty plik 
    df_clean.to_excel("czysty_plik_shumee.xlsx",index=False)

    print(f"[DUP] Raporty zapisane:")
    print(f" ├─ CSV : {out_csv}")
    print(f" └─ XLSX: {out_xlsx}")
