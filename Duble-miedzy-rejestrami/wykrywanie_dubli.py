import pandas as pd
import os
import time

input_file = "great 08-09 (1).xlsx"

# wczytanie pliku
df = pd.read_excel(input_file, dtype=str)   # wymusza odczyt wszystkiego jako tekst
print(df.head())
print("Kolumny w pliku:", df.columns.tolist())

# usunięcie ewentualnego apostrofu z przodu
for col in ["NIP", "Numer dokumentu"]:
    df[col] = df[col].astype(str).str.lstrip("'").str.strip()
df["NIP"] = df["NIP"].str.replace(r"\D", "", regex=True)


# sprawdzanie duplikatów
dups = df[df.duplicated(subset=["NIP", "Numer dokumentu","Brutto","Netto"], keep=False)]

if dups.empty:
    print("[DUP] Brak duplikatów !")
else:
    print(f"[DUP] Znaleziono {len(dups)} duplikatów w pliku!")
    print(dups[["NIP", "Numer dokumentu"]].to_string(index=False))

    # zapis do CSV z timestampem
    ts = time.strftime("%Y%m%d_%H%M%S")
    os.makedirs("dup", exist_ok=True)
    out_path = os.path.join("dup", f"duplikaty_{ts}.csv")
    dups.to_csv(out_path, index=False, encoding="utf-8")
    print(f"[DUP] Raport zapisany: {out_path}")
