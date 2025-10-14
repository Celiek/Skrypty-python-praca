import pandas as pd
import re
import glob
from pathlib import Path


# === 1. Wczytanie plików ELIXIR ===
def parse_elixir_file(path: Path):
    records = []
    with open(path, encoding="iso8859_2") as f:
        for line in f:
            if not line.strip().startswith("110,"):
                continue
            cols = line.strip().split(",")
            if len(cols) < 13:
                continue
            data_platnosci = cols[1]
            kwota_gr = cols[2]
            szczegoly = cols[11].strip('"')
            kontrahent = cols[8]

            # wydobądź NIP z /IDC/
            m_nip = re.search(r"/IDC/(\d{10})", szczegoly)
            nip = m_nip.group(1) if m_nip else ""

            # wydobądź kwotę VAT (niekonieczne, ale bywa przydatne)
            m_vat = re.search(r"/VAT/([\d,.-]+)", szczegoly)
            vat_txt = m_vat.group(1).replace(",", ".") if m_vat else ""

            records.append({
                "plik_elixir": path.name,
                "kontrahent": kontrahent.strip(),
                "nip": nip,
                "kwota_brutto_pln": round(float(kwota_gr) / 100, 2),
                "data_platnosci": data_platnosci,
                "vat_txt": vat_txt
            })
    return pd.DataFrame(records)


elixir_files = glob.glob("*_przelewy_w_*.txt")
elixir_dfs = [parse_elixir_file(Path(f)) for f in elixir_files]
df_elixir_all = pd.concat(elixir_dfs, ignore_index=True)
print(f"[INFO] Wczytano {len(df_elixir_all)} rekordów z {len(elixir_files)} plików ELIXIR.")

# === 2. Wczytanie plików Excel (faktur) ===
excel_files = glob.glob("*.xlsx")
excels = []
for f in excel_files:
    try:
        df = pd.read_excel(f)
        df.columns = df.columns.str.strip().str.lower()
        if not {"nip", "brutto"}.issubset(df.columns):
            continue
        df["nip"] = df["nip"].astype(str).str.replace(r"\D", "", regex=True)
        df["brutto"] = df["brutto"].astype(str).str.replace(",", ".").astype(float).round(2)
        df["plik_excel"] = Path(f).name
        excels.append(df)
    except Exception as e:
        print(f"[WARN] Nie udało się wczytać {f}: {e}")

df_all_excels = pd.concat(excels, ignore_index=True)
print(f"[INFO] Wczytano {len(df_all_excels)} rekordów z {len(excel_files)} plików Excel.")

# === 3. Grupowanie po plikach i porównanie ===
# Tworzymy listę sum po NIP i kwocie dla każdego źródła
agg_excels = (
    df_all_excels.groupby(["plik_excel", "nip"], as_index=False)
    .agg(suma_brutto=("brutto", "sum"), liczba_faktur=("brutto", "count"))
)

agg_elixir = (
    df_elixir_all.groupby(["plik_elixir", "nip"], as_index=False)
    .agg(suma_brutto=("kwota_brutto_pln", "sum"), liczba_przelewow=("kwota_brutto_pln", "count"))
)

# === 4. Szukanie dopasowań (heurystyka: zbliżona suma brutto ± 1zł) ===
matches = []
for _, e_row in agg_elixir.iterrows():
    sub = agg_excels[
        (agg_excels["nip"] == e_row["nip"])
        & (abs(agg_excels["suma_brutto"] - e_row["suma_brutto"]) <= 1.0)
        ]
    for _, x_row in sub.iterrows():
        matches.append({
            "plik_elixir": e_row["plik_elixir"],
            "plik_excel": x_row["plik_excel"],
            "nip": e_row["nip"],
            "suma_elixir": e_row["suma_brutto"],
            "suma_excel": x_row["suma_brutto"],
            "różnica": round(e_row["suma_brutto"] - x_row["suma_brutto"], 2),
            "liczba_faktur": x_row["liczba_faktur"],
            "liczba_przelewow": e_row["liczba_przelewow"]
        })

df_matches = pd.DataFrame(matches)
df_matches = df_matches.sort_values(["plik_elixir", "nip", "różnica"])

df_matches.to_excel("mapowanie_elixir_do_excel.xlsx", index=False)
print("[OK] Zapisano raport: mapowanie_elixir_do_excel.xlsx")
