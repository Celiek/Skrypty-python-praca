import pandas as pd
from pathlib import Path

FOLDER = r"C:\Users\DELL\Documents\raporty_sm"   # <- tutaj folder z wieloma raport_XXXXX.xlsx
BAZA = r"C:\Users\DELL\Documents\Skrypty\Skrypty-python-praca\kontrahenci 3 procent\Lista Merchantow.xlsx"     # <- plik z kolumną NIP
OUTPUT = "wynik.xlsx"

def extract_nip(filename: str) -> str | None:
    """Wyciąga NIP z pliku typu raport_5273012424.xlsx"""
    name = filename.split('.')[0]
    if name.startswith("raport_"):
        nip = name.replace("raport_", "")
        if nip.isdigit() and len(nip) == 10:
            return nip
    return None

# === 1. Wczytaj bazę NIP-ów ===
df_baza = pd.read_excel(BAZA, dtype=str)
df_baza["NIP"] = (
    df_baza["NIP"]
    .astype(str)
    .str.replace(r"\D", "", regex=True)
    .str.strip()
)

baza_nipy = set(df_baza["NIP"].dropna())

# === 2. Przetwórz wszystkie pliki w folderze ===
results = []

for path in Path(FOLDER).glob("raport_*.xlsx"):
    nip = extract_nip(path.name)

    if nip:
        exists = nip in baza_nipy
        results.append({
            "plik": path.name,
            "NIP": nip,
            "Występuje w bazie": "TAK" if exists else "NIE"
        })
    else:
        results.append({
            "plik": path.name,
            "NIP": "",
            "Występuje w bazie": "BŁĘDNY FORMAT NAZWY"
        })

# === 3. Zapisz wynik do Excela ===
df = pd.DataFrame(results)
df.to_excel(OUTPUT, index=False)

print(f"✅ Gotowe! Zapisano do: {OUTPUT}")
