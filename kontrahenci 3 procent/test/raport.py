import os
import re
import pandas as pd

# ==================================================
# KONFIGURACJA
# ==================================================
FOLDER_B = r"C:\Users\DELL\Documents\Skrypty\Skrypty-python-praca\kontrahenci 3 procent\raporty_xlsx\shumee\2025-12-10"
FOLDER_A = r"C:\Users\DELL\Documents\Skrypty\Skrypty-python-praca\kontrahenci 3 procent\raporty_xlsx\shumee\2025-12-15"

EXCEL_PROWIZJE = r"C:\Users\DELL\Documents\Skrypty\Skrypty-python-praca\kontrahenci 3 procent\Lista merchantów.xlsx"
OUTPUT_XLSX = "raport_zbiorczy_shumee.xlsx"

COL_NIP = "NIP"
COL_PROWIZJA_OD = "Od kiedy prowizja 3%"

COLUMN_I_INDEX = 8   # kolumna I

# ==================================================
# FUNKCJE WSPÓLNE
# ==================================================

def extract_nip_from_filename(fname: str) -> str | None:
    name = fname.rsplit(".", 1)[0]
    m = re.search(r"raport_(\d{10})", name)
    return m.group(1) if m else None


def clean_nip(val) -> str | None:
    if pd.isna(val):
        return None
    nip = re.sub(r"\D", "", str(val))
    return nip if len(nip) == 10 else None


def extract_last_value_from_col_I(path: str):
    try:
        df = pd.read_excel(path, header=None)
        if df.shape[1] <= COLUMN_I_INDEX:
            return None
        col = df.iloc[:, COLUMN_I_INDEX].dropna()
        return col.iloc[-1] if not col.empty else None
    except Exception:
        return None


def collect_folder_data(folder: str, nip_set_other: set[str], nip_to_prowizja: dict):
    rows = []

    for fname in sorted(os.listdir(folder)):
        if not fname.lower().endswith(".xlsx"):
            continue

        path = os.path.join(folder, fname)
        nip = extract_nip_from_filename(fname)
        suma = extract_last_value_from_col_I(path)

        prowizja_od = nip_to_prowizja.get(nip) if nip else None

        if not nip:
            status_excel = "BRAK NIP W NAZWIE"
        elif nip not in nip_to_prowizja:
            status_excel = "NIP NIE W EXCELU"
        elif not prowizja_od:
            status_excel = "BRAK DATY 3%"
        else:
            status_excel = "OK"

        rows.append({
            "plik": fname,
            "nip": nip,
            "suma_prowizji": suma,
            "prowizja_3_od": prowizja_od,
            "status_w_excelu": status_excel,
            "wystepuje_w_drugim_folderze": "TAK" if nip in nip_set_other else "NIE"
        })

    return pd.DataFrame(rows)


# ==================================================
# MAIN
# ==================================================

# --- Excel z prowizją 3%
df_prow = pd.read_excel(EXCEL_PROWIZJE, dtype=str)
df_prow["NIP_CLEAN"] = df_prow[COL_NIP].apply(clean_nip)
nip_to_prowizja = (
    df_prow
    .set_index("NIP_CLEAN")[COL_PROWIZJA_OD]
    .dropna()
    .to_dict()
)

# --- NIP-y w folderach
def collect_nips(folder):
    return {
        extract_nip_from_filename(f)
        for f in os.listdir(folder)
        if f.lower().endswith(".xlsx") and extract_nip_from_filename(f)
    }

nips_A = collect_nips(FOLDER_A)
nips_B = collect_nips(FOLDER_B)

# --- Dane do arkuszy
df_A = collect_folder_data(FOLDER_A, nips_B, nip_to_prowizja)
df_B = collect_folder_data(FOLDER_B, nips_A, nip_to_prowizja)

# --- ZAPIS DO JEDNEGO EXCELA
with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
    df_A.to_excel(writer, sheet_name="Folder_A", index=False)
    df_B.to_excel(writer, sheet_name="Folder_B", index=False)

print(f"✔ Utworzono plik: {OUTPUT_XLSX}")
