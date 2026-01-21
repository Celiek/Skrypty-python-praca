import pandas as pd
import re

# Metoda heurustyczna i twardo zakodowana
# KONFIGURACJA
# =============================

INPUT_XLSX  = r"sm test 15.01.xlsx"
OUTPUT_XLSX = "duplikaty extra rejestr zakup test1.xlsx"

# =============================
# FUNKCJE POMOCNICZE
# =============================

def numer_signature(x: str) -> str:
    return "|".join(re.findall(r"\d+", str(x)))

def split_long_number(sig: str):
    if "|" in sig or not sig.isdigit() or len(sig) < 8:
        return None

    year = sig[-4:]
    month = sig[-6:-4]
    number = sig[:-6]

    if not (month.isdigit() and 1 <= int(month) <= 12):
        return None

    return f"{number}|{month}|{year}"

def heuristic_sig(sig: str) -> str:
    split = split_long_number(sig)
    return split if split else sig

# =============================
# WCZYTANIE
# =============================

df = pd.read_excel(INPUT_XLSX)

# =============================
# NORMALIZACJA
# =============================

df["Data wystawienia"] = pd.to_datetime(df["Data wystawienia"], errors="raise")

for col in ["Netto", "VAT", "Brutto"]:
    df[col] = pd.to_numeric(df[col], errors="raise").round(2)

df["NIP"] = df["NIP"].astype(str).str.replace(r"\D", "", regex=True)

# =============================
# SYGNATURY NUMERU
# =============================

df["NUM_SIG"]   = df["Numer dokumentu"].map(numer_signature)
df["NUM_SIG_H"] = df["NUM_SIG"].map(heuristic_sig)

# =============================
# KLUCZE DUPLIKATU
# =============================

df["_DUP_KEY"] = (
    df["NIP"] + "|" +
    df["Data wystawienia"].dt.strftime("%Y-%m-%d") + "|" +
    df["Netto"].astype(str) + "|" +
    df["VAT"].astype(str) + "|" +
    df["Brutto"].astype(str) + "|" +
    df["NUM_SIG"]
)

df["_DUP_KEY_H"] = (
    df["NIP"] + "|" +
    df["Data wystawienia"].dt.strftime("%Y-%m-%d") + "|" +
    df["Netto"].astype(str) + "|" +
    df["VAT"].astype(str) + "|" +
    df["Brutto"].astype(str) + "|" +
    df["NUM_SIG_H"]
)

# =============================
# DUPLIKATY TWARDĘ
# =============================

hard_mask = df.duplicated("_DUP_KEY", keep=False)
dups = df[hard_mask].copy()

dups["LICZBA_WYSTAPIEN"] = (
    dups.groupby("_DUP_KEY")["_DUP_KEY"].transform("count")
)
dups["TYP_DUPLIKATU"] = "PEWNY"

hard_keys = set(dups["_DUP_KEY"])

# =============================
# DUPLIKATY HEURYSTYCZNE
# =============================

heur_mask = df.duplicated("_DUP_KEY_H", keep=False)
heur = df[heur_mask].copy()

heur = heur[~heur["_DUP_KEY"].isin(hard_keys)]
heur["TYP_DUPLIKATU"] = "HEURYSTYCZNY"

# =============================
# ZAPIS
# =============================

with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
    dups.drop(columns=["_DUP_KEY", "_DUP_KEY_H"], errors="ignore") \
        .sort_values(["NIP", "Data wystawienia", "Brutto"]) \
        .to_excel(writer, index=False, sheet_name="DUPLIKATY_PEWNE")

    heur.drop(columns=["_DUP_KEY", "_DUP_KEY_H"], errors="ignore") \
        .sort_values(["NIP", "Data wystawienia", "Brutto"]) \
        .to_excel(writer, index=False, sheet_name="DUPLIKATY_HEURYSTYCZNE")

print(f"✔ Gotowe. Twarde: {len(dups)} | Heurystyczne: {len(heur)}")
