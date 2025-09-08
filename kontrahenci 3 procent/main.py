import pandas as pd
from argparse import ArgumentParser, BooleanOptionalAction
from typing import Optional
from dotenv import load_dotenv
from smtplib import SMTP
import os
import re
from datetime import datetime,timedelta

from numpy.lib.recfunctions import find_duplicates

# Program odczytuje dane z pliku xlsx i wysyłą dane do fakturowni
# potem pobiera dane z fakturowni (może)
# wysyła emaile z fakturami do listy kontrahentów z plików

####
# Konfiguracja i pomniejsze narzędzia
####


COMPANIES = {
    "shumee": {
        "name_addr": os.getenv("SHUMEE_NAME_ADDR", 'Shumee Sp. z.o.o. aleja 1 Maja 31/33 lok. 6 90-739 Łódź Nr konta: 07 1140 1108 0000 3147 1800 1007 NIP:7252140827'),
        "nrb":       os.getenv("SHUMEE_NRB",       "07114011080000314718001007"),
        "bank_code": os.getenv("SHUMEE_BANK_CODE", "11401108"),
    },
    "greatstore": {
        "name_addr": os.getenv("GREATSTORE_NAME_ADDR", 'Greatstore Sp. z.o.o. aleja 1 Maja 31/33 lok. 6 90-739 NR konta: 35 1140 1108 0000 3639 6100 1006 Łódź NIP:7252291331 '),
        "nrb":       os.getenv("GREATSTORE_NRB",       "18102055610000310200035501"),
        "bank_code": os.getenv("GREATSTORE_BANK_CODE", "10205561"),
    },
    "extrastore": {
        "name_addr": os.getenv("EXTRASTORE_NAME_ADDR", 'Extrastore Sp. z.o.o. aleja 1 Maja 31/33 lok. 6 90-739 Łódź NIP 7252302342 Nr konta: 05 1140 2004 0000 3302 8042 9939'),
        "nrb":       os.getenv("EXTRASTORE_NRB",       "05114020040000330280429939"),
        "bank_code": os.getenv("EXTRASTORE_BANK_CODE", "11402004"),  # 8 cyfr
    },
}


def get_email_db(email:str):
    return 0

def nip_digits(nip: str) -> str:
    return re.sub(r"\D", "", str(nip or ""))

def serializacja_dat(x) -> str:
    """YYYYMMDD; obsługuje datetime/Timestamp, serial Excela oraz popularne stringi."""
    if isinstance(x, (datetime, pd.Timestamp)):
        return pd.to_datetime(x).strftime("%Y%m%d")

    if isinstance(x, (int, float)) and not pd.isna(x):
        # Excel 1900-date system (z "leap bug") → origin=1899-12-30
        try:
            return pd.to_datetime(x, unit="D", origin="1899-12-30").strftime("%Y%m%d")
        except Exception:
            pass

    if isinstance(x, str):
        x = x.strip()
        for fmt in ("%d.%m.%Y", "%d/%m/%Y", "%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y"):
            try:
                return datetime.strptime(x, fmt).strftime("%Y%m%d")
            except ValueError:
                continue

    raise ValueError(f"Nieobsługiwany format daty: {x!r}")

def clean_digits(s: str) -> str:
    return re.sub(r"\D", "", str(s or ""))

def valid_nip(nip: str) -> bool:
    nip = clean_digits(nip)
    if len(nip) != 10 or not nip.isdigit():
        return False
    w = [6, 5, 7, 2, 3, 4, 5, 6, 7]
    checksum = sum(int(nip[i]) * w[i] for i in range(9)) % 11
    return checksum == int(nip[9])

def normalize_nrb(account: str) -> str:
    """Zwraca 26 cyfr NRB (lub pusty string, gdy format niepoprawny)."""
    if not account:
        return ""
    acc = re.sub(r"\s", "", str(account))
    if len(acc) == 26 and acc.isdigit():
        return acc
    if acc.upper().startswith("PL") and len(acc) == 28 and acc[2:].isdigit():
        return acc[2:]
    return ""

def bank_code_from_nrb(nrb: str) -> str:
    """8 cyfr rozliczeniowych (poz. 3-10) albo ''. """
    nrb = normalize_nrb(nrb)
    if len(nrb) >= 10:
        return nrb[2:10]
    return ""


def handle_duplicates(df: pd.DataFrame,action: str = "error") -> pd.DataFrame:
    d, full_dups = find_duplicates()

    if full_dups.empty:
        return df
    preview_cols = ["Kontrahent","Numer dokumentu","Brutto"]
    print("[DUP] Wykryto duplikaty:\n",full_dups[preview_cols].to_string(index=False))

    if action == "error":
        raise ValueError("W pliku znajdują się duplikaty (patrz log powyżej).")
    elif action == "warn":
        return df
    elif action in ("drop_keep_first","drop_keep_last"):
        keep = "first" if action == "drop_keep_first" else "last"
        mask = d.duplicated(subset=["__doc_no_norm","__brut_gr"],keep=keep)
        cleaned = df.loc[~mask].copy()
        print(f"[DUP] Usunięto {mask.sum()} zduplikowanych wierszy ({action}).")
        return cleaned
    else:
        raise ValueError(f"Nieznane actions = '{action}'")

# Główna część logiki

def czytaj_plik(file:str,spolka: str):
    klucz = spolka.str().lower()

    # konfiguracja danych spółki do generowania emaili
    if klucz not in COMPANIES:
        raise ValueError(f"Nieznana firma {spolka} popraw to")
    conf = COMPANIES[klucz]
    adres_spolki = conf["namer_addr"]


    df = pd.read_excel(file)
    # wymagane są Kontrahent,Netto, Data_wystawienia,Nr_dokumentu
    wymagane_kolumny ={"Data wystawienia","Netto","Kontrahent","Numer dokumentu"}



if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument(description="Generator faktur z fakturowni wraz z wysyłaniem ich bezpośrednio")
    parser.add_argument("input",help="Ścieżka do xlsx z danymi do faktur")
    args = parser.parse_args()
    czytaj_plik(args.input)