import os
import re
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from argparse import ArgumentParser, BooleanOptionalAction
from typing import Optional
import unicodedata
import pandas as pd

from dotenv import load_dotenv

# =========================
# Konfiguracja
# =========================

load_dotenv()

COMPANIES = {
    "shumee": {
        "name_addr": os.getenv("SHUMEE_NAME_ADDR", 'Shumee Sp. z.o.o.| aleja 1 Maja 31/33 lok. 6| 90-739 Łódź'),
        "nrb":       os.getenv("SHUMEE_NRB",       "07114011080000314718001007"),
        "bank_code": os.getenv("SHUMEE_BANK_CODE", "11401108"),
    },
    "greatstore": {
        "name_addr": os.getenv("GREATSTORE_NAME_ADDR", 'Greatstore Sp. z.o.o.| aleja 1 Maja 31/33 lok. 6| 90-739 Łódź'),
        "nrb":       os.getenv("GREATSTORE_NRB",       "18102055610000310200035501"),
        "bank_code": os.getenv("GREATSTORE_BANK_CODE", "10205561"),
    },
    "extrastore": {
        "name_addr": os.getenv("EXTRASTORE_NAME_ADDR", 'Extrastore Sp. z.o.o.| aleja 1 Maja 31/33 lok. 6| 90-739 Łódź'),
        "nrb":       os.getenv("EXTRASTORE_NRB",       "05114020040000330280429939"),
        "bank_code": os.getenv("EXTRASTORE_BANK_CODE", "11402004"),
    },
}

OUTPUT_DIR = os.getenv("OUTPUT_DIR", ".")
os.makedirs(OUTPUT_DIR, exist_ok=True)

OUTPUT_ENCODING = os.getenv("OUTPUT_ENCODING", "iso8859_2").lower()

# =========================
# Walidacja danych
# =========================

def validate_df(df: pd.DataFrame,
                date_col="Data wpływu",
                netto_col="Netto",
                vat_col="VAT",
                brutto_col="Brutto",
                tol: float = 0.01,
                on_error: str = "skip") -> tuple[pd.DataFrame, list[dict]]:
    required = {date_col, netto_col, vat_col, brutto_col}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Brak kolumn: {', '.join(sorted(missing))}")

    d = df.copy()
    error_log: list[dict] = []

    def _to_num_cell(x):
        if pd.isna(x):
            return pd.NA
        s = str(x).strip()
        s = (s.replace("\u00A0", "")
               .replace("\u202F", "")
               .replace(" ", "")
               .replace("−", "-")
               .replace("–", "-")
               .replace("—", "-"))
        if s.startswith("(") and s.endswith(")"):
            s = "-" + s[1:-1]
        if s.endswith("-") and s.count("-") == 1:
            s = "-" + s[:-1]
        if "," in s and "." in s:
            if s.rfind(",") > s.rfind("."):
                s = s.replace(".", "").replace(",", ".")
            else:
                s = s.replace(",", "")
        elif "," in s:
            s = s.replace(",", ".")
        try:
            return float(s)
        except Exception:
            return pd.NA

    d["_netto_num"]  = d[netto_col].apply(_to_num_cell)
    d["_vat_num"]    = d[vat_col].apply(_to_num_cell)
    d["_brutto_num"] = d[brutto_col].apply(_to_num_cell)

    diff = (d["_brutto_num"] - (d["_netto_num"] + d["_vat_num"])).abs()
    mask_sum_mismatch = (diff > tol).fillna(False)
    for idx in d.index[mask_sum_mismatch]:
        error_log.append({
            "type": "sum_mismatch",
            "row": int(idx),
            "netto": d.loc[idx, netto_col],
            "vat": d.loc[idx, vat_col],
            "brutto": d.loc[idx, brutto_col],
            "diff": float(diff.loc[idx])
        })

    def _safe_date(val):
        try:
            return serializacja_dat(val)
        except Exception:
            return None
    d["__data_str"] = d[date_col].map(_safe_date)

    any_error = (
        d["_netto_num"].isna() |
        d["_vat_num"].isna() |
        d["_brutto_num"].isna() |
        mask_sum_mismatch |
        d["__data_str"].isna()
    )

    if on_error == "skip":
        d = d.loc[~any_error].copy()
    elif on_error == "raise" and any_error.any():
        raise ValueError(f"Wykryto błędy walidacji w {int(any_error.sum())} wierszach.")

    d[netto_col]  = d["_netto_num"].astype(float)
    d[vat_col]    = d["_vat_num"].astype(float)
    d[brutto_col] = d["_brutto_num"].astype(float)

    d.drop(columns=[c for c in d.columns if c.startswith("_") and c != "__data_str"],
           inplace=True, errors="ignore")
    return d, error_log

# =========================
# Utils
# =========================

def nip_digits(nip: str) -> str:
    return re.sub(r"\D", "", str(nip or ""))

def trim_to(s: str, max_len: int) -> str:
    s = s or ""
    return s[:max_len]

def money_to_grosze(value) -> int:
    if pd.isna(value):
        return 0
    d = Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return int((d * 100).to_integral_value())

def serializacja_dat(x) -> str:
    if isinstance(x, (datetime, pd.Timestamp)):
        return pd.to_datetime(x).strftime("%Y%m%d")
    if isinstance(x, (int, float)) and not pd.isna(x):
        return pd.to_datetime(x, unit="D", origin="1899-12-30").strftime("%Y%m%d")
    if isinstance(x, str):
        x = x.strip()
        for fmt in ("%d.%m.%Y", "%d/%m/%Y", "%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y"):
            try:
                return datetime.strptime(x, fmt).strftime("%Y%m%d")
            except ValueError:
                continue
    raise ValueError(f"Nieobsługiwany format daty: {x!r}")

def sanitize_text(text: str) -> str:
    if text is None:
        return ""
    text = unicodedata.normalize("NFKC", str(text))
    bad = '*;!+?#'
    cleaned = "".join(c for c in str(text) if c not in bad)
    return " ".join(cleaned.split())

def add_days_to_date_str(date_str: str, days: int) -> str:
    dt = datetime.strptime(date_str, "%Y%m%d")
    return (dt + timedelta(days=days)).strftime("%Y%m%d")

def bank_code_from_nrb(nrb: str) -> str:
    nrb = re.sub(r"\D", "", str(nrb))
    return nrb[2:10] if len(nrb) >= 10 else ""

# =========================
# Budowa rekordu ELIXIR
# =========================

def build_payment_record(data_platnosci, kwota_brutto_gr, nr_rozliczeniowy_zleceniodawcy,
                         tryb_realizacji, rachunek_zleceniodawcy, rachunek_kontrahenta,
                         nazwa_i_adres_zleceniodawcy, nazwa_i_adres_kontrahenta,
                         nr_rozliczeniowy_banku_kontrahenta, szczegoly_platnosci,
                         klasyfikacja, informacja_klient_bank) -> str:
    fields = [
        "210",
        data_platnosci,
        str(kwota_brutto_gr),
        nr_rozliczeniowy_zleceniodawcy,
        tryb_realizacji,
        rachunek_zleceniodawcy,
        rachunek_kontrahenta,
        f'"{sanitize_text(nazwa_i_adres_zleceniodawcy)}"',
        f'"{sanitize_text(nazwa_i_adres_kontrahenta)}"',
        "0",
        nr_rozliczeniowy_banku_kontrahenta,
        f'"{sanitize_text(szczegoly_platnosci)}"',
        "",
        "",
        klasyfikacja,
        f'"{trim_to(sanitize_text(informacja_klient_bank), 19)}"',
    ]
    return ",".join(fields)

