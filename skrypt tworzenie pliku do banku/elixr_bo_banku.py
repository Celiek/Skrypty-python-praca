import hashlib
import io
import logging
import os
import random
import re
import shutil
import sys
import time
from argparse import ArgumentParser, BooleanOptionalAction
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime
from datetime import timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Optional, Dict, List

import pandas as pd
import psycopg2
import py7zr
import requests
import unicodedata
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By

import json

# TODO
# Dodać nie branie pod uwagę faktur testowych - Status PREMERCHANT  DONE
# Zmiana nazwy plkiku na kolumne 17 elixir DONE
# ujemne pozycje nie mogą być uwzględnianie w agregacji faktur DONE
# zmienić agregację przelewów po nipie i dacie DONE
# Dodać podział na dni na pliki DONE
# Sprawdzać datę wystawienia faktury, jeśli jest ona późniejsza niż dzisiejsza data - przelew ma wyjść
# tego samego dnia DONE

# TODO 2:
# zmienić zapis przelewów do podfolderów DONE
# dodać kod sprawdzający czy kontrahent jest na białęj liscie DONE
# dodać generowanie raportów ( do wysyłki emailem do kontrahenta) 1/2 DONE
# ogarnąć formatownaie daty jako nr dokumentu DONE

# TODO 3:
# zmienić sposób generownaia pliku tj zamienić nazwę kontrahenta z pliku na nazwę z bazy danych DONE


# TODO 4:
# Dla Spółki Action dodać oddzielne nry kont bankowych

#######################
# INSTRUKCJA OBSLUGI CLI
#######################
# Shumee (auto nazwa wyjścia):
# py elixir_do_banku.py ".\plik_testowy.xlsx" -c shumee
#
# Greatstore (własna ścieżka + blokada duplikatów):
# py elixir_do_banku.py ".\plik_testowy.xlsx" -c greatstore -o ".\export\greatstore_elixir.txt" --dup error
#
# Extrastore (widoczna przeglądarka – debug scrapera):
# py elixir_do_banku.py ".\plik_testowy.xlsx" -c extrastore --no-headless

# =========================
# Konfiguracja i narzędzia
# =========================

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

COMPANIES = {
    "shumee": {
        "name_addr": os.getenv("SHUMEE_NAME_ADDR", 'Supermerchant Sp. z.o.o.| aleja 1 Maja 31/33 lok. 6| 90-739 Łódź'),
        "nrb":       os.getenv("SHUMEE_NRB",       "07114011080000314718001007"),
        "bank_code": os.getenv("SHUMEE_BANK_CODE", "11401108"),
        "forbidden_name": ['MORELE.NET sp. z o.o','GLOBAL INCOME SPÓŁKA Z OGRANICZONĄ ODPOWIEDZIALNOŚCIĄ','MORELE.NET SPÓŁKA Z OGRANICZONĄ ODPOWIEDZIALNOŚCIĄ'],
        "forbidden_nip": [9451972201,5862167315]
    },
    "greatstore": {
        "name_addr": os.getenv("GREATSTORE_NAME_ADDR", 'Greatstore Sp. z.o.o.| aleja 1 Maja 31/33 lok. 6| 90-739 Łódź'),
        "nrb":       os.getenv("GREATSTORE_NRB",       "35114011080000363961001006"),
        "bank_code": os.getenv("GREATSTORE_BANK_CODE", "11401108"),
        "forbidden_name": ['MORELE.NET SPÓŁKA Z OGRANICZONĄ ODPOWIEDZIALNOŚCIĄ','MORELE.NET sp. z o.o'],
        "forbidden_nip": [9451972201,5862167315]
    },
    "extrastore": {
        "name_addr": os.getenv("EXTRASTORE_NAME_ADDR", 'Extrastore Sp. z.o.o.| aleja 1 Maja 31/33 lok. 6| 90-739 Łódź'),
        "nrb":       os.getenv("EXTRASTORE_NRB",       "05114020040000330280429939"),
        "bank_code": os.getenv("EXTRASTORE_BANK_CODE", "11402004"),  # 8 cyfr
        "forbidden_name": ['MORELE.NET SPÓŁKA Z OGRANICZONĄ ODPOWIEDZIALNOŚCIĄ','MORELE.NET sp. z o.o'],
        "forbidden_nip": [9451972201,5862167315]
    },
}

CHROMEDRIVER_PATH = os.getenv("CHROMEDRIVER_PATH", r"C:\tools\chromedriver-win64\chromedriver.exe")

OUTPUT_DIR = os.getenv("OUTPUT_DIR", ".")
os.makedirs(OUTPUT_DIR, exist_ok=True)

_WINDOWS_FORBIDDEN = set('<>:"/\\|?*')
_WINDOWS_RESERVED  = {
    "CON","PRN","AUX","NUL",
    *(f"COM{i}" for i in range(1,10)),
    *(f"LPT{i}" for i in range(1,10)),
}


forbidden_kontrahenci = pd.DataFrame({
    'MORELE.NET sp. z o.o','Global Income sp. z o.o.'
})

# Domyślnie ISO-8859-2 (lub nadpisz w .env)
OUTPUT_ENCODING = os.getenv("OUTPUT_ENCODING", "iso8859_2").lower()

# =========================
# Normalizacja / kodowanie
# =========================

def build_json_przelew_company_nip(df: pd.DataFrame,
                                   agg: pd.DataFrame,
                                   company_key: str) -> dict:
    """
    JSON:
      meta: company, generated_at, source_columns
      groups: 1 wpis = 1 (NIP/nazwa) x 1 data_wystawienia,
              sumy + lista pozycji (faktur) z tego dnia
    Zakłada, że:
      df ma: __grp_key, __data_str
      agg ma: __grp_key, __data_str, data_platnosci, nip_clean, kontrahent,
              suma_netto, suma_vat, suma_brutto, suma_*_gr
    """
    payload = {
        "meta": {
            "company": company_key,
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "source_columns": list(df.columns),
        },
        "groups": []
    }

    cols_needed = ["__grp_key", "__data_str", "Numer dokumentu", "Data wystawienia",
                   "Netto", "VAT", "Brutto", "NIP", "Kontrahent"]
    missing = [c for c in cols_needed if c not in df.columns]
    if missing:
        raise ValueError(f"Brak kolumn w df: {missing}")

    df_idx = df[cols_needed].copy()

    def _yyyymmdd_to_iso(d: str) -> str:
        d = str(d)
        return f"{d[:4]}-{d[4:6]}-{d[6:8]}"

    for _, row in agg.iterrows():
        grp_key  = str(row["__grp_key"])
        data_str = str(row["__data_str"])  # YYYYMMDD
        iso_date = _yyyymmdd_to_iso(data_str)

        sub = df_idx[(df_idx["__grp_key"] == grp_key) & (df_idx["__data_str"] == data_str)].copy()

        items = []
        for _, r in sub.iterrows():
            items.append({
                "numer_dokumentu": str(r["Numer dokumentu"]),
                "data_wystawienia": "" if pd.isna(r["Data wystawienia"]) else str(r["Data wystawienia"]),
                "netto": float(r["Netto"]),
                "vat": float(r["VAT"]),
                "brutto": float(r["Brutto"]),
                "nip": "" if pd.isna(r.get("NIP")) else str(r.get("NIP")),
                "kontrahent": "" if pd.isna(r.get("Kontrahent")) else str(r.get("Kontrahent")),
            })

        group_entry = {
            "group_id": f"{company_key}:{(row.get('nip_clean') or 'NAME')}:{data_str}",
            "nip": "" if pd.isna(row.get("nip_clean")) else str(row.get("nip_clean")),
            "kontrahent": "" if pd.isna(row.get("kontrahent")) else str(row.get("kontrahent")),
            "data_wystawienia": iso_date,
            "data_platnosci": _yyyymmdd_to_iso(str(row.get("data_platnosci"))),
            "suma": {
                "netto": float(row.get("suma_netto") or 0.0),
                "vat": float(row.get("suma_vat") or 0.0),
                "brutto": float(row.get("suma_brutto") or 0.0),
                "netto_gr": int(row.get("suma_netto_gr") or 0),
                "vat_gr": int(row.get("suma_vat_gr") or 0),
                "brutto_gr": int(row.get("suma_brutto_gr") or 0),
            },
            "pozycje": items
        }
        payload["groups"].append(group_entry)

    return payload

def save_grouped_json(df: pd.DataFrame,
                      agg: pd.DataFrame,
                      company_key: str,
                      base_dir: str = "json") -> str:
    """
    Zapis do: json/<firma>/<YYYY-MM-DD>/<firma>_przelewy_<YYYY-MM-DD>.json
    Zwraca pełną ścieżkę.
    """
    payload = build_json_przelew_company_nip(df, agg, company_key)
    out_date = datetime.now().strftime("%Y-%m-%d")
    out_dir = Path(base_dir) / company_key / out_date
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"{company_key}_przelewy_{out_date}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return str(out_path)
# --- WALIDACJA DANYCH ---


def validate_df(
    df: pd.DataFrame,
    *,
    date_col: str = "Data wpływu",
    netto_col: str = "Netto",
    vat_col: str = "VAT",
    brutto_col: str = "Brutto",
    tol: float = 0.01,
    on_error: str = "skip",   # 'skip' | 'keep' | 'raise'
) -> tuple[pd.DataFrame, list[dict]]:
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

    # --- log nie-liczb ---
    for col_name, num_col, tag in [
        (netto_col,  "_netto_num",  "bad_number_netto"),
        (vat_col,    "_vat_num",    "bad_number_vat"),
        (brutto_col, "_brutto_num", "bad_number_brutto"),
    ]:
        mask = d[num_col].isna()
        for idx in d.index[mask]:
            error_log.append({
                "type": tag,
                "row": int(idx),
                "doc": str(d.loc[idx].get("Numer dokumentu", "")),
                "value": d.loc[idx, col_name],
                "msg": f"{col_name}: nie-liczbowe/NaN"
            })

    # --- ujemne wartości / równe zeru -> usuwanie z pliku ---
    for num_col, orig_col, tag in [
        ("_netto_num",  netto_col,  "negative_netto"),
        ("_vat_num",    vat_col,    "negative_vat"),
        ("_brutto_num", brutto_col, "negative_brutto"),
    ]:
        mask = (d[num_col] <= 0).fillna(False)
        for idx in d.index[mask]:
            error_log.append({
                "type": tag,
                "row": int(idx),
                "doc": str(d.loc[idx].get("Numer dokumentu", "")),
                "value": d.loc[idx, orig_col],
                "msg": f"{orig_col}: wartość ujemna (korekta)"
            })

    # --- spójność kwot ---
    diff = (d["_brutto_num"] - (d["_netto_num"] + d["_vat_num"])).abs()
    mask_sum_mismatch = (diff > tol).fillna(False)
    for idx in d.index[mask_sum_mismatch]:
        error_log.append({
            "type": "sum_mismatch",
            "row": int(idx),
            "doc": str(d.loc[idx].get("Numer dokumentu", "")),
            "netto": d.loc[idx, netto_col],
            "vat": d.loc[idx, vat_col],
            "brutto": d.loc[idx, brutto_col],
            "diff": float(diff.loc[idx]),
            "msg": f"Niespójność sumy > {tol}"
        })

    # --- data -> __data_str ---
    def _safe_date(val):
        try:
            return serializacja_dat(val)
        except Exception:
            return None

    d["__data_str"] = d[date_col].map(_safe_date)
    mask_bad_date = d["__data_str"].isna()
    for idx in d.index[mask_bad_date]:
        error_log.append({
            "type": "bad_date",
            "row": int(idx),
            "doc": str(d.loc[idx].get("Numer dokumentu", "")),
            "value": d.loc[idx, date_col],
            "msg": "Błąd serializacji daty"
        })


    any_error = (
        d["_netto_num"].isna()  |
        d["_vat_num"].isna()    |
        d["_brutto_num"].isna() |
        mask_sum_mismatch       |
        mask_bad_date
    )

    if on_error == "skip":
        d = d.loc[~any_error].copy()
    elif on_error == "raise":
        if any_error.any():
            raise ValueError(f"Wykryto błędy walidacji w {int(any_error.sum())} wierszach.")
    elif on_error != "keep":
        raise ValueError("validate_df.on_error ∈ {'skip','keep','raise'}")

    # nadpisz kolumny na floaty
    d[netto_col]  = d["_netto_num"].astype(float)
    d[vat_col]    = d["_vat_num"].astype(float)
    d[brutto_col] = d["_brutto_num"].astype(float)

    d.drop(columns=[c for c in d.columns if c.startswith("_") and c != "__data_str"],
           inplace=True, errors="ignore")
    return d, error_log


def sanitize_text(text: str) -> str:
    """Tekst bez przecinków/cudzysłowów i śmieci – bezpieczny do ELIXIR-0 (max 32 znaki)."""
    if text is None:
        return ""
    t = _elixir_safe_text(text)
    bad = ',*"\'\r\n\t;!+?#'   # niedozwolone znaki
    t = "".join(c for c in str(t) if c not in bad)
    t = " ".join(t.split())
    t = re.sub(r'\s*\|\s*', '|', t).strip('| ')
    # stałe ograniczenie długości
    return t[:32]

# ===========================================
# Utils
# ===========================================

def convert_dates_to_strings(df, column_name):
    """Zamienia tylko wartości typu datetime na stringi w formacie DD/MM/YYYY"""
    if column_name not in df.columns:
        raise ValueError(f"Brak kolumny '{column_name}' w DataFrame.")

    def _convert(val):
        # jeśli to obiekt datetime lub Timestamp — konwertuj
        if isinstance(val, (pd.Timestamp, datetime)):
            return val.strftime("%d/%m/%Y")
        return val  # resztę zostaw bez zmian

    df[column_name] = df[column_name].apply(_convert)
    return df

def _slugify_filename(s: str, *, max_len: int = 60) -> str:
    """
    Tworzy bezpieczną nazwę pliku dla Windows/macOS/Linux:
    - podmienia niedozwolone znaki (w tym cudzysłów i apostrof) na '_',
    - zwija wielokrotne '_' w jedno,
    - usuwa kropki/spacje z końca,
    - unika nazw zarezerwowanych (CON/PRN/AUX/NUL/COM1../LPT1..).
    """
    if not s:
        return "plik"

    # normalizacja + usunięcie diakrytyków
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.strip()

    # wymiana wszystkich niedozwolonych na '_'
    s = "".join(("_" if ch in _WINDOWS_FORBIDDEN or ch in {"'", "`"} else ch) for ch in s)

    # przepuszczamy tylko [A-Za-z0-9_. -], resztę na ' '
    s = re.sub(r"[^A-Za-z0-9_. \-]", "_", s)

    # zwijanie wielokrotnych podkreślników/spacji
    s = re.sub(r"[ _]+", " ", s)
    s = s.replace(" ", "_")
    s = re.sub(r"_+", "_", s)

    # usunięcie kropek/spacji/podkreślników z początku/końca
    s = s.strip(" ._")

    # pusta po czyszczeniu?
    if not s:
        s = "plik"

    base_upper = s.upper()
    if base_upper in _WINDOWS_RESERVED:
        s = f"_{s}"

    # ograniczenie długości
    s = s[:max_len].rstrip(" ._")

    # jeszcze raz awaryjnie
    if not s:
        s = "plik"

    return s

def export_grouped_excels(df: pd.DataFrame, out_dir: str, nazwa_spolki: str) -> dict[str, str]:
    data_folder = datetime.now().strftime("%d-%m-%Y")
    base_path = Path(out_dir) / nazwa_spolki / data_folder
    base_path.mkdir(parents=True, exist_ok=True)

    Path(out_dir).mkdir(parents=True, exist_ok=True)
    wanted = ["Numer dokumentu", "Data wystawienia", "Netto", "VAT", "Brutto"]

    if "Data wystawienia" in df.columns:
        try:
            df["Data wystawienia"] = pd.to_datetime(df["Data wystawienia"], errors="coerce", dayfirst=True)
            df["Data wystawienia"] = df["Data wystawienia"].dt.strftime("%d.%m.%Y")
        except Exception as e:
            print(f"[WARN] Problem z konwersją 'Data wystawienia': {e}")

    if "Numer dokumentu" in df.columns:
        df = convert_dates_to_strings(df, "Numer dokumentu")

    cols = [c for c in wanted if c in df.columns]
    if not cols:
        raise ValueError("Brak kolumn do eksportu raportów - sprawdź nazwy w dataframe.")

    out_map: dict[str, str] = {}
    g = df.groupby("NIP", dropna=False, as_index=False)

    for nip, sub in g:
        nip_str = str(nip).strip()
        kontrahent = ""
        if "Kontrahent" in sub.columns and not sub["Kontrahent"].isna().all():
            kontrahent = str(sub["Kontrahent"].iloc[0] or "")

        fname = f"{_slugify_filename(kontrahent)}_{datetime.now().strftime('%Y-%m-%d')}.xlsx"
        fpath = base_path / fname

        sub[cols].to_excel(fpath, index=False)
        out_map[nip_str] = str(fpath.resolve())

    print(f"[RAPORTY] zapisano raporty w pliku")
    return out_map

def _only_digits(s: str) -> str:
    return re.sub(r"\D", "", str(s or "")).strip()


# funkcja zapisująca błedy do plików
# def export_error_log(error_log: list[dict], out_csv_path: str):
#     """Pełny log do jednego CSV + osobne pliki per-typ."""
#     if not error_log:
#         print("[VALID] Brak błędów – nic nie eksportuję.")
#         # zamiast return -> pozwól funkcji się zakończyć
#         return None
#
#     df_all = pd.DataFrame(error_log)
#     os.makedirs(os.path.dirname(out_csv_path) or ".", exist_ok=True)
#     df_all.to_csv(out_csv_path, index=False, encoding="utf-8-sig")
#     print(f"[VALID] Pełny log błędów zapisany: {out_csv_path}")


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

# =========================
# DB helpers
# =========================

def get_paid_invoice_keys() -> set[tuple[str, str]]:
    """
    Zwraca zbiór kluczy (nip_clean, numer_faktury_norm),
    czyli faktur, które JUŻ są w bazie (tabela faktury).
    """
    sql = """
        SELECT m.nip, f.numer_faktury
        FROM faktury f
        JOIN merchanci m ON m.id = f.id_kontrahenta
    """
    keys: set[tuple[str, str]] = set()

    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            for row in cur.fetchall():
                nip_clean = nip_digits(row["nip"])
                if len(nip_clean) != 10:
                    # ignorujemy dziwne NIP-y
                    continue
                doc_norm = _norm_doc_no(row["numer_faktury"])
                keys.add((nip_clean, doc_norm))

    logging.info("[DB] Wczytano %d opłaconych faktur z bazy.", len(keys))
    return keys

def nipy_db():
    query = """
        SELECT NIP from merchanci where NIP IS NOT NULL;
    """
    with db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
    return results

@contextmanager
def db_conn():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        yield conn
    finally:
        conn.close()

def db_fetchone(query: str, params: tuple):
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            return cur.fetchone()

def db_execute(query: str, params: tuple):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            conn.commit()

def nr_konta_z_bazy(nip: str, company:str):
    nip_num = int(nip_digits(nip))

    nrykont = {
        "shumee": "nr_konta_sm",
        "extrastore":"nr_konta_es",
        "greatstore":"nr_konta_gs",
    }

    nr_konta = nrykont.get(company, "nr_konta_sm")
    query = f"SELECT {nr_konta} from merchanci where nip =%s"
    rec = db_fetchone(query, (nip_num,))
    if rec and rec.get(nr_konta):
        print(rec[nr_konta])
        return rec[nr_konta]

    print(f"Brak nr konta w bazie dla NIP: {nip} {company}")
    return None

def zapisz_adres_do_bazy(nip: str, adres: str):
    nip_num = int(nip_digits(nip))
    db_execute("UPDATE Merchanci SET adres = %s WHERE nip = %s", (adres, nip_num))

def clean_address(addr: str) -> str:
    if not addr:
        return ""
    t = unicodedata.normalize("NFKC", str(addr))
    t = re.sub(r'^[\-\u2010\u2011\u2012\u2013\u2014\u2212\s]*\|+', '', t)
    t = re.sub(r'[\-\u2010\u2011\u2012\u2013\u2014\u2212]', ' ', t)
    t = re.sub(r'\s+', ' ', t).strip()
    t = re.sub(r'(\b\d{2}) (\d{3}\b)', r'\1-\2', t)
    t = re.sub(r'\s*\|\s*', '|', t)
    t = t.strip('|')
    return t

def adres_z_bazy(nip: str) -> str | None:
    nip_num = int(nip_digits(nip))
    rec = db_fetchone("SELECT adres FROM merchanci WHERE nip = %s", (nip_num,))
    return clean_address(rec["adres"]) if rec and rec.get("adres") else None
_ELIXIR_SAFE_MAP = {
    "\u2018": "'", "\u2019": "'",
    "\u201C": '"', "\u201D": '"', "\u201E": '"',
    "\u2013": "-", "\u2014": "-",
    "\u00A0": " ",
    "\u2026": "...",
    "\u2007": " ",
    "\u2009": " ",
    "\u00AD": "-",
    "-": "-",
}

def _elixir_safe_text(s: str) -> str:
    if s is None:
        return ""
    t = unicodedata.normalize("NFKC", str(s))
    t = t.translate(str.maketrans(_ELIXIR_SAFE_MAP))
    return t

def losowe_opoznienie(min_sec=0.05, max_sec=0.1):
    time.sleep(random.uniform(min_sec, max_sec))

def _latin_safe(s: str) -> str:
    return s.encode(OUTPUT_ENCODING, errors="replace").decode(OUTPUT_ENCODING)

def _latin_safe_join(lines: list[str]) -> str:
    return "\n".join(_latin_safe(line) for line in lines)

# ===================================
# Walidator białej listy kontrahentów
# ===================================

def clean_konto(konto: str) -> str:
    """Zwraca nr konta jako 26 cyfr"""
    return re.sub(r"\D", "", str(konto)).zfill(26)

def clean_nip(nip: str) -> str:
    """Zwraca NIP jako 10 cyfr"""
    return re.sub(r"\D", "", str(nip)).zfill(10)

def get_file(url: str,output_dir: str = "pliki_plaskie") -> str:
    os.makedirs(output_dir,exist_ok=True)
    local_filename = url.split("/")[-1]
    local_path = os.path.join(output_dir, local_filename)

    head = requests.head(url)
    if head.status_code == 404:
        raise FileNotFoundError(f"[E] Plik {url} nie istnieje na serwerze MF.")

    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_path, "wb") as f:
            shutil.copyfileobj(r.raw, f)

    print(f"Pobrano plik płaski: {local_path}")
    return local_path

# pobiera plik płaski z użyciem get_file()
# następnie rozpakowuje go do folderu
def unzip():
    output_dir = "pliki_plaskie"
    data = datetime.today().strftime("%Y%m%d")
    link_plik_plaski = f"https://plikplaski.mf.gov.pl/pliki/{data}.7z"

    archive_path = os.path.join(output_dir, f"{data}.7z")
    json_path = os.path.join(output_dir, f"{data}.json")

    if os.path.isfile(json_path):
        print(f"[I] Plik {json_path} już rozpakowany.")
        return json_path

    # Pobranie archiwum jeśli nie ma
    if not os.path.isfile(archive_path):
        archive_path = get_file(link_plik_plaski, output_dir=output_dir)

    # Rozpakowanie archiwum
    with py7zr.SevenZipFile(archive_path, mode='r') as archive:
        archive.extractall(output_dir)

    print(f"Rozpakowano plik płaski: {json_path}")
    return json_path

def Sha512Hash1(nip: str, nr_konta: str, data: str, iters: int = 5000) -> str:
    to_hash = str(data) + nip + nr_konta
    h = hashlib.sha512(to_hash.encode("utf-8")).hexdigest()
    for _ in range(iters - 1):
        h = hashlib.sha512(h.encode("utf-8")).hexdigest()
    return h

def Sha512HashNIP(nip: str, data: str, iters: int = 5000) -> str:
    nip = clean_nip(nip)
    to_hash = data + nip
    h = hashlib.sha512(to_hash.encode("utf-8")).hexdigest()
    for _ in range(iters - 1):
        h = hashlib.sha512(h.encode("utf-8")).hexdigest()
    return h

def data_from_db() -> Dict[str, str]:
    try:
        print("[DEBUG] Pobieram nipy i nr kont z merchanci:" )
        query = """
        SELECT nip, nr_konta_sm
        FROM merchanci
        WHERE nip IS NOT NULL AND nr_konta_sm IS NOT NULL;
        """
        result = {}

        with db_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query)
                for row in cur.fetchall():
                    nip_raw = str(row["nip"]).strip()

                    if nip_raw.endswith(".0"):
                        nip_raw = nip_raw[:-2]
                    nip_clean = clean_nip(nip_raw)
                    konto_clean = clean_konto(row["nr_konta_sm"])
                    result[nip_clean] = konto_clean

    except Exception as e:
        import traceback
        print("[X] Błąd w data_from_db:", e)
        traceback.print_exc()
        return {}

    print(f"[DB] Pobranie {len(result)} rekordów z merchanci.")
    return result

def group_maski_by_bank(maski: List[str]) -> Dict[str, List[str]]:
    grouped = {}
    for m in maski:
        bank_code = m[2:10]
        grouped.setdefault(bank_code, []).append(m)
    return grouped

def load_nipy_z_excela(file_path: str) -> set[str]:
    """Wczytuje kolumnę 'NIP' z pliku Excel i zwraca zestaw NIP-ów (10 cyfr)."""
    df = pd.read_excel(file_path)

    if "NIP" not in df.columns:
        raise ValueError("[WL] W pliku Excel nie znaleziono kolumny 'NIP'.")

    nipy = (
        df["NIP"]
        .dropna()
        .astype(str)
        .map(clean_nip)
        .unique()
    )

    print(f"[WL] Wczytano {len(nipy)} unikalnych NIP-ów z Excela.")
    #
    return set(nipy)

def load_flatfile(json_file: str):
    """Wczytuje plik płaski do pamięci"""
    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    naglowek = data.get("naglowek", {})
    gen_date = (
        naglowek.get("dataGenerowaniaPliku")
        or naglowek.get("dataGenerowaniaDanych")
        or naglowek.get("data")
        or datetime.today().strftime("%Y%m%d")
    ).replace("-", "")

    iters = int(naglowek.get("liczbaTransformacji", 5000))
    czynni = set(data.get("skrotyPodatnikowCzynnych", []))
    zwolnieni = set(data.get("skrotyPodatnikowZwolnionych", []))
    maski_map = group_maski_by_bank(data.get("maski", []))

    return gen_date, iters, czynni, zwolnieni, maski_map


def apply_mask(nr_konta: str, maska: str) -> str:
    result = []
    for i, m in enumerate(maska):
        if m == 'X':
            result.append('X')
        elif m == 'Y' and i < len(nr_konta):
            result.append(nr_konta[i])
        else:
            result.append(m)
    return "".join(result)

def sprawdz_excelowe_kontrahenty(json_file: str, excel_file: str):
    """Sprawdza NIP-y z Excela i bazy w pliku płaskim MF"""
    baza_danych = data_from_db()
    nipy_excel = load_nipy_z_excela(excel_file)

    # For DEBUG purposes only
    #print("[WL] Debug – 10 pierwszych z Excela:", list(nipy_excel)[:10])
    #print("[WL] Debug – 10 pierwszych z bazy:", list(baza_danych.keys())[:10])
    #print("[WL] Typ pierwszego z bazy:", type(next(iter(baza_danych.keys()))))

    nipy_wspolne = nipy_excel & set(baza_danych.keys())
    if not nipy_wspolne:
        print("[E] Brak wspólnych NIP-ów między Excelem a bazą.")
        return []

    gen_date, iters, czynni, zwolnieni, maski_map = load_flatfile(json_file)
    brakujace = []

    for nip in sorted(nipy_wspolne):
        konto = baza_danych.get(nip)
        nip_clean = clean_nip(nip)
        konto_clean = clean_konto(konto)
        znaleziony = False

        # pełny NRB
        hash_value = Sha512Hash1(nip_clean, konto_clean, data=gen_date, iters=iters)
        if hash_value in czynni or hash_value in zwolnieni:
            znaleziony = True
        else:
            # maski wg banku
            bank_code = konto_clean[2:10]
            for maska in maski_map.get(bank_code, []):
                masked_account = apply_mask(konto_clean, maska)
                hash_value = Sha512Hash1(nip_clean, masked_account, data=gen_date, iters=iters)
                if hash_value in czynni or hash_value in zwolnieni:
                    znaleziony = True
                    break
            # fallback po samym NIP
            if not znaleziony:
                hash_value = Sha512HashNIP(nip_clean, data=gen_date, iters=iters)
                if hash_value in czynni or hash_value in zwolnieni:
                    znaleziony = True

        if not znaleziony:
            brakujace.append((nip_clean, konto_clean))
            print(f"[WL] Brak w pliku płaskim: NIP={nip_clean}, Konto={konto_clean}")

    # raport CSV
    # if brakujace:
    #     df_b = pd.DataFrame(brakujace, columns=["NIP", "Konto"])
    #     df_b.to_csv("brak_na_bialej_liscie.csv", index=False, encoding="utf-8-sig")
    #     print(f"[WL] 📄 Zapisano raport: brak_na_bialej_liscie.csv")

    print(f"[WL] Sprawdzono kontrahentów: brak wpisu w pliku MF dla {len(brakujace)} pozycji.")
    return brakujace

def zapisz_faktury_do_bazy(df_to_db: pd.DataFrame, spolka: str) -> None:
    """
    Zapisuje faktury do tabeli 'faktury'.
    Zakładamy, że df_to_db ma kolumny:
      - 'Numer dokumentu'
      - 'Data wystawienia'
      - 'Netto', 'VAT', 'Brutto'
      - 'NIP'
      - '__netto_gr', '__vat_gr', '__brutto_gr' (grosze)
    """

    required = {
        "Numer dokumentu", "Data wystawienia",
        "Netto", "VAT", "Brutto",
        "__netto_gr", "__vat_gr", "__brutto_gr",
        "__nip_clean",
    }

    if df_to_db.empty:
        logging.info("[DB] Brak danych do zapisania.")
        return

    df_to_db = df_to_db.copy()
    df_to_db["__nip_clean"] = df_to_db["NIP"].astype(str).str.replace(r"\D", "", regex=True)

    missing = required - set(df_to_db.columns)
    if missing:
        raise ValueError(f"[DB] Brakuje kolumn w df_to_db: {', '.join(sorted(missing))}")

    print("[DEBUG] nazwy kolumn:")
    print(df_to_db.columns)

    with db_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:

        # wczytujemy istniejące rekordy -> (id_kontrahenta, numer_faktury)
        cur.execute("""
            SELECT id_kontrahenta, numer_faktury
            FROM faktury
        """)
        existing = {
            (row["id_kontrahenta"], row["numer_faktury"].strip())
            for row in cur.fetchall()
        }

        inserted = 0
        skipped = 0

        for _, row in df_to_db.iterrows():
            numer_faktury = str(row["Numer dokumentu"]).strip()

            # data wystawienia
            data_wystawienia = pd.to_datetime(
                row["Data wystawienia"],
                dayfirst=True,
                errors="coerce"
            )
            if pd.isna(data_wystawienia):
                skipped += 1
                logging.warning(
                    f"[DB] Zła data wystawienia dla FV {numer_faktury} → pomijam"
                )
                continue
            data_wystawienia = data_wystawienia.date()

            # kwoty zamieniane z gorszy na złotówki(dataframe w groszach)
            kw_netto  = (Decimal(row["__netto_gr"])  / 100).quantize(Decimal("0.01"))
            kw_vat    = (Decimal(row["__vat_gr"])    / 100).quantize(Decimal("0.01"))
            kw_brutto = (Decimal(row["__brutto_gr"]) / 100).quantize(Decimal("0.01"))

            nip = row["__nip_clean"]
            if len(nip) != 10 or not nip.isdigit():
                skipped += 1
                logging.warning(f"[DB] Zły NIP ({nip}) → pomijam FV {numer_faktury}")
                continue

            # Pobierz kontrahenta
            cur.execute("SELECT id FROM merchanci WHERE nip = %s", (int(nip),))
            kontrahent = cur.fetchone()
            if not kontrahent:
                skipped += 1
                logging.warning(f"[DB] Brak kontrahenta NIP={nip} → pomijam FV {numer_faktury}")
                continue

            id_kontrahenta = kontrahent["id"]

            # Duplikat?
            key = (id_kontrahenta, numer_faktury)
            if key in existing:
                skipped += 1
                logging.info(f"[DB] Pominięto duplikat: FV {numer_faktury} (NIP={nip})")
                continue

            # INSERT
            try:
                cur.execute("""
                    INSERT INTO faktury (
                        numer_faktury, data_wystawienia,
                        kwota_netto, kwota_vat, kwota_brutto,
                        typ_faktury, id_kontrahenta, nazwa_spolki
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id_kontrahenta, numer_faktury) DO NOTHING
                """, (
                    numer_faktury, data_wystawienia,
                    kw_netto, kw_vat, kw_brutto,
                    "POJEDYNCZA", id_kontrahenta, spolka
                ))

                # jeżeli DB nie wywaliła błędu – traktujemy jako zapisane / zignorowane przez ON CONFLICT
                existing.add(key)
                inserted += 1

            except psycopg2.Error as e:
                skipped += 1
                logging.error(
                    f"[DB] BŁĄD zapisu faktury {numer_faktury} (NIP={nip}): {e.pgerror}"
                )
                conn.rollback()        # anuluj ten INSERT, reszta transakcji dalej żyje
                continue

        conn.commit()

        logging.info(
            f"[DB] Zapisano prób {inserted} INSERT-ów (część mogła zostać zignorowana przez ON CONFLICT), "
            f"pominięto {skipped} wierszy (braki, błędy lub duplikaty)."
        )

# ============================================
# LOSOWE OPÓŹNIENIE
# ============================================

def losowe_opoznienie(min_sec=0.05, max_sec=0.25):
    time.sleep(random.uniform(min_sec, max_sec))

# ============================================
# SCRAPER REGON (Selenium + BS4 + HTML do pamięci)
# ============================================

class RegonScraper:
    """Jedna przeglądarka na cały wsad."""

    CHROMEDRIVER_PATH = os.getenv(
        "CHROMEDRIVER_PATH",
        r"C:\tools\chromedriver-win64\chromedriver.exe"
    )

    def __init__(self, chromedriver_path: str = CHROMEDRIVER_PATH, headless: bool = True):
        self.chromedriver_path = chromedriver_path
        self.headless = headless
        self.driver = None

        # HTML w pamięci
        self.html = None
        self.soup = None

    def __enter__(self):
        options = Options()
        if self.headless:
            options.add_argument("--headless=new")
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1920x1080")
            options.add_argument("--log-level=3")
            options.add_argument("--disable-logging")
            options.add_argument("--silent")
            options.add_experimental_option('excludeSwitches', ['enable-logging'])

        service = Service(self.chromedriver_path, log_path=os.devnull)
        self.driver = webdriver.Chrome(service=service, options=options)
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.driver:
            self.driver.quit()

    # ================================================
    # GŁÓWNA FUNKCJA SCRAPERA – pobiera dane z tabeli
    # ================================================
    def scrape_nip(self, nip: str) -> list[str]:
        d = self.driver

        d.get("https://wyszukiwarkaregon.stat.gov.pl/appBIR/index.aspx")
        losowe_opoznienie(0.05, 0.25)

        pole = d.find_element(By.ID, "txtNip")
        pole.clear()
        pole.send_keys(str(nip))

        d.find_element(By.ID, "btnSzukaj").click()
        losowe_opoznienie(0.15, 0.3)

        # ✅ Zapisz HTML do pamięci
        self.html = d.page_source
        self.soup = BeautifulSoup(self.html, "html.parser")

        # ✅ Znajdź wyniki
        rows = d.find_elements(By.CLASS_NAME, "tabelaZbiorczaListaJednostekAltRow") + \
               d.find_elements(By.CLASS_NAME, "tabelaZbiorczaListaJednostekRow")

        if not rows:
            return []

        cells = rows[0].find_elements(By.TAG_NAME, "td")
        return [c.text.strip() for c in cells]

# ============================================
# WYCIĄGANIE ADRESU
# ============================================

def wyciagnij_adres_z_komorek(cells: list[str]) -> str:
    """Przekształca komórki scrapera na format: Ulica|Kod|Miasto|Gmina ..."""

    if not cells:
        return ""

    # oryginalny zakres 5–8 (4 pola)
    start, end = 5, 9

    frag = cells[start:end] if len(cells) >= end else cells[max(0, len(cells) - 4):]

    if not frag:
        return ""

    def sanitize_text(s: str) -> str:
        return s.replace("\xa0", " ").strip()

    def clip35(s: str) -> str:
        return sanitize_text(s)[:35]

    return "|".join(clip35(x) for x in frag if x)

# =====================================================
#  GET-OR-FETCH (DB → scrape → DB)
#  tutaj integrujesz swoje adres_z_bazy(), nazwa_z_bazy()
# =====================================================

def get_or_fetch_adres(nip_clean: str, scraper: RegonScraper) -> str:
    """
    1. Próbuje wziąć adres z DB
    2. Jeśli brak → scrapuje REGON
    3. Zapisuje do DB
    """
    try:
        adr = adres_z_bazy(nip_clean) or ""
        nazwa = nazwa_z_bazy(nip_clean) or ""
        if adr.strip():
            return f"{nazwa}|{adr}" if nazwa else adr
    except Exception as e:
        print(f"[W] DB error: {e}")

    # SCRAPER
    try:
        raw_cells = scraper.scrape_nip(nip_clean)
        losowe_opoznienie(0.1, 0.25)

        adr = wyciagnij_adres_z_komorek(raw_cells)
        nazwa = nazwa_z_bazy(nip_clean) or ""

        if adr.strip():
            full_val = f"{nazwa}|{adr}" if nazwa else adr
            try:
                zapisz_adres_do_bazy(nip_clean, adr)
            except Exception as e:
                print(f"[W] Nie zapisano do DB: {e}")

            return full_val

        return nazwa or ""

    except Exception as e:
        print(f"[W] Błąd scrapera REGON: {e}")
        return ""

def get_or_fetch_konto(nip_clean: str, company:str) -> str:
    try:
        raw = nr_konta_z_bazy(nip_clean,company) or ""
        nrb = normalize_nrb(raw)
        print(f"Nr banku dla {nip_clean} : {nrb}")
        return nrb
    except Exception as e:
        print(f"[W] Błąd DB przy pobieraniu konta dla NIP {nip_clean}: {e}")
        return ""

def csv_quote(s: str) -> str:
    s = s or ""
    s = s.replace('"', '""')
    return f'"{s}"'

# =========================
# Budowa rekordu
# =========================

def nazwa_z_bazy(nip: str) -> str | None:
    nip_num = int(nip_digits(nip))
    rec = db_fetchone("SELECT nazwa FROM merchanci WHERE nip = %s", (nip_num,))
    if rec and rec.get("nazwa"):
        return cut_to_30(sanitize_text(rec["nazwa"]))
    return None

def build_payment_record(
    data_platnosci: str,
    kwota_brutto_gr: int,
    nr_rozliczeniowy_zleceniodawcy: str,
    tryb_realizacji: str,
    rachunek_zleceniodawcy: str,
    rachunek_kontrahenta: str,
    nazwa_i_adres_zleceniodawcy: str,
    nazwa_i_adres_kontrahenta: str,
    nr_rozliczeniowy_banku_kontrahenta: str,
    szczegoly_platnosci: str,
    klasyfikacja:str
    # klasyfikacja: str,
    # informacja_klient_bank: str,
) -> str:

    fields = [
        "110",
        data_platnosci,
        str(kwota_brutto_gr),
        nr_rozliczeniowy_zleceniodawcy,
        tryb_realizacji,
        rachunek_zleceniodawcy,
        rachunek_kontrahenta,
        sanitize_text(nazwa_i_adres_zleceniodawcy),  # 8
        sanitize_text(nazwa_i_adres_kontrahenta),    # 9
        "0",
        nr_rozliczeniowy_banku_kontrahenta,
        csv_quote(  szczegoly_platnosci),          # 12  (np. /NIP/…|/IDP/…|/TYT/…)
        "",
        "",
        klasyfikacja,
        # trim_to(sanitize_text(informacja_klient_bank), 19),  # 16 (max 19)
    ]
    return ",".join(fields)

def _norm_doc_no(x: str) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip()
    s = re.sub(r"\s+", " ", s)
    s = s.upper()
    return s

def _money_to_gr_series(s: pd.Series) -> pd.Series:
    return s.apply(money_to_grosze)

def find_duplicates(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    required = {"Numer dokumentu", "Netto", "VAT", "Brutto"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Brak kolumn: {', '.join(sorted(missing))}")

    d = df.copy()
    d["__doc_no_norm"] = df["Numer dokumentu"].map(_norm_doc_no)
    d["__netto_gr"] = _money_to_gr_series(df["Netto"])
    d["__vat_gr"]   = _money_to_gr_series(df["VAT"])
    d["__brut_gr"]  = _money_to_gr_series(df["Brutto"])

    group_sizes = d.groupby(["__doc_no_norm", "__netto_gr", "__vat_gr", "__brut_gr"])["Numer dokumentu"].transform("size")
    d["__is_dup_group"] = group_sizes > 1
    full_dup_groups = d.loc[d["__is_dup_group"]].copy()

    return d, full_dup_groups.sort_values(["__doc_no_norm", "__netto_gr", "__vat_gr", "__brut_gr"])



def handle_duplicates(df: pd.DataFrame, action: str = "error") -> pd.DataFrame:
    d, full_dups = find_duplicates(df)

    if full_dups.empty:
        return df

    preview_cols  = ["Numer dokumentu", "Netto", "VAT", "Brutto"]
    print("[DUP] Wykryto duplikaty:\n", full_dups[preview_cols].to_string(index=False))

    if action == "error":
        raise ValueError("W pliku znajdują się duplikaty (patrz log powyżej).")
    elif action == "warn":
        return df
    elif action in ("drop_keep_first", "drop_keep_last"):
        keep = "first" if action == "drop_keep_first" else "last"
        mask = d.duplicated(subset=["__doc_no_norm", "__netto_gr", "__vat_gr", "__brut_gr"], keep=keep)
        cleaned = df.loc[~mask].copy()
        print(f"[DUP] Usunięto {mask.sum()} zduplikowanych wierszy ({action}).")
        return cleaned
    else:
        raise ValueError(f"Nieznane action='{action}'")

# używane do skracania ciągów znaków do 30
# użycie : głónie do nazwa i adres kontrahenta
def cut_to_30(s: str) -> str:
    if not s:
        return ""
    return s[:30]

def export_duplicates_report(df: pd.DataFrame, out_path: str):
    _, full_dups = find_duplicates(df)
    if full_dups.empty:
        print("[DUP] Brak duplikatów – raport nie został utworzony.")
        return
    cols = ["Numer dokumentu", "Netto", "VAT", "Brutto"]
    full_dups[cols].to_csv(out_path, index=False, encoding="utf-8")
    print(f"[DUP] Raport duplikatów zapisany: {out_path}")

#######################################
# Ładowanie listy świąt wolnych od pracy
# ze strony NBP
#######################################

def load_holidays_or_exit() -> set:
    """
    Pobiera święta z NBP.
    Jeśli scrapowanie się nie powiedzie → KOŃCZY PROGRAM z komunikatem.
    Zwraca: set() dat typu datetime.date
    """
    URL = "https://nbp.pl/o-nbp/dni-wolne/"
    HEADERS = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "pl-PL,pl;q=0.9,en;q=0.8",
    }

    try:
        r = requests.get(URL, headers=HEADERS, timeout=15)
        r.raise_for_status()
    except Exception as e:
        print("❌ BŁĄD: Nie można połączyć się z NBP!")
        print("🔌 Sprawdź internet lub dostęp do https://nbp.pl/o-nbp/dni-wolne/")
        print(f"🔍 Szczegóły: {e}")
        sys.exit(1)

    # Scrap HTML
    try:
        soup = BeautifulSoup(r.text, "lxml")
        table = soup.select_one("table.table")

        if table is None:
            raise RuntimeError("Brak tabeli świąt w HTML")

        df = pd.read_html(io.StringIO(str(table)))[0]
    except Exception as e:
        print("❌ BŁĄD: Nie udało się odczytać tabeli świąt ze strony NBP!")
        print("🔍 Struktura strony mogła się zmienić.")
        print(f"📄 Szczegóły: {e}")
        sys.exit(1)

    # Konwersja świąt → set(datetime.date)
    holidays = set()
    year = datetime.now().year

    for _, row in df.iterrows():
        val = str(row.iloc[1])
        m = re.match(r"(\d{1,2})\s+(\w+)", val)
        if not m:
            continue

        day, month_name = m.groups()

        months = {
            'stycznia': 1, 'lutego': 2, 'marca': 3, 'kwietnia': 4,
            'maja': 5, 'czerwca': 6, 'lipca': 7, 'sierpnia': 8,
            'września': 9, 'października': 10, 'listopada': 11, 'grudnia': 12
        }

        if month_name.lower() not in months:
            continue

        holidays.add(datetime(year, months[month_name.lower()], int(day)).date())

    # print(f"[HOLIDAYS] Załadowano {len(holidays)} świąt z NBP.")
    return holidays

# -------------------------------
# CACHE ŚWIĄT ŁADOWANY TYLKO RAZ
# -------------------------------
try:
    HOLIDAYS = load_holidays_or_exit()
except SystemExit:
    raise


def get_previous_workday(date: datetime) -> datetime.date:
    prev = (date - timedelta(days=1)).date()

    while prev.weekday() >= 5 or prev in HOLIDAYS:
        prev -= timedelta(days=1)

    return prev


def _safe_add30(s_min: str | None, s_max: str | None) -> str:
    today = datetime.now().date()
    base_str = s_max or s_min

    if not base_str:
        return today.strftime("%Y%m%d")

    base_date = datetime.strptime(base_str, "%Y%m%d").date()

    if base_date == today:
        return today.strftime("%Y%m%d")

    target = base_date + timedelta(days=30)

    # 👉 korzystamy z globalnego HOLIDAYS
    if target.weekday() >= 5 or target in HOLIDAYS:
        target = get_previous_workday(datetime.combine(target, datetime.min.time()))

    return target.strftime("%Y%m%d")


def gr_to_pln_comma(v_gr: int) -> str:
    v = int(v_gr)
    sign = "-" if v < 0 else ""
    v = abs(v)
    return f"{sign}{v//100},{v%100:02d}"

# ====================================
# GŁÓWNA FUNKCJA zapisująco - tworząca
# ====================================

# statusy płątników vat
ACTIVE_STATUSES = {"Czynny", "ACTIVE", "czynny"}

def przetworz_plik_xlsx(
    input_file: str,
    *,
    company: str,
    output_path: Optional[str] = None,
    duplicates_action: str = "warn",
    headless: bool = True,
    merged_csv: Optional[str] = None,
    per_group_dir: Optional[str] = None,
    save_db: bool = False,
):
    # Część walidacyjno - sprawdzająca
    key = company.strip().lower()
    if key not in COMPANIES:
        raise ValueError(f"Nieznana firma: {company}. Dozwolone: {', '.join(sorted(COMPANIES))}")

    # --- Integracja z Białą Listą MF ---
    try:
        print("[WL] Pobieram i sprawdzam plik płaski MF...")
        json_path = unzip()
        sprawdz_excelowe_kontrahenty(json_path, input_file)
    except Exception as e:
        print(f"[WL] ⚠️ Błąd podczas sprawdzania kontrahentów: {e}")

    conf = COMPANIES[key]
    nazwa_i_adres_zleceniodawcy = conf["name_addr"]
    nr_rozliczeniowy_zleceniodawcy = conf["bank_code"]  # 8 cyfr
    rachunek_zleceniodawcy = conf["nrb"]                # 26 cyfr
    tryb_realizacji = "0"
    klasyfikacja = "53"

    # sanity check nadawcy
    if len(re.sub(r"\D", "", rachunek_zleceniodawcy)) != 26:
        raise ValueError(f"NRB nadawcy ma niepoprawną długość (26 cyfr): {rachunek_zleceniodawcy}")
    if not re.fullmatch(r"\d{8}", nr_rozliczeniowy_zleceniodawcy):
        raise ValueError(f"Kod rozliczeniowy nadawcy musi mieć 8 cyfr: {nr_rozliczeniowy_zleceniodawcy}")

    # --- ścieżki / timestamp ---
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    if output_path is None:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        output_path = os.path.join(OUTPUT_DIR, f"{key}_przelewy_{ts}.txt")

    # --- wczytanie + duplikaty ---
    df = pd.read_excel(input_file)
    df = handle_duplicates(df, action=duplicates_action)
    export_duplicates_report(df, os.path.join(OUTPUT_DIR, f"duplikaty_{ts}.csv"))

    # --- weryfikacja kolumn ---
    wymagane = {"Numer dokumentu", "Kontrahent", "NIP", "Data wpływu", "Brutto", "Netto", "VAT","Data wystawienia"}
    brak = wymagane - set(df.columns)
    if brak:
        raise ValueError(f"Brak kolumn w pliku: {', '.join(sorted(brak))}")

    # --- wyszukiwanie zakazanych kontrahentów ---
    forbidden_name_list = COMPANIES[key]["forbidden_name"]
    forbidden_nip_list = COMPANIES[key]["forbidden_nip"]

    # Sprawdzenie czy da się pobrać dni wolne od pracy ze strony głównej NBP
    HOLIDAYS = load_holidays_or_exit()


    mask = df["Kontrahent"].isin(forbidden_name_list)
    maska_nip = df["NIP"].isin(forbidden_nip_list)
    if mask.any():
        print("[WARN] PLIK ZAWIERA ZAKAZANYCH KONTRAHENTÓW:")
        print(df.loc[mask,["Kontrahent"]])
    if maska_nip.any():
        print("[WARN] PLIK ZAWIERA NIPY ZAKAZANYCH KONTRAHENTÓW:")
        print(df.loc[maska_nip, ["NIP"]])

    # --- walidacja (loguj, nie wycinaj) ---
    df, error_log = validate_df(
        df,
        date_col="Data wystawienia",
        netto_col="Netto",
        vat_col="VAT",
        brutto_col="Brutto",
        tol=0.01,
        on_error="keep",
    )
    #export_error_log(error_log, os.path.join(OUTPUT_DIR, f"errors_{ts}.csv"))

    # --- ujemne kwoty do raportu i outputu ---
    mask_negative = (df["Netto"] <= 0) | (df["VAT"] <= 0) | (df["Brutto"] <= 0)
    if mask_negative.any():
        print(f"[WARN] Pomijam {int(mask_negative.sum())} wierszy z ujemnymi kwotami (zapisano raport).")
        df.loc[mask_negative].to_csv(os.path.join(OUTPUT_DIR, f"ujemne_{ts}.csv"), index=False, encoding="utf-8-sig")
    df = df.loc[~mask_negative].copy()
    if df.empty:
        with open(output_path, "w", encoding=OUTPUT_ENCODING) as f:
            f.write("")
        print("[INFO] Po filtracji brak poprawnych wierszy.")
        return
    try:
        raporty = export_grouped_excels(
            df=df,
            out_dir="raporty_faktur_xlsx",
            nazwa_spolki=company.upper()
        )
        print("[XLSX] Raporty zapisane:", len(raporty))
    except Exception as e:
        print(f"[XLSX] Błąd generowania raportów: {e}")

    raport_excell_dir = "raporty_faktur_xlsx"

    logging.info(f"[EXPORT] Zapisano raport z fakturami kontrahentów do folderu %s",raport_excell_dir)

    df["__vat_gr"] = df["VAT"].apply(money_to_grosze)
    df["__netto_gr"] = df["Netto"].apply(money_to_grosze)
    df["__brutto_gr"] = df["Brutto"].apply(money_to_grosze)

    df["__data_str"] = df["Data wystawienia"].apply(serializacja_dat)  # YYYYMMDD
    df["__nip_clean"] = df["NIP"].astype(str).str.replace(r"\D", "", regex=True)
    df["__nip_two"] = df["NIP"]
    df["__doc_no_norm"] = df["Numer dokumentu"].map(_norm_doc_no)

    #  pobierz z bazy faktury już opłacone ( po nipie i nr_faktury)
    paid_keys = get_paid_invoice_keys()  # set[(nip_clean, numer_faktury_norm)]

    # oznaczanie już opłaconych wierszy
    def _is_paid(row) -> bool:
        nip = row["__nip_clean"]
        if len(nip) != 10 or not nip.isdigit():
            return False  # bez NIP-a nie jesteśmy w stanie sprawdzić -> traktujemy jako nowe
        key = (nip, row["__doc_no_norm"])
        return key in paid_keys

    mask_paid = df.apply(_is_paid, axis=1)
    num_paid = int(mask_paid.sum())

    if num_paid:
        skipped_path = os.path.join(
            OUTPUT_DIR,
            f"pominiete_oplacone_{key}_{ts}.csv"
        )
        df.loc[mask_paid, ["Numer dokumentu", "NIP", "Kontrahent", "Brutto"]].to_csv(
            skipped_path,
            index=False,
            encoding="utf-8-sig"
        )
        print(f"[DB] Pominięto {num_paid} wierszy – faktury już opłacone (log: {skipped_path})")

    # 3) wyrzucenie opłaconych faktury z dalszego przetwarzania
    df = df.loc[~mask_paid].copy()

    if df.empty:
        print("[ELIXIR] Wszystkie faktury z pliku są już opłacone – nie generuję plików ELIXIR.")
        return

    # klucz grupowania: NIP(10) albo fallback NAME::
    df["__grp_key"] = df.apply(
        lambda r: r["__nip_clean"] if len(r["__nip_clean"]) == 10 else f"NAME::{r['Kontrahent']}",
        axis=1
    )

    # --- agregacja: kontrahent/dzień (data wpływu) ---
    df_day = df.loc[df["__data_str"].notna()].copy()
    agg = (
        df_day.groupby(["__grp_key", "__data_str"], as_index=False)
        .agg(
            nip_clean   =("__nip_clean", "first"),
            kontrahent  =("Kontrahent", "first"),
            first_doc   =("Numer dokumentu", "first"),
            suma_brutto =("Brutto", "sum"),
            suma_vat    =("VAT", "sum"),
            suma_netto  =("Netto", "sum"),
            cnt_docs    =("Numer dokumentu", "nunique"),
            cnt_rows    =("Brutto", "size"),
        )
    )

    # kwoty w groszach po sumowaniu w PLN
    agg["suma_brutto_gr"] = agg["suma_brutto"].apply(money_to_grosze)
    agg["suma_vat_gr"]    = agg["suma_vat"].apply(money_to_grosze)
    agg["suma_netto_gr"]  = agg["suma_netto"].apply(money_to_grosze)

    # daty do pliku bankowego
    def _safe_ddmmyy(s: str) -> str:
        return datetime.strptime(s, "%Y%m%d").strftime("%d%m%y")
    agg["data_platnosci"]     = agg["__data_str"].apply(lambda s: _safe_add30(s, s))  # +30 dni
    agg["data_wplywu_ddmmyy"] = agg["__data_str"].apply(_safe_ddmmyy)

    # twardy check kolumn
    required = {"__data_str", "data_platnosci", "data_wplywu_ddmmyy",
                "suma_brutto_gr", "suma_vat_gr", "suma_netto_gr"}
    missing = required - set(agg.columns)
    if missing:
        raise RuntimeError(f"Brakuje kolumn w 'agg': {missing}")

    json_path = save_grouped_json(df, agg, key, base_dir="json")
    print(f"[JSON] Zapisano plik: {json_path}")

    if merged_csv:
        # Bezpieczna serializacja "Data wpływu" -> YYYYMMDD
        def _safe_yyyymmdd(val):
            try:
                return serializacja_dat(val)
            except Exception:
                return None

        df_rep = df.copy()
        # Data wpływu jako string YYYYMMDD do grupowania
        df_rep["__wplyw_str"] = df_rep["Data wystawienia"].map(_safe_yyyymmdd)
        # NIP oczyszczony z niedozwolonych znaków (10 cyfr albo pusty)
        df_rep["__nip_clean"] = df_rep["NIP"].astype(str).str.replace(r"\D", "", regex=True)

        # Tylko wiersze z poprawną datą wpływu
        df_rep = df_rep.loc[df_rep["__wplyw_str"].notna()].copy()

        # Agregacja po: Data wpływu, NIP, Kontrahent
        # Sumujemy na groszach, potem konwertujemy do PLN z 2 miejscami.
        raport = (
            df_rep.groupby(["__wplyw_str", "__nip_clean", "Kontrahent"], as_index=False)
            .agg(Suma_Brutto_gr=("__brutto_gr", "sum"))
        )

        # Format daty w raporcie jako YYYY-MM-DD (czytelniejsze) i kolumny wyjściowe w żądanej kolejności
        raport["Data wystawienia"] = pd.to_datetime(raport["__wplyw_str"], format="%Y%m%d").dt.strftime("%Y-%m-%d")
        raport["NIP"] = raport["__nip_clean"]
        raport["Kontrahent"] = raport["Kontrahent"]
        raport["Kwota Brutto"] = (raport["Suma_Brutto_gr"] / 100).round(2)

        raport = raport[["Data wystawienia", "NIP", "Kontrahent", "Kwota Brutto"]].sort_values(
            ["Data wystawienia", "NIP", "Kontrahent"])

        raport.to_csv(merged_csv, index=False, encoding="utf-8-sig")
        print(f"[RAPORT] zapisany raport do pliku: {merged_csv}")

    # --- PREFETCH WL: NIP -> NRB (DB) oraz batch /search/bank-account/{NRB} ---
    valid_nips = sorted({
        str(n) for n in agg["nip_clean"].astype(str)
        if n.isdigit() and len(str(n)) == 10
    })

    # konto per NIP z DB; brak => "000...0"
    nip_to_nrb: dict[str, str] = {}
    for nip in valid_nips:
        raw = get_or_fetch_konto(nip, company) or ""
        nrb = normalize_nrb(raw)
        nip_to_nrb[nip] = nrb if nrb else "0" * 26


    # --- generowanie rekordów i buforowanie per-dzień ---
    lines_by_day: dict[str, list[str]] = defaultdict(list)
    adres_cache: dict[str, str] = {}

    from selenium.common.exceptions import WebDriverException  # na wszelki wypadek

    try:
        with RegonScraper(CHROMEDRIVER_PATH, headless=headless) as scraper:
            for _, row in agg.iterrows():
                nip_clean = str(row["nip_clean"] or "")
                valid_nip = nip_clean.isdigit() and len(nip_clean) == 10
                kontrahent_name = row["kontrahent"]

                # kwoty już po agregacji
                kw_brutto_gr = int(row["suma_brutto_gr"])
                kw_vat_gr    = int(row["suma_vat_gr"])
                # adres + konto kontrahenta
                if valid_nip:
                    adres_kontr = adres_cache.get(nip_clean) or get_or_fetch_adres(nip_clean, scraper)
                    adres_cache[nip_clean] = adres_kontr
                    rachunek_kontrahenta = nip_to_nrb.get(nip_clean, "0"*26)
                else:
                    adres_kontr = kontrahent_name
                    rachunek_kontrahenta = "0" * 26

                adres_kontr = clean_address(adres_kontr)
                nr_rozliczeniowy_banku_kontrahenta = bank_code_from_nrb(rachunek_kontrahenta)


                # pole 16 (max 19) – NIP + ddmmyy z daty wpływu
                nip_for_ref = nip_clean if valid_nip else "NA"
                data_wplywu_ddmmyy = row["data_wplywu_ddmmyy"]
                informacja = (f"{nip_for_ref}{data_wplywu_ddmmyy}")

                vat_txt = gr_to_pln_comma(kw_vat_gr)

                szczegoly = (
                    f"/VAT/{vat_txt}|"
                    f"/IDC/{nip_clean or 'NA'}|"
                    f"/INV/FV{data_wplywu_ddmmyy}|"
                    f"/IDP/{informacja}"
                )

                # szczegoly_wrapped = wrap_szczegoly(szczegoly)

                line = build_payment_record(
                    data_platnosci=row["data_platnosci"],  # już policzone w agg (D+30)
                    kwota_brutto_gr=kw_brutto_gr,
                    nr_rozliczeniowy_zleceniodawcy=nr_rozliczeniowy_zleceniodawcy,
                    tryb_realizacji=tryb_realizacji,
                    rachunek_zleceniodawcy=rachunek_zleceniodawcy,
                    rachunek_kontrahenta=rachunek_kontrahenta,
                    nazwa_i_adres_zleceniodawcy=nazwa_i_adres_zleceniodawcy,
                    nazwa_i_adres_kontrahenta=adres_kontr,
                    nr_rozliczeniowy_banku_kontrahenta=nr_rozliczeniowy_banku_kontrahenta,
                    szczegoly_platnosci=szczegoly,
                    klasyfikacja=klasyfikacja
                )

                day_key = row["__data_str"]  # YYYYMMDD
                lines_by_day[day_key].append(line)

    except WebDriverException as e:
        print(f"[SCRAPER] Błąd Selenium: {e}. Kontynuuję bez scrapera (adresy mogą być surowe).")
        # awaryjnie bez adresów z REGON – generuj analogicznie
        for _, row in agg.iterrows():
            nip_clean = str(row["nip_clean"] or "")
            valid_nip = nip_clean.isdigit() and len(nip_clean) == 10
            kontrahent_name = row["kontrahent"]

            kw_brutto_gr = int(row["suma_brutto_gr"])
            kw_vat_gr    = int(row["suma_vat_gr"])

            if valid_nip:
                adres_kontr = kontrahent_name  # fallback
                rachunek_kontrahenta = nip_to_nrb.get(nip_clean, "0"*26)
            else:
                adres_kontr = kontrahent_name
                rachunek_kontrahenta = "0"*26

            adres_kontr = clean_address(adres_kontr)
            nr_rozliczeniowy_banku_kontrahenta = bank_code_from_nrb(rachunek_kontrahenta)

            nip_for_ref = nip_clean if valid_nip else "NA"
            data_wplywu_ddmmyy = row["data_wplywu_ddmmyy"]
            informacja = trim_to(f"{nip_for_ref}{data_wplywu_ddmmyy}", 19)

            vat_txt = gr_to_pln_comma(kw_vat_gr)

            szczegoly = (
                f"/VAT/{vat_txt}|"
                f"/IDC/{nip_clean or 'NA'}|"
                f"/INV/FV{data_wplywu_ddmmyy}|"
                f"/IDP/{informacja}"
            )

            line = build_payment_record(
                data_platnosci=row["data_platnosci"],
                kwota_brutto_gr=kw_brutto_gr,
                nr_rozliczeniowy_zleceniodawcy=nr_rozliczeniowy_zleceniodawcy,
                tryb_realizacji=tryb_realizacji,
                rachunek_zleceniodawcy=rachunek_zleceniodawcy,
                rachunek_kontrahenta=rachunek_kontrahenta,
                nazwa_i_adres_zleceniodawcy=nazwa_i_adres_zleceniodawcy,
                nazwa_i_adres_kontrahenta=adres_kontr,
                nr_rozliczeniowy_banku_kontrahenta=nr_rozliczeniowy_banku_kontrahenta,
                szczegoly_platnosci=szczegoly,
                klasyfikacja=klasyfikacja
            )

            day_key = row["__data_str"]
            lines_by_day[day_key].append(line)

    # --- zapis per-dzień: <firma>_przelewy_w_<ddmmyy_wplywu>_p_<ddmmyy_platnosci>.txt ---
    # zmiana miejsca zapisu plików z głównego folderu na bardziej uporządkowany sposób)

    today_str = datetime.now().strftime("%Y%m%d")
    base_dir = os.path.join("pliki_przelewow", key, f"przelewy_{key}_{today_str}")
    os.makedirs(base_dir, exist_ok=True)

    def _ddmmyy(s: str) -> str:
        return datetime.strptime(s, "%Y%m%d").strftime("%d%m%y")

    total_saved = 0
    for day_key, day_lines in sorted(lines_by_day.items()):
        data_wystawienia    = day_key
        data_platnosci = _safe_add30(day_key, day_key)
        out_name = f"{key}_przelewy_w_{_ddmmyy(data_wystawienia)}_p_{_ddmmyy(data_platnosci)}.txt"
        out_day_path = os.path.join(base_dir, out_name)

        # kontrola znaków (opcjonalna)
        for i, line in enumerate(day_lines, start=1):
            try:
                line.encode(OUTPUT_ENCODING, errors="strict")
            except UnicodeEncodeError as e:
                bad = line[e.start:e.end]
                print(f"[ENC] {day_key} linia {i}: niekodowalne {repr(bad)} → zastąpię '?'")

        with open(out_day_path, "w", encoding=OUTPUT_ENCODING, newline="") as f:
            f.write(_latin_safe_join(day_lines))

        print(f"[ELIXIR] Zapisano {len(day_lines)} rekordów dla dnia {day_key} → {out_day_path} (encoding={OUTPUT_ENCODING})")
        total_saved += len(day_lines)

    print(f"[ELIXIR] Łącznie zapisanych rekordów (wszystkie dni): {total_saved}")

    # zapis faktur do bazy danych
    if getattr(args, "save_db", False):
        try:
            logging.info("[DB] Rozpoczynam zapis faktur do bazy...")
            # tu zapisujemy dokładnie te faktury, które poszły do ELIXIR-a
            zapisz_faktury_do_bazy(df_day, company.upper())
        except Exception as e:
            logging.error("[DB] Błąd zapisu faktur do bazy: %s", e)
    else:
        logging.info("[DB] Pominięto zapis do bazy (brak flagi --save-db).")


# --- CLI aplikacji ---
if __name__ == "__main__":
    parser = ArgumentParser(description="Generator pliku ELIXIR-0 dla mBanku")
    parser.add_argument("input", help="Ścieżka do XLSX z fakturami")
    parser.add_argument("-o", "--output", help="Ścieżka wyjściowa .txt (domyślnie: ./<firma>_przelewy_<ts>.txt)")
    parser.add_argument("-c", "--company",
                        required=True,
                        choices=sorted(COMPANIES.keys()),
                        help=f"Firma (nadawca): {', '.join(sorted(COMPANIES.keys()))}")
    parser.add_argument("--dup",
                        choices=["error", "warn", "drop_keep_first", "drop_keep_last"],
                        default="warn",
                        help="Obsługa duplikatów (domyślnie: warn)")
    parser.add_argument("--headless", action=BooleanOptionalAction, default=True,
                        help="Selenium w trybie bez okna (domyślnie: włączony)")
    parser.add_argument("--merged-csv",
                        help="Ścieżka zbiorczego CSV z raportem scalonych grup (domyślnie: ./raport_scalonych_<ts>.csv)")
    parser.add_argument("--per-group-dir",
                        help="Katalog na osobne CSV per kontrahent/per data; jeśli nie podasz – nie tworzy.")
    parser.add_argument("--save-db",action="store_true",help="zapisuje faktury do bazy danych jako ich status jako opłacony")
    args = parser.parse_args()

    przetworz_plik_xlsx(
        args.input,
        company=args.company,
        output_path=args.output,
        duplicates_action=args.dup,
        headless=args.headless,
        merged_csv=args.merged_csv,
        per_group_dir=args.per_group_dir,
        save_db = args.save_db
    )