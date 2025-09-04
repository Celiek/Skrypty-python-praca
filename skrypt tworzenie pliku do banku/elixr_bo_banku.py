import os
import re
import time
import random
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from contextlib import contextmanager
import unicodedata
from argparse import ArgumentParser, BooleanOptionalAction
from typing import Optional

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from collections import Counter
import json
import requests

# TODO
# Zmiana nazwy plkiku na kolumne 17 elixir DONE
# ujemne pozycje nie mogą być uwzględnianie w agregacji faktur DONE
# zmienić agregację przelewów po nipie i dacie DONE - check first
# Dodać cachowanie danych do pliku żeby nie palić dziennego limitu
# zapytań do whitelisty vatowców

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
        "bank_code": os.getenv("EXTRASTORE_BANK_CODE", "11402004"),  # 8 cyfr
    },
}

CHROMEDRIVER_PATH = os.getenv("CHROMEDRIVER_PATH", r"C:\tools\chromedriver-win64\chromedriver.exe")

OUTPUT_DIR = os.getenv("OUTPUT_DIR", ".")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Domyślnie ISO-8859-2 (lub nadpisz w .env)
OUTPUT_ENCODING = os.getenv("OUTPUT_ENCODING", "iso8859_2").lower()

# =========================
# Normalizacja / kodowanie
# =========================

# --- WALIDACJA DANYCH ---

def wl_api_iso_date_from_yyyymmdd(yyyymmdd: str | None) -> str:
    """Zamienia YYYYMMDD -> YYYY-MM-DD; gdy brak/niepoprawne, zwraca dzisiejszą."""
    try:
        if yyyymmdd:
            return datetime.strptime(yyyymmdd, "%Y%m%d").strftime("%Y-%m-%d")
    except Exception:
        pass
    return datetime.now().strftime("%Y-%m-%d")


def wl_search_nip(nip: str, date_iso: str) -> dict:
    """
    GET https://wl-api.mf.gov.pl/api/search/nip/{nip}?date=YYYY-MM-DD
    Zwraca: {"ok": bool, "status": str|None, "subject": dict|None, "error": str|None}
    """
    url = f"https://wl-api.mf.gov.pl/api/search/nip/{nip}?date={date_iso}"
    try:
        r = requests.get(url, timeout=12)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        return {"ok": False, "status": None, "subject": None, "error": f"HTTP/parse: {e}"}

    # Normalizacja znanych struktur odpowiedzi
    try:
        result = data.get("result") or {}
        subject = result.get("subject")
        if subject is None:
            # niektóre warianty zwracają listę subjects
            subjects = result.get("subjects") or []
            subject = subjects[0] if subjects else None
        status = None
        if subject:
            # w API MF klucz zwykle nazywa się 'statusVat'
            status = subject.get("statusVat") or subject.get("status")
        return {"ok": True, "status": status, "subject": subject, "error": None}
    except Exception as e:
        return {"ok": False, "status": None, "subject": None, "error": f"schema: {e}"}


def wl_check_account(nip: str, bank_account_nrb: str, date_iso: str) -> dict:
    """
    GET https://wl-api.mf.gov.pl/api/check/nip/{nip}/bank-account/{nrb}?date=YYYY-MM-DD
    Zwraca: {"ok": bool, "assigned": bool|None, "error": str|None}
    """
    nrb = normalize_nrb(bank_account_nrb)
    if not nrb:
        return {"ok": False, "assigned": None, "error": "Pusty/niepoprawny NRB"}
    url = f"https://wl-api.mf.gov.pl/api/check/nip/{nip}/bank-account/{nrb}?date={date_iso}"
    try:
        r = requests.get(url, timeout=12)
        r.raise_for_status()
        data = r.json()
        result = data.get("result") or {}
        # wg specyfikacji MF bywa 'accountAssigned'
        assigned = result.get("accountAssigned")
        # czasem API zwraca 'message' przy błędach 200 OK
        if assigned is None and result.get("message"):
            return {"ok": False, "assigned": None, "error": result.get("message")}
        return {"ok": True, "assigned": bool(assigned), "error": None}
    except Exception as e:
        return {"ok": False, "assigned": None, "error": f"HTTP/parse: {e}"}


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

    # --- ujemne wartości -> tylko LOG (nie błąd blokujący) ---
    for num_col, orig_col, tag in [
        ("_netto_num",  netto_col,  "negative_netto"),
        ("_vat_num",    vat_col,    "negative_vat"),
        ("_brutto_num", brutto_col, "negative_brutto"),
    ]:
        mask = (d[num_col] < 0).fillna(False)
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

    # 🚨 UJEMNE KWOTY NIE są błędem blokującym
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
    """Usuwa zabronione znaki i nadmiarowe spacje."""
    if text is None:
        return ""
    text = _elixir_safe_text(text)
    bad = '*;!+?#'
    cleaned = "".join(c for c in str(text) if c not in bad)
    return " ".join(cleaned.split())

def add_days_to_date_str(date_str: str, days: int) -> str:
    """Dodaje dni do daty (YYYYMMDD) i zwraca (YYYYMMDD)."""
    dt = datetime.strptime(date_str, "%Y%m%d")
    dt_new = dt + timedelta(days=days)
    return dt_new.strftime("%Y%m%d")

def sanitize_nazwa_folderu(text: str) -> str:
    """Sanityzacja nazw folderów pod Windows/Unix."""
    if text is None:
        return ""
    text = _elixir_safe_text(text)
    bad = '*;!+?#",<>:\\/|'
    cleaned = "".join(c for c in str(text) if c not in bad)
    return " ".join(cleaned.split())


# ===========================================
# Utils
# ===========================================
def print_error_summary(error_log: list[dict], *, max_per_type: int = 10):
    """Krótkie podsumowanie + próbka błędów dla każdego typu."""
    if not error_log:
        print("[VALID] Brak błędów ✅")
        return

    cnt = Counter(e["type"] for e in error_log)
    print("[VALID] Szczegóły błędów (liczba wystąpień):")
    for t, n in cnt.most_common():
        print(f"  - {t}: {n}")

    # Próbki (żeby nie zalać konsoli)
    print("\n[VALID] Próbki błędów (po maks. {} na typ):".format(max_per_type))
    by_type = {}
    for e in error_log:
        by_type.setdefault(e["type"], []).append(e)

    for t, rows in by_type.items():
        sample = rows[:max_per_type]
        df_err = pd.DataFrame(sample)
        cols_pref = ["row", "doc", "value", "netto", "vat", "brutto", "diff", "msg"]
        cols = [c for c in cols_pref if c in df_err.columns]
        print(f"\n--- {t} (pokazuję {len(sample)} z {len(rows)}) ---")
        if cols:
            print(df_err[cols].to_string(index=False))
        else:
            print(pd.DataFrame(sample).to_string(index=False))

def export_error_log(error_log: list[dict], out_csv_path: str):
    """Pełny log do jednego CSV + osobne pliki per-typ."""
    if not error_log:
        print("[VALID] Brak błędów – nic nie eksportuję.")
        return

    df_all = pd.DataFrame(error_log)
    os.makedirs(os.path.dirname(out_csv_path) or ".", exist_ok=True)
    df_all.to_csv(out_csv_path, index=False, encoding="utf-8-sig")
    print(f"[VALID] Pełny log błędów zapisany: {out_csv_path}")

    # podział per-typ (wygodne do filtracji w Excelu)
    for t, sub in df_all.groupby("type"):
        safe_t = re.sub(r"[^0-9A-Za-z_.-]+", "_", str(t))
        per_type_path = out_csv_path.replace(".csv", f"_{safe_t}.csv")
        sub.to_csv(per_type_path, index=False, encoding="utf-8-sig")
        print(f"[VALID] Log '{t}' zapisany: {per_type_path}")


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

def is_blank(s: str | None) -> bool:
    return s is None or str(s).strip() == ""

# =========================
# DB helpers
# =========================

def bulk_insert_oplacone_faktury(rows: list[tuple]):
    if not rows:
        return
    sql = """
    INSERT INTO "Oplacone_Faktury" (
        polecenie_zaplaty,
        data_platnosci,
        kwota_platnosci,
        nr_rozliczeniowy_banku_kontrahenta,
        tryb_realizacji_platnosci,
        nr_rachunku_zleceniodawcy,
        nr_rachunku_kontrahenta,
        nazwa_i_adres_zleceniodawcy,
        nazwa_i_adres_kontrahenta,
        oplaty_i_prowizje,
        pole_13,
        pole_14,
        klasyfikacja_polecenia,
        informacja_klient_bank,
        szczegoly_platnosci
    ) VALUES %s
    """
    with db_conn() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows)
        conn.commit()

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

def nr_konta_z_bazy(nip: str):
    nip_num = int(nip_digits(nip))
    rec = db_fetchone("SELECT nr_konta FROM Merchanci WHERE nip = %s", (nip_num,))
    if rec and rec.get("nr_konta"):
        return rec["nr_konta"]
    else:
        print(f"Brak nr konta w bazie dla nipu :{nip}")
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



# =========================
# Scraper REGON (Selenium)
# =========================


class RegonScraper:
    """Jedna przeglądarka na cały wsad."""
    def __init__(self, chromedriver_path: str = CHROMEDRIVER_PATH, headless: bool = True):
        self.chromedriver_path = chromedriver_path
        self.headless = headless
        self.driver = None

    def __enter__(self):
        options = Options()
        if self.headless:
            options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920x1080")
        service = Service(self.chromedriver_path)
        self.driver = webdriver.Chrome(service=service, options=options)
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.driver:
            self.driver.quit()

    def scrape_nip(self, nip: str) -> list[str]:
        d = self.driver
        d.get("https://wyszukiwarkaregon.stat.gov.pl/appBIR/index.aspx")
        losowe_opoznienie(0.05, 0.15)
        d.find_element(By.ID, "txtNip").clear()
        d.find_element(By.ID, "txtNip").send_keys(str(nip))
        d.find_element(By.ID, "btnSzukaj").click()
        losowe_opoznienie(0.15, 0.3)

        rows = d.find_elements(By.CLASS_NAME, "tabelaZbiorczaListaJednostekAltRow")
        if not rows:
            return []
        cells = rows[0].find_elements(By.TAG_NAME, "td")
        return [c.text.strip() for c in cells]

def wyciagnij_adres_z_komorek(cells: list[str]) -> str:
    if not cells:
        return ""
    start, end = 5, 9
    frag = cells[start:end] if len(cells) >= end else cells[max(0, len(cells)-4):]
    if not frag:
        return ""
    def clip35(s: str) -> str:
        return s[:35]
    return "|".join(clip35(sanitize_text(x)) for x in frag if x)

# =========================
# Get-or-fetch (DB → scrape → DB)
# =========================

def get_or_fetch_adres(nip_clean: str, scraper: "RegonScraper") -> str:
    try:
        adr = adres_z_bazy(nip_clean) or ""
        if not is_blank(adr):
            return adr
    except Exception as e:
        print(f"[W] Błąd DB przy pobieraniu adresu dla NIP {nip_clean}: {e}")

    try:
        cells = scraper.scrape_nip(nip_clean)
        losowe_opoznienie(0.05, 0.1)
        adr = wyciagnij_adres_z_komorek(cells)
        if not is_blank(adr):
            try:
                zapisz_adres_do_bazy(nip_clean, adr)
            except Exception as e:
                print(f"[W] Nie udało się zapisać adresu do DB dla NIP {nip_clean}: {e}")
        return adr or ""
    except Exception as e:
        print(f"[W] Błąd scrapera REGON dla NIP {nip_clean}: {e}")
        return ""

def get_or_fetch_konto(nip_clean: str) -> str:
    try:
        raw = nr_konta_z_bazy(nip_clean) or ""
        nrb = normalize_nrb(raw)
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
    klasyfikacja: str,
    informacja_klient_bank: str,
) -> str:
    fields = [
        "210",
        data_platnosci,
        str(kwota_brutto_gr),
        nr_rozliczeniowy_zleceniodawcy,
        tryb_realizacji,
        rachunek_zleceniodawcy,
        rachunek_kontrahenta,
        csv_quote(sanitize_text(nazwa_i_adres_zleceniodawcy)),  # 8
        csv_quote(sanitize_text(nazwa_i_adres_kontrahenta)),    # 9
        "0",
        nr_rozliczeniowy_banku_kontrahenta,
        csv_quote(sanitize_text(szczegoly_platnosci)),          # 12
        "",
        "",
        klasyfikacja,
        csv_quote(trim_to(sanitize_text(informacja_klient_bank), 19)),  # 16
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

    mdup = d.duplicated(subset=["__doc_no_norm", "__netto_gr", "__vat_gr", "__brut_gr"], keep="first")
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

def export_duplicates_report(df: pd.DataFrame, out_path: str):
    _, full_dups = find_duplicates(df)
    if full_dups.empty:
        print("[DUP] Brak duplikatów – raport nie został utworzony.")
        return
    cols = ["Numer dokumentu", "Netto", "VAT", "Brutto"]
    full_dups[cols].to_csv(out_path, index=False, encoding="utf-8")
    print(f"[DUP] Raport duplikatów zapisany: {out_path}")

def _group_key(row) -> str:
    """NIP (10 cyfr) albo fallback na nazwę kontrahenta."""
    nipc = nip_digits(row.get("NIP", ""))
    if len(nipc) == 10 and nipc.isdigit():
        return nipc
    name = str(row.get("Kontrahent", "")).strip().upper()
    return f"NAME::{name}"

def _safe_add30(s_min: str | None, s_max: str | None) -> str:
    base = s_max or s_min
    return add_days_to_date_str(base, 30) if base else datetime.now().strftime("%Y%m%d")

# ====================================
# GŁÓWNA FUNKCJA zapisująco - tworząca
# ====================================

def przetworz_plik_xlsx(
    input_file: str,
    *,
    company: str,
    output_path: Optional[str] = None,
    duplicates_action: str = "warn",
    headless: bool = True,
    merged_csv: Optional[str] = None,
    per_group_dir: Optional[str] = None,
    wl_check: bool = True
):
    # Lista błędów (w pamięci)
    error_log: list[dict] = []

    # --- walidacja spółki ---
    key = company.strip().lower()
    if key not in COMPANIES:
        raise ValueError(f"Nieznana firma: {company}. Dozwolone: {', '.join(sorted(COMPANIES))}")

    conf = COMPANIES[key]
    nazwa_i_adres_zleceniodawcy = conf["name_addr"]
    nr_rozliczeniowy_zleceniodawcy = conf["bank_code"]
    rachunek_zleceniodawcy = conf["nrb"]
    tryb_realizacji = "0"
    klasyfikacja = "01"

    # sanity check nadawcy
    if len(re.sub(r"\D", "", rachunek_zleceniodawcy)) != 26:
        raise ValueError(f"NRB nadawcy ma niepoprawną długość (26 cyfr): {rachunek_zleceniodawcy}")
    if not re.fullmatch(r"\d{8}", nr_rozliczeniowy_zleceniodawcy):
        raise ValueError(f"Kod rozliczeniowy nadawcy musi mieć 8 cyfr: {nr_rozliczeniowy_zleceniodawcy}")

    # --- wczytanie + duplikaty + kolumny ---
    df = pd.read_excel(input_file)
    df = handle_duplicates(df, action=duplicates_action)

    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    export_duplicates_report(df, os.path.join(OUTPUT_DIR, f"duplikaty_{ts}.csv"))

    wymagane_kolumny = {"Numer dokumentu", "Kontrahent", "NIP", "Data wpływu", "Brutto", "Netto", "VAT"}
    brak = wymagane_kolumny - set(df.columns)
    if brak:
        raise ValueError(f"Brak kolumn w pliku: {', '.join(sorted(brak))}")

    # WALIDACJA KOLUMN
    df, error_log = validate_df(
        df,
        date_col="Data wpływu",
        netto_col="Netto",
        vat_col="VAT",
        brutto_col="Brutto",
        tol=0.01,
        on_error="keep"
    )
    print(f"[VALID] Wykryto {len(error_log)} błędów. (trzymane w pamięci w zmiennej error_log)")
    print_error_summary(error_log, max_per_type=10)
    errors_csv = os.path.join(OUTPUT_DIR, f"errors_{ts}.csv")
    export_error_log(error_log, errors_csv)


    if df.empty:
        with open(output_path, "w", encoding=OUTPUT_ENCODING, newline="") as f:
            f.write("")
        print(f"[INFO] Po walidacji brak poprawnych wierszy. Zapisano pusty plik: {output_path}")
        return

    # --- przygotowanie do agregacji ---
    df = df.copy()

    # grosze – policz po walidacji (bo validate_df kasuje techniczne kolumny)
    df["__brutto_gr"] = df["Brutto"].apply(money_to_grosze)
    df["__vat_gr"] = df["VAT"].apply(money_to_grosze)
    df["__netto_gr"] = df["Netto"].apply(money_to_grosze)
    df["__data_str"] = df["Data wpływu"].apply(serializacja_dat)


    print("[DEBUG] Wiersze po walidacji:", len(df))
    print("[DEBUG] Suma brutto globalnie (PLN):", round(df["Brutto"].sum(), 2))
    print("[DEBUG] Suma brutto globalnie (gr):", int(df["__brutto_gr"].sum()))

    # surowy i oczyszczony NIP
    df["__nip_raw_str"] = df["NIP"].astype(str).str.strip()
    df["__nip_clean"] = df["__nip_raw_str"].str.replace(r"\D", "", regex=True)

    def _grp_key(row):
        dd = row["__nip_clean"]
        if len(dd) == 10 and dd.isdigit():
            return dd
        return f"NAME::{str(row['Kontrahent']).strip().upper()}"

    df["__grp_key"] = df.apply(_grp_key, axis=1)
    df["__doc_no_norm"] = df["Numer dokumentu"].astype(str).str.strip().str.replace(r"\s+", " ", regex=True).str.upper()

    # grosze – przydadzą się tylko do kontroli/raportów
    df["__brutto_gr"] = df["Brutto"].apply(money_to_grosze)
    df["__vat_gr"] = df["VAT"].apply(money_to_grosze)
    df["__netto_gr"] = df["Netto"].apply(money_to_grosze)

    print("[DEBUG] Wiersze po walidacji:", len(df))
    print("[DEBUG] Suma brutto globalnie (PLN):", round(df["Brutto"].sum(), 2))
    print("[DEBUG] Suma brutto globalnie (gr):", df["__brutto_gr"].sum())

    mask_no_date = df["__data_str"].isna()
    if mask_no_date.any():
        print(f"[WARN] Pomijam {int(mask_no_date.sum())} wierszy bez poprawnej daty 'Data wpływu'.")
    df_day = df.loc[~mask_no_date].copy()

    agg = (
        df_day.groupby(["__grp_key", "__data_str"], as_index=False)
        .agg(
            nip_clean=("__nip_clean", "first"),
            kontrahent=("Kontrahent", "first"),
            suma_brutto=("Brutto", "sum"),
            suma_vat=("VAT", "sum"),
            suma_netto=("Netto", "sum"),
            cnt_docs=("Numer dokumentu", "nunique"),
            cnt_rows=("Brutto", "size"),
            data_min=("__data_str", "min"),
            data_max=("__data_str", "max"),
        )
    )

    # grosze po sumowaniu w PLN
    agg["suma_brutto_gr"] = agg["suma_brutto"].apply(money_to_grosze)
    agg["suma_vat_gr"] = agg["suma_vat"].apply(money_to_grosze)
    agg["suma_netto_gr"] = agg["suma_netto"].apply(money_to_grosze)

    # sanity-check
    check_agg = int(agg["suma_brutto_gr"].sum())
    check_src = int(df_day["__brutto_gr"].sum())
    print("[CHECK] Łącznie po agregacji (gr):", check_agg)
    print("[CHECK] Łącznie bezpośrednio z pliku (gr):", check_src)

    # daty per-dzień
    def _safe_ddmmyy(s: str) -> str:
        return datetime.strptime(s, "%Y%m%d").strftime("%d%m%y")

    agg["data_platnosci"] = agg["__data_str"].map(lambda s: _safe_add30(s, s))
    agg["data_wplywu_ddmmyy"] = agg["__data_str"].map(_safe_ddmmyy)

    print("[DEBUG] Agg (kontrahent × dzień) – PLN:")
    print(agg[["__grp_key", "__data_str", "suma_brutto"]].head(10))

    # daty płatności / informacja do banku
    agg["data_platnosci"] = [
        _safe_add30(row["__data_str"], row["__data_str"]) for _, row in agg.iterrows()
    ]
    agg["data_wplywu_ddmmyy"] = [
        _safe_ddmmyy(row["__data_str"]) for _, row in agg.iterrows()
    ]

    # (opcjonalnie) szybki wgląd
    print("[DEBUG] Agg kontrahent (PLN):")
    print(agg[["__grp_key", "suma_brutto"]].head(10))

    # ---------- RAPORT ZBIORCZY ----------
    raport_wiersze = []
    for _, row in agg.iterrows():
        raport_wiersze.append({
            "grp_key": row["__grp_key"],
            "nip": row["nip_clean"] if (isinstance(row["nip_clean"], str) and row["nip_clean"].isdigit() and len(
                row["nip_clean"]) == 10) else "",
            "kontrahent": row["kontrahent"],
            "liczba_faktur": int(row["cnt_docs"]),
            "liczba_wierszy": int(row["cnt_rows"]),
            "suma_netto_pln": round(float(row["suma_netto"]), 2),
            "suma_vat_pln": round(float(row["suma_vat"]), 2),
            "suma_brutto_pln": round(float(row["suma_brutto"]), 2),
            "suma_netto_gr": int(row["suma_netto_gr"]),
            "suma_vat_gr": int(row["suma_vat_gr"]),
            "suma_brutto_gr": int(row["suma_brutto_gr"]),
            "data_min": row["data_min"],
            "data_max": row["data_max"],
        })

    pd.DataFrame(raport_wiersze).to_csv(merged_csv, index=False, encoding="utf-8-sig")
    print(f"[RAPORT] Zapisano zbiorczy raport scalonych grup: {merged_csv}")

    # ---------- PLIKI ELIXIR ----------
    adres_cache: dict[str, str] = {}
    konto_cache: dict[str, str] = {}
    lines: list[str] = []

    with RegonScraper(CHROMEDRIVER_PATH, headless=headless) as scraper:
        for _, row in agg.iterrows():
            nip_clean = str(row["nip_clean"] or "")
            valid = len(nip_clean) == 10 and nip_clean.isdigit()
            kontrahent_name = row["kontrahent"]
            kw_brutto_gr = int(row["suma_brutto_gr"])
            kw_vat_gr = int(row["suma_vat_gr"])
            cnt_docs = int(row["cnt_docs"])

            if valid:
                if nip_clean in adres_cache:
                    adres_kontr = adres_cache[nip_clean]
                else:
                    adres_kontr = get_or_fetch_adres(nip_clean, scraper)
                    adres_cache[nip_clean] = adres_kontr
                if nip_clean in konto_cache:
                    rachunek_kontrahenta = konto_cache[nip_clean]
                else:
                    rachunek_kontrahenta = get_or_fetch_konto(nip_clean) or "0" * 26
                    konto_cache[nip_clean] = rachunek_kontrahenta
            else:
                adres_kontr = kontrahent_name
                rachunek_kontrahenta = "0" * 26

            adres_kontr = clean_address(adres_kontr)
            nr_rozliczeniowy_banku_kontrahenta = bank_code_from_nrb(rachunek_kontrahenta)

            nip_for_ref = nip_clean if valid else "NA"
            data_wplywu = row["data_wplywu_ddmmyy"]
            informacja = trim_to(f"{nip_for_ref}{data_wplywu}", 19)

            szczegoly = f"/NIP/{nip_clean or 'NA'}|/CNT/{cnt_docs}|/VAT/{kw_vat_gr}|/AMT/{kw_brutto_gr}"

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
                klasyfikacja=klasyfikacja,
                informacja_klient_bank=informacja,
            )
            lines.append(line)

            # zapis raportu do pliku kwota Vat w groszach, kwota brutto w groszach, ilosć dokumentów, ne dokumentu
            base_dir = os.path.join("raporty", sanitize_nazwa_folderu(row["kontrahent"]))
            date_dir = os.path.join(base_dir, row["data_wplywu_ddmmyy"])
            os.makedirs(date_dir, exist_ok=True)


            doc_no = df_day.loc[
                (df_day["__grp_key"] == row["__grp_key"]) &
                (df_day["__data_str"] == row["__data_str"]),
                "Numer dokumentu"
            ].astype(str).iloc[0]

            zawartosc = f"{kw_vat_gr} {kw_brutto_gr} {cnt_docs} {doc_no}"

            # Nazwa pliku na podstawie informacji klient-bank
            filename = re.sub(r"[^0-9A-Za-z_.-]+", "_", informacja) + ".csv"

            # Ścieżka do pliku
            csv_path = os.path.join(date_dir, filename)

            # Zapisuj dane rekordu do pliku
            with open(csv_path, "w", encoding="utf-8", newline="") as f:
                f.write(zawartosc + "\n")  # pojedynczy rekord

    # zapis raportów z faktur do plików
    if not output_path:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        output_path = os.path.join(OUTPUT_DIR, f"{key}_przelewy_{ts}.txt")
    if merged_csv is None:
        merged_csv = os.path.join(OUTPUT_DIR, f"raport_scalonych_{ts}.csv")
    if per_group_dir:
        os.makedirs(per_group_dir, exist_ok=True)

    # --- zapis ELIXIR ---
    with open(output_path, "w", encoding=OUTPUT_ENCODING, newline="") as f:
        f.write(_latin_safe_join(lines))

    print(f"Zapisano {len(lines)} rekordów (po agregacji po kontrahencie) do: {output_path} (encoding={OUTPUT_ENCODING})")




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
    parser.add_argument("--wl-check", action=BooleanOptionalAction, default=True,
                        help="Sprawdzaj NIP/rachunek w białej liście MF (domyślnie: włączone)")

    args = parser.parse_args()

    przetworz_plik_xlsx(
        args.input,
        company=args.company,
        output_path=args.output,
        duplicates_action=args.dup,
        headless=args.headless,
        merged_csv=args.merged_csv,
        per_group_dir=args.per_group_dir,
        wl_check=args.wl_check,
    )