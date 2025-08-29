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

# TODO
# BIAŁA LIST PŁĄTNIKÓW VAT nr kont, nipy poprawność

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

def _latin_safe(s: str) -> str:
    """Bezpieczne rzutowanie do OUTPUT_ENCODING; spoza zakresu → '?'."""
    return s.encode(OUTPUT_ENCODING, errors="replace").decode(OUTPUT_ENCODING)

def _latin_safe_join(lines: list[str]) -> str:
    return "\n".join(_latin_safe(line) for line in lines)

# ===========================================
# Utils
# ===========================================

def losowe_opoznienie(min_sec=0.05, max_sec=0.1):
    time.sleep(random.uniform(min_sec, max_sec))

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

def _safe_serializacja(x) -> str:
    try:
        return serializacja_dat(x)
    except Exception:
        return datetime.now().strftime("%Y%m%d")

def _group_key(row) -> str:
    """NIP (10 cyfr) albo fallback na nazwę kontrahenta."""
    nipc = nip_digits(row.get("NIP", ""))
    if len(nipc) == 10 and nipc.isdigit():
        return nipc
    name = str(row.get("Kontrahent", "")).strip().upper()
    return f"NAME::{name}"

# =========================
# GŁÓWNA FUNKCJA
# =========================

def przetworz_plik_xlsx(
    input_file: str,
    *,
    company: str,
    output_path: Optional[str] = None,
    duplicates_action: str = "warn",
    headless: bool = True,
    merged_csv: Optional[str] = None,
    per_group_dir: Optional[str] = None,
):
    # walidacja spółki
    key = company.strip().lower()
    if key not in COMPANIES:
        raise ValueError(f"Nieznana firma: {company}. Dozwolone: {', '.join(sorted(COMPANIES))}")

    conf = COMPANIES[key]
    nazwa_i_adres_zleceniodawcy = conf["name_addr"]
    nr_rozliczeniowy_zleceniodawcy = conf["bank_code"]
    rachunek_zleceniodawcy = conf["nrb"]
    tryb_realizacji = "0"
    klasyfikacja = "01"

    # sanity check danych firmy
    if len(re.sub(r"\D", "", rachunek_zleceniodawcy)) != 26:
        raise ValueError(f"NRB nadawcy ma niepoprawną długość (26 cyfr): {rachunek_zleceniodawcy}")
    if not re.fullmatch(r"\d{8}", nr_rozliczeniowy_zleceniodawcy):
        raise ValueError(f"Kod rozliczeniowy nadawcy musi mieć 8 cyfr: {nr_rozliczeniowy_zleceniodawcy}")

    # przygotowanie ścieżek wyjścia
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    if not output_path:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        output_path = os.path.join(OUTPUT_DIR, f"{key}_przelewy_{ts}.txt")
    if merged_csv is None:
        merged_csv = os.path.join(OUTPUT_DIR, f"raport_scalonych_{ts}.csv")
    if per_group_dir:
        os.makedirs(per_group_dir, exist_ok=True)

    # wczytanie danych + obsługa duplikatów + walidacja kolumn
    df = pd.read_excel(input_file)
    df = handle_duplicates(df, action=duplicates_action)
    export_duplicates_report(df, os.path.join(OUTPUT_DIR, f"duplikaty_{ts}.csv"))

    wymagane_kolumny = {"Numer dokumentu", "Kontrahent", "NIP", "Data wpływu", "Brutto", "Netto", "VAT"}
    brak = wymagane_kolumny - set(df.columns)
    if brak:
        raise ValueError(f"Brak kolumn w pliku: {', '.join(sorted(brak))}")

    # przygotowanie do agregacji
    df = df.copy()
    df["__nip_clean"] = df["NIP"].map(nip_digits)
    df["__is_valid_nip"] = df["__nip_clean"].map(lambda x: len(x) == 10 and x.isdigit())
    df["__grp_key"] = df.apply(_group_key, axis=1)
    df["__brutto_gr"] = df["Brutto"].apply(money_to_grosze)
    df["__vat_gr"] = df["VAT"].apply(money_to_grosze)
    df["__data_str"] = df["Data wpływu"].map(_safe_serializacja)  # YYYYMMDD string

    # --- AGREGACJA: tylko po kontrahencie (bez dnia) ---
    dates = (
        df.groupby("__grp_key")["__data_str"]
          .agg(["min", "max"])
          .rename(columns={"min": "data_min", "max": "data_max"})
          .reset_index()
    )

    agg_core = (
        df.groupby("__grp_key", as_index=False)
          .agg(
              nip_clean=("__nip_clean", "first"),
              kontrahent=("Kontrahent", lambda s: str(s.iloc[0])),
              suma_brutto=("__brutto_gr", "sum"),
              suma_vat=("__vat_gr", "sum"),
              cnt=("Numer dokumentu", "count"),
              first_doc=("Numer dokumentu", lambda s: str(s.iloc[0]).strip()),
          )
    )

    agg = agg_core.merge(dates, on="__grp_key", how="left")

    def _safe_add30(s_min: str | None, s_max: str | None) -> str:
        base = s_max or s_min
        return add_days_to_date_str(base, 30) if base else datetime.now().strftime("%Y%m%d")

    agg["data_platnosci"] = [
        _safe_add30(r.data_min, r.data_max) for r in agg.itertuples(index=False)
    ]
    agg["data_wplywu_ddmmyy"] = [
        datetime.strptime((r.data_max or r.data_min), "%Y%m%d").strftime("%d%m%y")
        for r in agg.itertuples(index=False)
    ]

    adres_cache: dict[str, str] = {}
    konto_cache: dict[str, str] = {}
    lines: list[str] = []

    def _safe_name(s: str) -> str:
        s = str(s or "").strip()
        return re.sub(r"[^0-9A-Za-z_.-]+", "_", s)[:80]

    # ---------- RAPORTY CSV ----------
    raport_wiersze = []

    for _, row in agg.iterrows():
        grp_key = row["__grp_key"]
        nip_clean = str(row["nip_clean"] or "")
        valid_nip = len(nip_clean) == 10 and nip_clean.isdigit()
        kontrahent_name = str(row["kontrahent"])
        cnt_docs = int(row["cnt"])
        suma_brutto = int(row["suma_brutto"])
        suma_vat = int(row["suma_vat"])

        # wszystkie wiersze tego kontrahenta (niezależnie od daty)
        sub = df.loc[df["__grp_key"] == grp_key].copy()

        # (opcjonalnie) zapis CSV per kontrahent/per data
        if per_group_dir:
            kontrahent_poprawna_nazwa = sanitize_nazwa_folderu(kontrahent_name)
            kontrahent_dir = os.path.join(per_group_dir, kontrahent_poprawna_nazwa)
            os.makedirs(kontrahent_dir, exist_ok=True)

            for data_str in sorted(sub["__data_str"].unique()):
                date_dir = os.path.join(kontrahent_dir, data_str)
                os.makedirs(date_dir, exist_ok=True)

                cols = [c for c in ["Numer dokumentu", "Netto", "VAT", "Brutto", "Opis"] if c in sub.columns]
                sub_out = sub.loc[sub["__data_str"] == data_str, cols].copy()

                if valid_nip:
                    fname = f"nip_{nip_clean}_{data_str}.csv"
                else:
                    fname = f"name_{_safe_name(kontrahent_poprawna_nazwa)}_{data_str}.csv"

                out_path = os.path.join(date_dir, fname)
                sub_out.to_csv(out_path, index=False, encoding="utf-8-sig")
                print(f"[RAPORT] Zapisano plik: {out_path}")

        # raport zbiorczy (jedna linia na kontrahenta)
        doc_list = [str(x).strip() for x in sub["Numer dokumentu"].tolist() if str(x).strip()]
        docs_joined = " | ".join(doc_list)
        raport_wiersze.append({
            "grp_key": grp_key,
            "nip": nip_clean if valid_nip else "",
            "kontrahent": kontrahent_name,
            "liczba_faktur": cnt_docs,
            "suma_brutto_gr": suma_brutto,
            "suma_vat_gr": suma_vat,
            "faktury": docs_joined,
        })

    pd.DataFrame(raport_wiersze).to_csv(merged_csv, index=False, encoding="utf-8-sig")
    print(f"[RAPORT] Zapisano zbiorczy raport scalonych grup: {merged_csv}")
    if per_group_dir:
        print(f"[RAPORT] Osobne CSV w katalogu: {per_group_dir}")

    # ---------- PLIKI ELIXIR ----------
    with RegonScraper(CHROMEDRIVER_PATH, headless=headless) as scraper:
        for _, row in agg.iterrows():
            nip_clean = str(row["nip_clean"] or "")
            valid = len(nip_clean) == 10 and nip_clean.isdigit()
            kontrahent_name = row["kontrahent"]
            kw_brutto_gr = int(row["suma_brutto"])
            kw_vat_gr = int(row["suma_vat"])
            cnt_docs = int(row["cnt"])

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
            data_wplywu = row["data_wplywu_ddmmyy"]  # ddmmyy z data_max (lub data_min)
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

    # zapis ELIXIR
    for i, line in enumerate(lines, start=1):
        try:
            line.encode(OUTPUT_ENCODING, errors="strict")
        except UnicodeEncodeError as e:
            bad = line[e.start:e.end]
            print(f"[ENC] Linia {i}: niekodowalne znaki {repr(bad)} → zostaną zastąpione '?'")

    payload = _latin_safe_join(lines)
    with open(output_path, "w", encoding=OUTPUT_ENCODING, newline="") as f:
        f.write(payload)

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

    args = parser.parse_args()

    przetworz_plik_xlsx(
        args.input,
        company=args.company,
        output_path=args.output,
        duplicates_action=args.dup,
        headless=args.headless,
        merged_csv=args.merged_csv,
        per_group_dir=args.per_group_dir,
    )
