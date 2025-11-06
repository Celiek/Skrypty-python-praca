import os

import pandas as pd
import psycopg2
import unicodedata
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor
import re
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


_WINDOWS_FORBIDDEN = set('<>:"/\\|?*')
_WINDOWS_RESERVED = {
    "CON", "PRN", "AUX", "NUL",
    *(f"COM{i}" for i in range(1, 10)),
    *(f"LPT{i}" for i in range(1, 10)),
}

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

def db_conn():
    return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)

def _only_digits(s: str) -> str:
    return re.sub(r"\D", "", str(s or "")).strip()

def _slugify_filename(s: str, *, max_len: int = 60) -> str:
    if not s:
        return "plik"
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^A-Za-z0-9_.\- ]", "_", s).strip()
    s = re.sub(r"[ _]+", "_", s)
    if s.upper() in _WINDOWS_RESERVED:
        s = f"_{s}"
    return s[:max_len].rstrip("._") or "plik"

def _safe_name(name: str) -> str:
    name = (name or "").strip() or "plik"
    return re.sub(r'[<>:"/\\|?*]+', "_", name).strip(" .")[:150] or "plik"


def clean_nip(nip_raw: str) -> str:
    """
    Normalizuje format NIP:
    - usuwa prefiksy PL, spacje, myślniki, kropki,
    - konwertuje na 10-cyfrowy ciąg,
    - usuwa końcówkę .0 (z Excela),
    - dodaje wiodące zera jeśli długość < 10.
    """
    if not nip_raw:
        print("[DEBUG] BRAK NIPÓW")
        return ""

    nip = str(nip_raw).strip().upper()
    nip = nip.replace("PL", "")
    nip = nip.replace(" ", "").replace("-", "").replace(".", "")
    nip = re.sub(r"[^\d]", "", nip)  # zostaw tylko cyfry

    # usuń końcówkę .0 (np. "1234567890.0")
    if nip.endswith("0") and "." in str(nip_raw):
        nip = nip.split(".")[0]

    # dopasuj długość do 10 znaków (np. "12345678" -> "0012345678")
    if len(nip) < 10 and nip.isdigit():
        nip = nip.zfill(10)

    # obetnij do 10 cyfr (czasem Excel zapisze coś w stylu "1234567890123")
    if len(nip) > 10:
        nip = nip[:10]

    return nip

def clean_address(raw_addr: str) -> str:
    """
    Normalizuje adresy kontrahentów:
    - usuwa wielokrotne separatory '|' i '-'
    - usuwa podwójne spacje, taby i znaki specjalne
    - zachowuje polskie znaki
    - usuwa leading/trailing spacje
    """
    if not raw_addr:
        return ""

    addr = str(raw_addr).strip()

    # usuń powtórzenia separatorów
    addr = re.sub(r"[|]+", ", ", addr)
    addr = re.sub(r"[-]{2,}", "-", addr)

    # usuń niepotrzebne znaki nowej linii, taby itp.
    addr = re.sub(r"[\r\n\t]+", " ", addr)

    # usuń powtórzenia przecinków / spacji
    addr = re.sub(r"\s{2,}", " ", addr)
    addr = re.sub(r",\s*,", ",", addr)

    # usuń zbędne znaki na końcu
    addr = addr.strip(" ,;-")

    # zamiana na Unicode NFC (żeby polskie znaki były jednolite)
    addr = unicodedata.normalize("NFC", addr)

    # limit długości (Fakturownia ma ograniczenia)
    return addr[:250]


def clean_df(df : pd.DataFrame) -> pd.DataFrame:
    # usuwa duplikaty z dataframe i je loguje
    # na wejściu jest dataframe
    # funkcja zwraca Datafram

    df["NIP_clean"] = df["NIP"].apply(clean_nip)

    subset_cols = ["Data wystawienia", "Numer dokumentu", "NIP_clean", "Netto", "VAT", "Brutto"]

    # Znajdź duplikaty
    dupes = df[df.duplicated(subset=subset_cols, keep=False)]

    if not dupes.empty:
        logging.warning(f"[DUPLIKATY] Wykryto {len(dupes)} rekordów powtarzających się.")
        for _, row in dupes.iterrows():
            logging.warning(
                f"[DUPLIKAT] Faktura: {row['Numer dokumentu']} | "
                f"NIP: {row['NIP_clean']} | "
                f"Data: {row['Data wystawienia']} | Netto: {row['Netto']} | Brutto: {row['Brutto']}"
            )
    else:
        logging.info("[DUPLIKATY] Brak powtórzonych faktur.")

    # Usuń duplikaty (zostaw pierwsze wystąpienie)
    df = df.drop_duplicates(subset=subset_cols, keep="first")
    return df