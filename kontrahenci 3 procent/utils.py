import os
import re
import unicodedata
import psycopg2
from psycopg2.extras import RealDictCursor

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