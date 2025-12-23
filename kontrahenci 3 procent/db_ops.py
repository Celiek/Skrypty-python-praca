import logging
import math
import os
import re
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from typing import Dict

import pandas as pd
import unicodedata
from fastapi import requests
from psycopg2.extras import execute_values

from utils import clean_nip, db_conn

# =====================================================
# CONFIG
# =====================================================

FAKTUROWNIA_API = os.getenv("FAKTUROWNIA_API", "https://shumee.fakturownia.pl")
FAKTUROWNIA_TOKEN = os.getenv("FAKTUROWNIA_TOKEN")

# =====================================================
# MERCHANCI
# =====================================================

def norm_amount(val) -> Decimal:
    if val is None:
        return Decimal("0.00")

    # pandas / numpy NaN
    if isinstance(val, float) and math.isnan(val):
        return Decimal("0.00")

    if pd.isna(val):
        return Decimal("0.00")

    # Decimal z bazy
    if isinstance(val, Decimal):
        return val.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    # string z XLSX
    s = str(val).strip()

    if s == "":
        return Decimal("0.00")

    # zamiana przecinka na kropkę (PL XLSX!)
    s = s.replace(",", ".")

    try:
        return Decimal(s).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except InvalidOperation:
        raise ValueError(f"❌ Nieprawidłowa kwota: {val!r}")

def reserve_commission_invoice(company: str, nip: str, okres: str) -> bool:
    """
    Rezerwuje prawo do wystawienia FV prowizyjnej dla (company, nip, okres).
    Zwraca True jeśli rezerwacja się udała (nie było wcześniej), False jeśli już istnieje.
    """
    nip_i = int(clean_nip(nip))
    sql = """
        INSERT INTO prowizje_fakturownia (nazwa_spolki, nip, okres)
        VALUES (%s, %s, %s)
        ON CONFLICT (nazwa_spolki, nip, okres) DO NOTHING
        RETURNING 1
    """
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (company, nip_i, okres))
        row = cur.fetchone()
        conn.commit()

    ok = row is not None
    if not ok:
        logging.warning(f"[IDEMPOTENCY] FV już istnieje/rezerwacja zajęta: spolka={company} nip={nip_i} okres={okres}")
    return ok

def finalize_commission_invoice(company: str, nip: str, okres: str, fakturownia_id: int, fakturownia_nr: str | None):
    """
    Po udanym wystawieniu – zapisuje ID/numer FV do rejestru.
    """
    nip_i = int(clean_nip(nip))
    sql = """
        UPDATE prowizje_fakturownia
        SET fakturownia_id = %s,
            fakturownia_nr = %s
        WHERE nazwa_spolki = %s
          AND nip = %s
          AND okres = %s
    """
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (int(fakturownia_id), fakturownia_nr, company, nip_i, okres))
        conn.commit()

def filter_new_source_invoices(df: pd.DataFrame, company: str) -> pd.DataFrame:
    """
    Zwraca TYLKO faktury z XLSX, których NIE MA w DB.
    Klucz: (nip, numer_faktury, data_wystawienia) dla danej spółki.
    """
    if df is None or df.empty:
        return df

    df = df.copy()

    # --- normalizacja wejścia (MUSI być identyczna jak przy zapisie do DB) ---
    df["NIP"] = df["NIP"].astype(str).map(clean_nip)
    df["Numer dokumentu"] = df["Numer dokumentu"].astype(str).str.strip()

    # data -> date (bez czasu)
    df["Data wystawienia"] = pd.to_datetime(df["Data wystawienia"], errors="coerce").dt.date

    # wywal śmieciowe rekordy, które i tak nie powinny iść dalej
    df = df[df["NIP"].notna() & (df["NIP"] != "") & df["Data wystawienia"].notna()].copy()

    before = len(df)
    if before == 0:
        logging.info("[DB-FILTER] wejście=0 (po czyszczeniu)")
        return df

    sql = """
        SELECT nip, numer_faktury, data_wystawienia
        FROM faktury_do_prowizji
        WHERE nazwa_spolki = %s
    """

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (company,))
        existing = {
            (str(nip).strip(), str(nr).strip(), data)
            for (nip, nr, data) in cur.fetchall()
        }

    # budujemy klucz po stronie DF i filtrujemy
    keys = list(zip(df["NIP"].astype(str), df["Numer dokumentu"], df["Data wystawienia"]))
    mask = [k not in existing for k in keys]

    new_df = df.loc[mask].reset_index(drop=True)

    logging.info(
        f"[DB-FILTER] wejście={before} | już_w_DB={before - len(new_df)} | nowe={len(new_df)}"
    )
    return new_df

def get_addresses_from_db() -> Dict[str, str]:
    """Zwraca mapę {NIP: adres}."""
    sql = """
        SELECT nip, adres
        FROM merchanci
        WHERE nip IS NOT NULL AND adres IS NOT NULL
    """

    result = {}

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        for row in cur.fetchall():   # ✅ dict
            nip = row["nip"]
            addr = row["adres"]
            result[str(nip).strip()] = str(addr).strip()


    logging.info(f"[DB] Załadowano adresy: {len(result)}")
    return result

def get_names_from_db_for_nips(nips: list[str | int]) -> dict[str, str]:
    if not nips:
        return {}

    nips_int = [int(clean_nip(n)) for n in nips if clean_nip(n)]

    sql = """
        SELECT nip, nazwa
        FROM merchanci
        WHERE nip = ANY(%s::bigint[])
          AND nazwa IS NOT NULL
    """

    def _clean(name: str) -> str:
        t = unicodedata.normalize("NFKC", name)
        t = re.sub(r'[\-\u2010-\u2015]', ' ', t)
        return re.sub(r'\s+', ' ', t).strip()

    result = {}

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (nips_int,))
        for nip, name in cur.fetchall():
            result[str(nip)] = _clean(name)

    logging.info(f"[DB] Nazwy kontrahentów: {len(result)}")
    return result

# =====================================================
# FAKTURY ŹRÓDŁOWE (KLUCZOWE)
# =====================================================

def save_source_invoices(df: pd.DataFrame, company: str) -> tuple[int, int]:
    """
    Zapisuje WYŁĄCZNIE faktury źródłowe, które weszły do prowizji (df_new/df_for_reports).
    Liczy i loguje ile realnie wstawiono oraz ile pominięto (bo konflikt/duplikat).
    Zwraca: (inserted, skipped)
    """
    if df.empty:
        logging.info("[DB] Brak faktur prowizyjnych do zapisu.")
        return 0, 0

    required = {"NIP", "Numer dokumentu", "Data wystawienia", "Netto", "VAT", "Brutto"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"[DB] Brakuje kolumn: {', '.join(sorted(missing))}")

    records = []
    for _, row in df.iterrows():
        nip = clean_nip(row["NIP"])
        numer = str(row["Numer dokumentu"]).strip()
        data = pd.to_datetime(row["Data wystawienia"], errors="coerce")

        if not nip or not numer or pd.isna(data):
            continue

        records.append((
            nip,
            numer,
            data.date(),
            norm_amount(row["Netto"]),
            norm_amount(row["VAT"]),
            norm_amount(row["Brutto"]),
            company,
        ))

    if not records:
        logging.info("[DB] Po walidacji brak rekordów.")
        return 0, 0

    sql = """
        INSERT INTO faktury_do_prowizji (
            nip, numer_faktury, data_wystawienia,
            kwota_netto, kwota_vat, kwota_brutto,
            nazwa_spolki
        )
        VALUES %s
        ON CONFLICT (nip, numer_faktury, data_wystawienia, nazwa_spolki)
        DO NOTHING
        RETURNING 1
    """

    inserted = 0
    with db_conn() as conn, conn.cursor() as cur:
        # execute_values robi jeden INSERT na batch i wspiera RETURNING
        page_size = 500
        for i in range(0, len(records), page_size):
            chunk = records[i:i + page_size]
            execute_values(cur, sql, chunk, page_size=len(chunk))
            rows = cur.fetchall()  # z RETURNING 1
            inserted += len(rows)

        conn.commit()

    skipped = len(records) - inserted
    logging.info(f"[DB] Zapisano faktury źródłowe: {inserted}")
    logging.info(f"[DB] Pominięto (już w bazie / konflikt): {skipped}")

    return inserted, skipped

def mark_as_used_by_ids(
        source_ids: list[int],
        fakturownia_id: int,
        fakturownia_numer: str | None = None) -> int:
    if not source_ids:
        return 0

    sql = """
        UPDATE faktury_do_prowizji
        SET id_fakturowni = %s,
            fakturownia_numer = %s
        WHERE id_faktury = ANY(%s)
    """

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (int(fakturownia_id), fakturownia_numer, source_ids))
        updated = cur.rowcount
        conn.commit()

    logging.info(f"[DB] Oznaczono {updated} faktur (id_fakturowni={fakturownia_id}, nr={fakturownia_numer})")
    return updated

# =====================================================
# FAKTUROWNIA API
# =====================================================

NIP_RE = re.compile(r"\d+")

def _clean_nip_to_int(val) -> int | None:
    """
    Zwraca int NIP jeśli da się bezpiecznie wyciągnąć 10 cyfr.
    W przeciwnym razie None.
    """
    if val is None:
        return None

    s = str(val).strip()
    if not s:
        return None

    s = s.replace("PL", "").replace("pl", "").strip()
    digits = "".join(NIP_RE.findall(s))  # zostaw tylko cyfry

    # typowo NIP ma 10 cyfr (jeśli u Ciebie bywają inne, poluzuj warunek)
    if len(digits) != 10:
        return None

    try:
        return int(digits)
    except ValueError:
        return None

def get_source_ids_for_df(df_new: pd.DataFrame, company: str) -> dict[int, list[int]]:
    """
    Zwraca mapę:
        { nip_int: [id_faktury, ...] }
    dla faktur źródłowych zapisanych wcześniej do DB.
    ODPORNA na tuple / dict cursor.
    """

    if df_new is None or df_new.empty:
        logging.info("[DB] df_new puste – brak source IDs.")
        return {}

    nips = (
        df_new["NIP"]
        .astype(str)
        .map(clean_nip)
        .dropna()
        .unique()
        .tolist()
    )

    if not nips:
        logging.warning("[DB] Brak poprawnych NIP-ów w df_new.")
        return {}

    nips_int = [int(n) for n in nips]

    sql = """
        SELECT id_faktury, nip
        FROM faktury_do_prowizji
        WHERE nazwa_spolki = %s
          AND nip = ANY(%s::bigint[])
          AND id_fakturowni IS NULL
          AND nip IS NOT NULL
    """

    out: dict[int, list[int]] = {}

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (company, nips_int))
        rows = cur.fetchall()

    for row in rows:
        try:
            # ✅ obsługa dict (RealDictCursor)
            if isinstance(row, dict):
                id_faktury = row.get("id_faktury")
                nip = row.get("nip")
            # ✅ obsługa tuple
            else:
                id_faktury, nip = row

            if id_faktury is None or nip is None:
                continue

            nip_int = int(str(nip).strip())
            out.setdefault(nip_int, []).append(int(id_faktury))

        except Exception:
            logging.warning(
                f"[DB] Nieprawidłowy rekord w DB: {row!r}"
            )

    logging.info(
        f"[DB] get_source_ids_for_df: {sum(len(v) for v in out.values())} ID faktur"
    )
    return out

def get_invoice_details(invoice_id: int) -> dict:
    """Pobiera szczegóły faktury z API Fakturowni."""
    try:
        url = f"{FAKTUROWNIA_API}/invoices/{invoice_id}.json?api_token={FAKTUROWNIA_TOKEN}"
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logging.warning(f"[FAKTUROWNIA] Nie udało się pobrać szczegółów faktury {invoice_id}: {e}")
        return {}