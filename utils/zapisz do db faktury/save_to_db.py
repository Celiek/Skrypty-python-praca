from decimal import ROUND_HALF_UP, Decimal
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import re
import logging
from dotenv import load_dotenv
import os

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

from contextlib import contextmanager

@contextmanager
def db_conn():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        yield conn
    finally:
        conn.close()


# ============================================================
# Normalizacja numerów faktur
# ============================================================

def _norm_doc_no(x: str) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip()
    s = re.sub(r"\s+", " ", s)
    s = s.upper()
    return s


# ============================================================
# Operacje na kwotach → konwersja do groszy
# ============================================================

def money_to_grosze(value) -> int:
    if pd.isna(value):
        return 0
    d = Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return int((d * 100).to_integral_value())


def _money_to_gr_series(s: pd.Series) -> pd.Series:
    return s.apply(money_to_grosze)


# ============================================================
# Wyszukiwanie duplikatów w dokumencie
# ============================================================

def find_duplicates(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    required = {"Numer dokumentu", "Netto", "VAT", "Brutto"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Brak kolumn: {', '.join(sorted(missing))}")

    d = df.copy()
    d["__doc_no_norm"] = d["Numer dokumentu"].map(_norm_doc_no)
    d["__netto_gr"] = _money_to_gr_series(d["Netto"])
    d["__vat_gr"]   = _money_to_gr_series(d["VAT"])
    d["__brutto_gr"]  = _money_to_gr_series(d["Brutto"])

    group_sizes = d.groupby(
        ["__doc_no_norm", "__netto_gr", "__vat_gr", "__brutto_gr"]
    )["Numer dokumentu"].transform("size")

    d["__is_dup_group"] = group_sizes > 1
    full_dup_groups = d.loc[d["__is_dup_group"]].copy()

    return d, full_dup_groups.sort_values(
        ["__doc_no_norm", "__netto_gr", "__vat_gr", "__brutto_gr"]
    )


def handle_duplicates(df: pd.DataFrame, action: str = "error") -> pd.DataFrame:
    d, full_dups = find_duplicates(df)

    # Jeżeli nie ma duplikatów → ZWRACAMY D (bo zawiera przeliczone kolumny!)
    if full_dups.empty:
        return d

    preview_cols  = ["Numer dokumentu", "Netto", "VAT", "Brutto"]
    print("[DUP] Wykryto duplikaty:\n",
          full_dups[preview_cols].to_string(index=False))

    if action == "error":
        raise ValueError("W pliku znajdują się duplikaty.")
    elif action == "warn":
        return d

    elif action in ("drop_keep_first", "drop_keep_last"):
        keep = "first" if action == "drop_keep_first" else "last"
        mask = d.duplicated(subset=["__doc_no_norm"], keep=keep)
        cleaned = d.loc[~mask].copy()
        print(f"[DUP] Usunięto {mask.sum()} duplikatów ({action}).")
        return cleaned

    else:
        raise ValueError(f"Nieznane action='{action}'")


# ============================================================
# Zapis danych do bazy
# ============================================================

def zapisz_faktury_do_bazy(df_to_db: pd.DataFrame, spolka: str):

    if df_to_db.empty:
        logging.info("[DB] Brak danych do zapisania.")
        return

    df_to_db = df_to_db.copy()

    # czysty NIP
    df_to_db["__nip_clean"] = df_to_db["NIP"].astype(str).str.replace(r"\D", "", regex=True)

    print("[DEBUG] nazwy kolumn:")
    print(df_to_db.columns)

    with db_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:

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
            data_wystawienia = pd.to_datetime(
                row["Data wystawienia"], dayfirst=True, errors="coerce"
            ).date()

            kw_netto  = Decimal(row["__netto_gr"]) / 100
            kw_vat    = Decimal(row["__vat_gr"]) / 100
            kw_brutto = Decimal(row["__brutto_gr"]) / 100

            nip = row["__nip_clean"]

            # Pobierz kontrahenta
            cur.execute("SELECT id FROM merchanci WHERE nip = %s", (nip,))
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

                existing.add((id_kontrahenta, numer_faktury))
                inserted += 1

            except psycopg2.Error as e:
                skipped += 1
                logging.error(
                    f"[DB] BŁĄD zapisu faktury {numer_faktury} (NIP={nip}): {e.pgerror}"
                )
                conn.rollback()        # anuluj sam INSERT
                continue

            existing.add(key)
            inserted += 1

        conn.commit()

        logging.info(
            f"[DB] Zapisano {inserted} faktur, pominięto {skipped} duplikatów."
        )


# ============================================================
# MAIN
# ============================================================

def main():
    file = r"C:\Users\DELL\Documents\Skrypty\Skrypty-python-praca\skrypt tworzenie pliku do banku\zakup test great 15.12.xlsx"
    df = pd.read_excel(file)

    df = handle_duplicates(df, action="drop_keep_first")

    # Walidacja kolumn
    wymagane = {
        "Numer dokumentu", "Kontrahent", "NIP",
        "Data wpływu", "Brutto", "Netto", "VAT", "Data wystawienia"
    }
    brak = wymagane - set(df.columns)
    if brak:
        raise ValueError(f"Brak kolumn: {', '.join(sorted(brak))}")

    zapisz_faktury_do_bazy(df, "greatstore")
    print("zapisane")


main()
