import logging
import os
import re
from decimal import Decimal

import pandas as pd
import requests
import unicodedata
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor
from sqlalchemy.dialects.postgresql import psycopg2

from utils import db_conn

load_dotenv()

def get_addresses_from_db() -> dict[str, str]:
    """
    Pobiera NIP i adres z tabeli 'merchanci' i zwraca słownik {NIP: adres}.
    Działa niezależnie od typu kursora (tuple lub dict).
    """
    result = {}
    query = """
    SELECT nip, adres
    FROM merchanci
    WHERE nip IS NOT NULL
      AND adres IS NOT NULL;
    """

    # otwieramy połączenie do bazy
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            print(f"[DEBUG] Pobranie {len(rows)} wierszy z merchanci.")

            # sprawdzamy typ pierwszego rekordu (tuple vs dict)
            if len(rows) > 0:
                sample = rows[0]
                if isinstance(sample, dict):
                    # jeśli RealDictCursor — klucze
                    for row in rows:
                        nip_raw = row["nip"]
                        addr_raw = row["adres"]
                        if nip_raw and addr_raw:
                            result[str(nip_raw).strip()] = str(addr_raw).strip()
                else:
                    # jeśli zwykły cursor — indeksy
                    for row in rows:
                        nip_raw = row[0]
                        addr_raw = row[1]
                        if nip_raw and addr_raw:
                            result[str(nip_raw).strip()] = str(addr_raw).strip()

    print(f"[DEBUG] Utworzono mapę adresów: {len(result)} rekordów.")
    return result

def insert_new_invoices_from_xlsx(xlsx_path: str, company: str):
    """
    Wstawia do tabeli 'faktury_do_prowizji' rekordy z XLSX, których numer_faktury nie istnieje w bazie.
    Wymagane kolumny: Numer dokumentu, Data wystawienia, Netto, VAT, Brutto.
    """
    df_to_db = pd.read_excel(xlsx_path)
    spolka =company

    if df_to_db.empty:
        logging.info("[DB] Brak danych do zapisania.")
        return

    df_to_db = df_to_db.copy()

    # --- 1. Walidacja kolumn (ZANIM dotkniemy NIP) ---
    required = {
        "Numer dokumentu", "Data wystawienia",
        "Netto", "VAT", "Brutto",
        "NIP",
        "__netto_gr", "__vat_gr", "__brutto_gr"
    }
    missing = required - set(df_to_db.columns)
    if missing:
        raise ValueError(f"[DB] Brakuje kolumn: {', '.join(sorted(missing))}")

    # --- 2. Normalizacja NIP ---
    df_to_db["__nip_clean"] = df_to_db["NIP"].astype(str).str.replace(r"\D", "", regex=True)

    # --- 3. Połączenie ---
    with db_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:

        # Pobierz obecne rekordy z bazy, aby szybko sprawdzać duplikaty
        cur.execute("""
                SELECT nip, numer_faktury, nazwa_spolki,
                       kwota_netto, kwota_vat, kwota_brutto
                FROM faktury_prowizje
            """)

        existing = {
            (row["nip"], row["numer_faktury"], row["nazwa_spolki"]):
                (float(row["kwota_netto"]), float(row["kwota_vat"]), float(row["kwota_brutto"]))
            for row in cur.fetchall()
        }

        inserted = 0
        skipped = 0

        # --- 4. Iteracja przez rekordy ---
        for _, row in df_to_db.iterrows():

            numer = str(row["Numer dokumentu"]).strip()
            nip = row["__nip_clean"]

            if not numer or not nip:
                skipped += 1
                continue

            key = (nip, numer, spolka)

            kw_netto = float(Decimal(row["__netto_gr"]) / 100)
            kw_vat = float(Decimal(row["__vat_gr"]) / 100)
            kw_brutto = float(Decimal(row["__brutto_gr"]) / 100)

            # --- 4A. Duplikat już istnieje ---
            if key in existing:
                old_netto, old_vat, old_brutto = existing[key]

                # Konflikt kwot
                if (old_netto, old_vat, old_brutto) != (kw_netto, kw_vat, kw_brutto):
                    logging.error(
                        f"[DB] KONFLIKT KWOT: FV {numer} NIP={nip} SP={spolka}  "
                        f"w bazie: {old_netto}/{old_vat}/{old_brutto}, "
                        f"w pliku: {kw_netto}/{kw_vat}/{kw_brutto}"
                    )

                skipped += 1
                continue

            # --- 4B. Próba INSERT ---
            try:
                cur.execute("""
                        INSERT INTO faktury_prowizje (
                            numer_faktury, data_wystawienia,
                            kwota_netto, kwota_vat, kwota_brutto,
                            nip, kontrahent, nazwa_spolki
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (nip, numer_faktury, nazwa_spolki) DO NOTHING
                    """, (
                    numer,
                    pd.to_datetime(row["Data wystawienia"], errors="coerce").date(),
                    kw_netto, kw_vat, kw_brutto,
                    nip,
                    row.get("Kontrahent", ""),
                    spolka
                ))

                # Dodajemy do pamięci — żeby kolejne wiersze nie wstawiały duplikatów
                existing[key] = (kw_netto, kw_vat, kw_brutto)
                inserted += 1

            except psycopg2.Error as e:
                logging.error(f"[DB] Błąd INSERT FV={numer} NIP={nip}: {e.pgerror}")
                conn.rollback()
                skipped += 1
                continue

        conn.commit()

        logging.info(
            f"[DB] Wynik zapisu: {inserted} dodanych, {skipped} pominiętych."
        )


def zapisz_powiazania_do_bazy(df, wyniki, company):
    """
    Tworzy powiązania między fakturami cząstkowymi i zbiorczymi.
    Szuka lokalnego ID faktury zbiorczej (na podstawie numeru faktury z Fakturowni).

    Tabela 'faktura_powiazania' musi mieć kolumny:
    - id_faktury_zbiorczej (BIGINT, FK do faktury)
    - id_faktury_skladnikowej (BIGINT, FK do faktury)
    - nr_faktury (VARCHAR)
    """
    if not wyniki:
        logging.warning("[POWIAZANIA] Brak wystawionych faktur zbiorczych — pomijam zapis.")
        return

    with db_conn() as conn:
        with conn.cursor() as cur:
            dodane, pominiete = 0, 0

            for w in wyniki:
                if not w.get("ok"):
                    continue

                logging.debug(f"[DEBUG] Dane z API Fakturowni dla faktury zbiorczej: {w}")

                nr_faktury_zbiorczej = (
                        w.get("number")
                        or w.get("invoice_number")
                        or w.get("full_number")
                        or w.get("name")
                        or f"FAKTURA-{w.get('id')}"
                )

                nr_faktury_zbiorczej = str(nr_faktury_zbiorczej).strip()

                nip = str(w.get("nip", "")).replace("PL", "").strip()

                # 🔹 wyszukaj id faktury zbiorczej w lokalnej tabeli
                cur.execute("""
                    SELECT id_faktury FROM faktury WHERE numer_faktury = %s
                """, (nr_faktury_zbiorczej,))
                res_zbiorcza = cur.fetchone()
                id_faktury_zbiorczej = res_zbiorcza["id_faktury"] if res_zbiorcza else None

                if not id_faktury_zbiorczej:
                    logging.warning(f"[POWIAZANIA] ⚠️ Brak lokalnego wpisu faktury zbiorczej {nr_faktury_zbiorczej}")
                    pominiete += 1
                    continue

                # 🔹 wyszukaj faktury cząstkowe dla tego NIPu
                sub = df[df["NIP"].astype(str).str.replace(r"\D", "", regex=True) == nip]
                if sub.empty:
                    logging.warning(f"[POWIAZANIA] ⚠️ Brak faktur cząstkowych dla NIP={nip}")
                    pominiete += 1
                    continue

                for _, row in sub.iterrows():
                    numer_faktury_skladnikowej = str(row["Numer dokumentu"]).strip()

                    # 🔹 pobierz id faktury cząstkowej
                    cur.execute("""
                        SELECT id_faktury FROM faktury_do_prowizji WHERE numer_faktury = %s
                    """, (numer_faktury_skladnikowej,))
                    res_skladnikowa = cur.fetchone()
                    if not res_skladnikowa:
                        logging.warning(
                            f"[POWIAZANIA] ⚠️ Nie znaleziono faktury cząstkowej {numer_faktury_skladnikowej}")
                        pominiete += 1
                        continue

                    id_faktury_skladnikowej = res_skladnikowa["id_faktury"]

                    # 🔹 zapisz powiązanie (pełne: id_zbiorczej, id_skladnikowej, nr_faktury)
                    cur.execute("""
                        INSERT INTO faktura_powiazania (
                            id_faktury_zbiorczej,
                            id_faktury_skladnikowej,
                            nr_faktury
                        )
                        VALUES (%s, %s, %s)
                        ON CONFLICT DO NOTHING;
                    """, (
                        id_faktury_zbiorczej,
                        id_faktury_skladnikowej,
                        nr_faktury_zbiorczej
                    ))
                    dodane += 1

            conn.commit()
            logging.info(f"[POWIAZANIA] ✅ Dodano {dodane} powiązań, pominięto {pominiete}.")

FAKTUROWNIA_API = os.getenv("FAKTUROWNIA_API", "https://shumee.fakturownia.pl")
FAKTUROWNIA_TOKEN = os.getenv("FAKTUROWNIA_TOKEN")

### DO SPRAWDZENIA
def get_names_from_db_for_nips(nips: list[str | int]) -> dict[str, str]:
    """
    Zwraca mapę {NIP: nazwa} dla podanych NIP-ów z tabeli `merchanci` (nip BIGINT).
    Czyści nazwę z '|', różnych rodzajów myślników, i nadmiarowych spacji / znaków nowej linii.
    """

    nips_bigint: list[int] = []
    for x in nips:
        try:
            s = str(x).strip()
            if s and s.replace(" ", "").isdigit():
                nips_bigint.append(int(s))
        except Exception:
            continue

    if not nips_bigint:
        return {}

    sql = """
        SELECT nip, nazwa
        FROM merchanci
        WHERE nip = ANY(%s::bigint[])
          AND nazwa IS NOT NULL
    """

    def _clean_name(name: str) -> str:
        # oczyszcza nazwę z dziwnych znaków
        # pobiera string z bazy
        # zwraca oczyszczy string(adres)
        if not name:
            return ""

        t = unicodedata.normalize("NFKC", str(name))
        t = re.sub(r'^[\-\u2010\u2011\u2012\u2013\u2014\u2212\s]*\|+', '', t)
        t = re.sub(r'[\-\u2010\u2011\u2012\u2013\u2014\u2212]', ' ', t)
        t = re.sub(r'\s+', ' ', t).strip()
        t = re.sub(r'(\b\d{2}) (\d{3}\b)', r'\1-\2', t)
        t = re.sub(r'\s*\|\s*', '|', t)
        t = t.strip('|')
        return t

    result: dict[str, str] = {}

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (nips_bigint,))
            rows = cur.fetchall()

            if not rows:
                return {}

            sample = rows[0]
            if isinstance(sample, dict):  # RealDictCursor
                for r in rows:
                    result[str(r["nip"]).strip()] = _clean_name(str(r["nazwa"]))
            else:
                for nip_val, nazwa_val in rows:
                    result[str(nip_val).strip()] = _clean_name(str(nazwa_val))

    logging.info(f"[DB] Zmapowano nazwy z bazy dla {len(result)}/{len(nips_bigint)} NIP-ów.")
    return result

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

def zapisz_faktury_prowizje(wyniki, company):
    """
    Zapisuje wystawione faktury prowizyjne z API do bazy lokalnej.
    """
    from utils import db_conn
    import logging
    from datetime import date

    zapisane, duplikaty = 0, 0

    with db_conn() as conn:
        with conn.cursor() as cur:
            for w in wyniki:
                if not w.get("ok"):
                    continue

                try:
                    cur.execute("""
                        INSERT INTO faktury_prowizje (
                            id_fakturowni,
                            numer_faktury,
                            data_wystawienia,
                            kwota_netto,
                            kwota_vat,
                            kwota_brutto,
                            nip,
                            kontrahent,
                            nazwa_spolki,
                            adres
                        )
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (numer_faktury) DO NOTHING;
                    """, (
                        w.get("id"),
                        w.get("number"),
                        w.get("issue_date") or date.today(),
                        w.get("netto") or 0,
                        w.get("vat") or 0,
                        w.get("brutto") or 0,
                        w.get("nip"),
                        w.get("buyer_name"),
                        company,
                        w.get("buyer_address", "")
                    ))
                    zapisane += 1
                except Exception as e:
                    duplikaty += 1
                    logging.warning(f"[DB] ⚠️ Duplikat/błąd zapisu faktury prowizyjnej {w.get('number')}: {e}")

        conn.commit()

    logging.info(f"[DB] ✅ Zapisano {zapisane} faktur prowizyjnych, pominięto {duplikaty}.")


def zapisz_powiazania(df, wyniki):
    """
    Tworzy powiązania między fakturami źródłowymi (tabela faktury)
    a wystawionymi fakturami prowizyjnymi (tabela faktury_prowizje).
    """

    dodane, pominiete = 0, 0

    with db_conn() as conn:
        with conn.cursor() as cur:
            for w in wyniki:
                if not w.get("ok"):
                    continue  # pomiń nieudane faktury

                faktura_api_id = w.get("id")
                nip = w.get("nip")

                # 🔍 znajdź lokalne ID faktury prowizyjnej (tej wystawionej przez API)
                cur.execute("""
                    SELECT id_faktury_prowizji 
                    FROM faktury_prowizje
                    WHERE id_fakturowni = %s;
                """, (faktura_api_id,))
                prow = cur.fetchone()

                if not prow:
                    logging.warning(f"[POWIAZANIA] ⚠️ Brak lokalnej faktury prowizyjnej {faktura_api_id}")
                    pominiete += 1
                    continue

                # 🧩 obsługa różnych typów zwróconych wyników (tuple lub dict)
                id_faktury_prowizji = (
                    prow.get("id_faktury_prowizji") if isinstance(prow, dict) else prow[0]
                )

                # 🔍 wybierz wszystkie faktury źródłowe dla danego NIP z DataFrame
                sub = df[df["NIP"] == nip]

                for _, f in sub.iterrows():
                    try:
                        numer_dokumentu = str(f.get("Numer dokumentu", "")).strip()
                        if not numer_dokumentu:
                            continue

                        # znajdź fakturę źródłową po numerze
                        cur.execute("""
                            SELECT id_faktury 
                            FROM faktury_do_prowizji
                            WHERE numer_faktury = %s;
                        """, (numer_dokumentu,))
                        src = cur.fetchone()
                        if not src:
                            logging.warning(f"[POWIAZANIA] ⚠️ Nie znaleziono faktury źródłowej '{numer_dokumentu}'")
                            pominiete += 1
                            continue

                        id_faktury_zrodlowej = (
                            src.get("id_faktury") if isinstance(src, dict) else src[0]
                        )

                        # 🧾 wstawienie powiązania
                        cur.execute("""
                            INSERT INTO powiazania_faktur (id_faktury_prowizji, id_faktury_zrodlowej)
                            VALUES (%s, %s)
                            ON CONFLICT DO NOTHING;
                        """, (id_faktury_prowizji, id_faktury_zrodlowej))
                        dodane += 1

                    except Exception as e:
                        logging.warning(f"[POWIAZANIA] ⚠️ Błąd przy zapisie powiązania: {e}")
                        pominiete += 1

        conn.commit()

    logging.info(f"[POWIAZANIA] ✅ Dodano {dodane} powiązań, pominięto {pominiete}.")


# funkcja sprawdza czy w bazie nie ma faktur cząstkowych na bazie któych zostały wystawione faktury
# 3%
def sprawdz_powielone_faktury(conn, df):
    """
    Sprawdza, czy kontrahenci (po NIP) mają już wystawione faktury prowizyjne,
    czyli ich faktury źródłowe są już powiązane w tabeli 'powiazania_faktur'.
    Zwraca listę NIP-ów do pominięcia.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT f.nip
        FROM faktury_prowizje f
        JOIN powiazania_faktur p ON f.id_faktury_prowizji = p.id_faktury_prowizji
        WHERE f.nip IS NOT NULL;
    """)

    rows = cur.fetchall()

    nipy_powiazane = set()
    for row in rows:
        if isinstance(row, dict):
            nip_val = row.get("nip")
        else:
            nip_val = row[0]
        if nip_val:
            nip_clean = str(nip_val).replace("PL", "").replace(" ", "").strip()
            nipy_powiazane.add(nip_clean)

    duplikaty = []
    for _, row in df.iterrows():
        nip = str(row.get("NIP", "")).replace("PL", "").replace(" ", "").strip()
        if nip in nipy_powiazane:
            duplikaty.append(nip)

    if duplikaty:
        logging.warning(
            f"[DUPLIKATY] ⚠️ Pominięto {len(duplikaty)} kontrahentów, którzy mają już wystawioną fakturę prowizyjną.")
    else:
        logging.info("[DUPLIKATY] ✅ Brak wcześniej rozliczonych kontrahentów.")

    return duplikaty
