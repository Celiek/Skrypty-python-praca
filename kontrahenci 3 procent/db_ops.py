import logging
import os
import re

import pandas as pd
import requests
import unicodedata
from dotenv import load_dotenv
from pandas.core.interchange.dataframe_protocol import DataFrame
from psycopg2.extras import RealDictCursor

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

def insert_new_invoices_from_xlsx(df: DataFrame, company: str):
    df_to_db = df.copy()
    spolka = company.strip()

    if df_to_db.empty:
        logging.info("[DB] Brak danych do zapisania.")
        return

    required = {"Numer dokumentu", "Data wystawienia", "Netto", "VAT", "Brutto", "NIP"}
    missing = required - set(df_to_db.columns)
    if missing:
        raise ValueError(f"[DB] Brakuje kolumn: {', '.join(sorted(missing))}")

    # Normalizacja NIP
    df_to_db["__nip"] = (
        df_to_db["NIP"]
        .astype(str)
        .str.replace(r"\D", "", regex=True)
        .str.strip()
    )

    inserted = 0
    skipped = 0

    with db_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:

        # Pobieramy wszystkie istniejące faktury
        cur.execute("""
            SELECT nip, numer_faktury, nazwa_spolki,
                   kwota_netto, kwota_vat, kwota_brutto
            FROM faktury_do_prowizji
        """)

        # Klucz → wartości kwot
        existing = {
            (r["nip"], r["numer_faktury"], r["nazwa_spolki"]):
                (float(r["kwota_netto"]),
                 float(r["kwota_vat"]),
                 float(r["kwota_brutto"]))
            for r in cur.fetchall()
        }

        for _, row in df_to_db.iterrows():

            numer = str(row["Numer dokumentu"]).strip()
            nip = row["__nip"]

            if not numer or not nip:
                skipped += 1
                continue

            key = (nip, numer, spolka)

            kw_netto = float(row["Netto"])
            kw_vat = float(row["VAT"])
            kw_brutto = float(row["Brutto"])

            # duplikat
            if key in existing:
                old = existing[key]
                if old == (kw_netto, kw_vat, kw_brutto):
                    logging.info(f"[DB] Duplikat: FV={numer} NIP={nip} SP={spolka} – pomijam")
                    skipped += 1
                    continue
                else:
                    logging.info(f"[DB] Korekta: FV={numer} {old[2]} zł → {kw_brutto} zł")

            data = pd.to_datetime(row["Data wystawienia"], errors="coerce")
            data = data.date() if not pd.isna(data) else None

            try:
                cur.execute("""
                    INSERT INTO faktury_do_prowizji (
                        numer_faktury, data_wystawienia,
                        kwota_netto, kwota_vat, kwota_brutto,
                        nip, nazwa_spolki
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    numer, data,
                    kw_netto, kw_vat, kw_brutto,
                    nip, spolka
                ))

                existing[key] = (kw_netto, kw_vat, kw_brutto)
                inserted += 1

            except Exception as e:
                logging.error(f"[DB] Błąd INSERT {numer}/{nip}: {e}")
                skipped += 1

        conn.commit()

    logging.info(f"[DB] Wynik: {inserted} dodanych, {skipped} pominiętych.")


FAKTUROWNIA_API = os.getenv("FAKTUROWNIA_API", "https://shumee.fakturownia.pl")
FAKTUROWNIA_TOKEN = os.getenv("FAKTUROWNIA_TOKEN")


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

# niepotrzeban funkcja
# def zapisz_faktury_do_prowizji(wyniki, company):
#     zapisane, pominiete = 0, 0
#
#     with db_conn() as conn, conn.cursor() as cur:
#         for w in wyniki:
#             if not w.get("ok"):
#                 continue
#
#             try:
#                 cur.execute("""
#                     INSERT INTO faktury_do_prowizji (
#                         id_fakturowni,
#                         numer_faktury,
#                         data_wystawienia,
#                         kwota_netto,
#                         kwota_vat,
#                         kwota_brutto,
#                         nip,
#                         nazwa_spolki
#                     )
#                     VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
#                     ON CONFLICT (nip, numer_faktury, nazwa_spolki) DO NOTHING;
#                 """, (
#                     w.get("id"),
#                     w.get("number"),
#                     w.get("issue_date"),
#                     w.get("netto"),
#                     w.get("vat"),
#                     w.get("brutto"),
#                     str(w.get("nip")).replace("PL","").strip(),
#                     company
#                 ))
#                 zapisane += 1
#
#             except Exception as e:
#                 logging.warning(f"[DB] Pominieto FV {w.get('number')}: {e}")
#                 pominiete += 1
#
#         conn.commit()
#
#     logging.info(f"[DB] {zapisane} dodanych, {pominiete} pominiętych.")

# def zapisz_powiazania(df, wyniki):
#     """
#     Tworzy powiązania między fakturami źródłowymi (tabela faktury)
#     a wystawionymi fakturami prowizyjnymi (tabela faktury_do_prowizji).
#     """
#
#     dodane, pominiete = 0, 0
#
#     with db_conn() as conn:
#         with conn.cursor() as cur:
#             for w in wyniki:
#                 if not w.get("ok"):
#                     continue  # pomiń nieudane faktury
#
#                 faktura_api_id = w.get("id")
#                 nip = w.get("nip")
#
#                 # 🔍 znajdź lokalne ID faktury prowizyjnej (tej wystawionej przez API)
#                 cur.execute("""
#                     SELECT id_faktury_prowizji
#                     FROM faktury_do_prowizji
#                     WHERE id_fakturowni = %s;
#                 """, (faktura_api_id,))
#                 prow = cur.fetchone()
#
#                 if not prow:
#                     logging.warning(f"[POWIAZANIA] ⚠️ Brak lokalnej faktury prowizyjnej {faktura_api_id}")
#                     pominiete += 1
#                     continue
#
#                 # obsługa różnych typów zwróconych wyników (tuple lub dict)
#                 id_faktury_prowizji = (
#                     prow.get("id_faktury_prowizji") if isinstance(prow, dict) else prow[0]
#                 )
#
#                 # 🔍 wybierz wszystkie faktury źródłowe dla danego NIP z DataFrame
#                 sub = df[df["NIP"] == nip]
#
#                 for _, f in sub.iterrows():
#                     try:
#                         numer_dokumentu = str(f.get("Numer dokumentu", "")).strip()
#                         if not numer_dokumentu:
#                             continue
#
#                         # znajdź fakturę źródłową po numerze
#                         cur.execute("""
#                             SELECT id_faktury
#                             FROM faktury_do_prowizji
#                             WHERE numer_faktury = %s;
#                         """, (numer_dokumentu,))
#                         src = cur.fetchone()
#                         if not src:
#                             logging.warning(f"[POWIAZANIA] ⚠️ Nie znaleziono faktury źródłowej '{numer_dokumentu}'")
#                             pominiete += 1
#                             continue
#
#                         id_faktury_zrodlowej = (
#                             src.get("id_faktury") if isinstance(src, dict) else src[0]
#                         )
#
#                         # 🧾 wstawienie powiązania
#                         cur.execute("""
#                             INSERT INTO powiazania_faktur (id_faktury_prowizji, id_faktury_zrodlowej)
#                             VALUES (%s, %s)
#                             ON CONFLICT DO NOTHING;
#                         """, (id_faktury_prowizji, id_faktury_zrodlowej))
#                         dodane += 1
#
#                     except Exception as e:
#                         logging.warning(f"[POWIAZANIA] ⚠️ Błąd przy zapisie powiązania: {e}")
#                         pominiete += 1
#
#         conn.commit()
#
#     logging.info(f"[POWIAZANIA] ✅ Dodano {dodane} powiązań, pominięto {pominiete}.")


# funkcja sprawdza czy w bazie nie ma faktur cząstkowych na bazie któych zostały wystawione faktury
# 3%
# def find_duplicate_source_invoices(df, company):
#     """
#     Sprawdza duplikaty na poziomie KONKRETNEJ faktury, a nie całego NIP-u.
#     Duplikatem jest:
#     - ten sam NIP
#     - ten sam numer faktury
#     - te same kwoty (Netto, VAT, Brutto)
#     - już wcześniej rozliczony w tej samej spółce
#
#     Zwraca indeksy do usunięcia z DF oraz info debug.
#     """
#     duplikaty_idx = []
#     checked = 0
#
#     with db_conn() as conn, conn.cursor() as cur:
#         for i, row in df.iterrows():
#             checked += 1
#
#             nip = str(row["NIP"]).strip()
#             numer = str(row["Numer dokumentu"]).strip()
#             kw_netto = float(row["Netto"])
#             kw_vat = float(row["VAT"])
#             kw_brutto = float(row["Brutto"])
#
#             if not nip or not numer:
#                 continue
#
#             cur.execute(
#                 """
#                 SELECT 1 FROM faktury_do_prowizji
#                 WHERE nip = %s
#                   AND numer_faktury = %s
#                   AND kwota_netto = %s
#                   AND kwota_vat = %s
#                   AND kwota_brutto = %s
#                   AND nazwa_spolki = %s
#                 LIMIT 1
#                 """,
#                 (nip, numer, kw_netto, kw_vat, kw_brutto, company)
#             )
#
#             if cur.fetchone():
#                 duplikaty_idx.append(i)
#
#     return duplikaty_idx, checked

def get_names_from_db_for_nips(nips: list[str | int]) -> dict[str, str]:
    """
    Zwraca mapę {NIP: nazwa} dla podanych NIP-ów z tabeli `merchanci` (nip BIGINT).
    Czyści nazwę.
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
        if not name:
            return ""
        t = unicodedata.normalize("NFKC", str(name))
        t = re.sub(r'^[\-\u2010\u2011\u2012\u2013\u2014\u2212\s]*\|+', '', t)
        t = re.sub(r'[\-\u2010\u2011\u2012\u2013\u2014\u2212]', ' ', t)
        t = re.sub(r'\s+', ' ', t).strip()
        t = re.sub(r'\s*\|\s*', '|', t)
        t = t.strip('|')
        return t

    result: dict[str, str] = {}

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (nips_bigint,))
        rows = cur.fetchall()

        for row in rows:
            if isinstance(row, dict):
                nip_val = row.get("nip")
                name_val = row.get("nazwa")
            else:
                nip_val = row[0]
                name_val = row[1]

            if nip_val:
                nip_clean = str(nip_val)
                result[nip_clean] = _clean_name(name_val)

    logging.info(f"[DEBUG] nazwy pobrane z DB: {len(result)} rekordów")
    return result
