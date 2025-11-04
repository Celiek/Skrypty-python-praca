import logging
import os
from decimal import Decimal

import pandas as pd
import requests
from dotenv import load_dotenv

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
    Wstawia do tabeli 'faktury' rekordy z XLSX, których numer_faktury nie istnieje w bazie.
    Wymagane kolumny: Numer dokumentu, Data wystawienia, Netto, VAT, Brutto.
    """
    df = pd.read_excel(xlsx_path)

    # standaryzacja nazw kolumn
    df.columns = [c.strip() for c in df.columns]
    required_cols = {"Numer dokumentu", "Data wystawienia", "Netto", "VAT", "Brutto"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Brakuje kolumn: {', '.join(missing)} w pliku {xlsx_path}")

    inserted, skipped = 0, 0

    with db_conn() as conn:
        with conn.cursor() as cur:
            for _, row in df.iterrows():
                numer = str(row["Numer dokumentu"]).strip()
                if not numer:
                    continue

                # sprawdzenie duplikatu
                cur.execute("SELECT 1 FROM faktury WHERE numer_faktury = %s", (numer,))
                if cur.fetchone():
                    skipped += 1
                    continue

                try:
                    kw_netto = Decimal(str(row["Netto"]).replace(",", "."))
                    kw_vat = Decimal(str(row["VAT"]).replace(",", "."))
                    kw_brutto = Decimal(str(row["Brutto"]).replace(",", "."))
                except Exception:
                    logging.warning(f"[DB] Błąd parsowania kwot w fakturze {numer}")
                    continue

                data_wyst = pd.to_datetime(row["Data wystawienia"], errors="coerce").date()

                cur.execute("""
                    INSERT INTO faktury (numer_faktury, data_wystawienia,
                                         kwota_netto, kwota_vat, kwota_brutto,
                                         typ_faktury, nazwa_spolki)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (numer_faktury) DO NOTHING;
                """, (numer, data_wyst, kw_netto, kw_vat, kw_brutto, "POJEDYNCZA", company))
                inserted += 1

        conn.commit()

    logging.info(f"[DB] ✅ Zapisano {inserted} nowych faktur, pominięto {skipped} duplikatów.")


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
                        SELECT id_faktury FROM faktury WHERE numer_faktury = %s
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

    # logging.debug(f"[DEBUG] Dane wejściowe do zapisz_faktury_prowizje: {wyniki}")

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
    import logging
    from utils import db_conn

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
                            FROM faktury 
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
