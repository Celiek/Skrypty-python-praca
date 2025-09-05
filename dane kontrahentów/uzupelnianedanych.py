import re
import logging
import pandas as pd
import psycopg2

# --- Konfiguracja logowania ---
logging.basicConfig(
    filename="import_kontrahenci.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


def waliduj_nip(nip: str) -> bool:
    """Sprawdza czy NIP ma 10 cyfr."""
    return nip.isdigit() and len(nip) == 10

def waliduj_nr_konta(nr: str) -> bool:
    """Sprawdza czy numer konta ma 26 cyfr (PL IBAN bez PL)."""
    return nr.isdigit() and len(nr) == 26


df = pd.read_excel("dane z numerami kont bankowych.xlsx", engine="openpyxl")

# --- Połączenie z bazą ---
conn = psycopg2.connect(
    host="localhost",
    database="merchanci",
    user="gabriel",
    password="lhj7r7nk7e"
)
cursor = conn.cursor()

for row in df.itertuples(index=False):
    nazwa = str(row.Kontrahent).strip()
    nr_konta = str(row._asdict().get('Rachunek kontrahenta', '')).replace("'", "").strip()
    opis = str(row._asdict().get('Opis transakcji', '')).strip()

    # Wyciągnięcie NIP
    match_nip = re.search(r'IDC/(\d+)', opis)
    if not match_nip:
        logging.warning(f"Brak NIP dla kontrahenta: {nazwa}")
        continue

    nip = match_nip.group(1)

    # Walidacja danych
    if not waliduj_nip(nip):
        logging.warning(f"NIP niepoprawny: {nip} (kontrahent: {nazwa})")
        continue

    if not waliduj_nr_konta(nr_konta):
        logging.warning(f"Numer konta niepoprawny: {nr_konta} (kontrahent: {nazwa})")
        continue

    # Aktualizacja w bazie
    try:
        cursor.execute(
            """
            UPDATE merchanci
            SET nr_konta = %s
            WHERE nip = %s AND nazwa LIKE %s
            """,
            (nr_konta, nip, f"%{nazwa}%")
        )
        logging.info(f"Zaktualizowano: {nazwa} (NIP: {nip})")
    except Exception as e:
        logging.error(f"Błąd przy aktualizacji {nazwa} (NIP: {nip}): {e}")
        conn.rollback()
    else:
        conn.commit()

# --- Sprzątanie ---
cursor.close()
conn.close()
logging.info("Import zakończony.")