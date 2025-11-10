import os
import pandas as pd
import logging
from dotenv import load_dotenv
from utils import db_conn, clean_nip

# === konfiguracja ===
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

FILES = [
    # r"fakturowanie wrzesien shumee.xlsx"
    # r"great 1_10_25",
     r"great scalone pazdziernik.xlsx",
]

SPOLKA = "extrastore"  # zmień na greatstore / extrastore


def import_invoices():
    dfs = []

    # === 1. Wczytaj pliki XLSX ===
    for f in FILES:
        if not os.path.exists(f):
            logging.warning(f"[FILE] ⚠️ Brak pliku: {f}")
            continue
        df = pd.read_excel(f)
        logging.info(f"[FILE] Wczytano {len(df)} rekordów z {os.path.basename(f)}")
        dfs.append(df)

    if not dfs:
        logging.error("❌ Nie znaleziono żadnych plików do importu.")
        return

    df = pd.concat(dfs, ignore_index=True)
    logging.info(f"[DATA] Scalono {len(df)} rekordów")

    # === 2. Normalizacja nazw kolumn ===
    df.columns = [str(c).strip().replace("'", "").lower() for c in df.columns]
    rename_map = {
        "data wystawienia": "Data wystawienia",
        "data wpływu": "Data wpływu",
        "data zakupu": "Data zakupu",
        "numer dokumentu": "Numer dokumentu",
        "kontrahent": "Kontrahent",
        "netto": "Netto",
        "vat": "VAT",
        "brutto": "Brutto",
        "opis": "Opis",
        "nip": "NIP",
    }
    df = df.rename(columns=rename_map)

    # === 3. Sprawdź wymagane kolumny ===
    required = ["Data wystawienia", "Numer dokumentu", "NIP", "Netto", "VAT", "Brutto"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"❌ Brak wymaganej kolumny: {col}")

    # === 4. Czyszczenie danych ===
    df["NIP"] = df["NIP"].astype(str).apply(clean_nip)
    df["Data wystawienia"] = pd.to_datetime(df["Data wystawienia"], errors="coerce")
    for c in ["Netto", "VAT", "Brutto"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df = df.dropna(subset=["Data wystawienia", "Numer dokumentu", "NIP"])
    df = df[df["Numer dokumentu"].astype(str).str.strip() != ""]
    before = len(df)
    df = df.drop_duplicates(subset=["Numer dokumentu"])
    logging.info(f"[CLEAN] Usunięto {before - len(df)} duplikatów → pozostało {len(df)} rekordów")

    # === 5. Zapis do bazy ===
    insert_sql = """
        INSERT INTO faktury_do_prowizji
            (numer_faktury, data_wystawienia, kwota_netto, kwota_vat, kwota_brutto, nazwa_spolki, id_fakturowni)
        VALUES (%s, %s, %s, %s, %s, %s, NULL)
        ON CONFLICT (numer_faktury) DO NOTHING;
    """

    inserted = 0
    with db_conn() as conn:
        with conn.cursor() as cur:
            for _, row in df.iterrows():
                cur.execute(
                    insert_sql,
                    (
                        str(row["Numer dokumentu"]).strip(),
                        row["Data wystawienia"].date(),
                        float(row["Netto"]),
                        float(row["VAT"]),
                        float(row["Brutto"]),
                        SPOLKA,
                    ),
                )
                inserted += cur.rowcount
        conn.commit()

    logging.info(f"[DB] ✅ Zapisano {inserted} nowych faktur do tabeli faktury_do_prowizji.")


if __name__ == "__main__":
    import_invoices()