import os
import logging
import pandas as pd
from argparse import ArgumentParser
from dotenv import load_dotenv
from datetime import date

from fakturownia_api import get_faktur, dodaj_faktury
from reports import export_grouped_excels
from db_ops import zapisz_faktury_do_bazy, zapisz_powiazania_do_bazy, get_addresses_from_db
from utils import clean_nip

# === Konfiguracja logowania ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# === Wczytanie zmiennych środowiskowych ===
load_dotenv()
if not os.getenv("API_KEY"):
    raise RuntimeError("❌ Brak API_KEY w pliku .env!")

# === Ustawienia identyfikatorów departamentów Fakturownia ===
DEPARTMENT_ID = {
    "shumee": 1732019,
    "greatstore": 1705454,
    "extrastore": 1705460,
}

# === Główna logika ===
if __name__ == "__main__":
    parser = ArgumentParser(description="Automatyzacja faktur prowizyjnych 3% / 2%")
    parser.add_argument("input", help="Plik XLSX z fakturami cząstkowymi")
    parser.add_argument("-c", "--company", required=True, help="Nazwa spółki: shumee / greatstore / extrastore")
    parser.add_argument("--invoices-only", action="store_true", help="Tylko wystawia i pobiera faktury (bez maili)")
    parser.add_argument("--save-db", action="store_true", help="Zapisuje faktury do bazy danych")
    args = parser.parse_args()

    company = args.company.lower().strip()
    logging.info(f"=== Uruchamianie dla spółki: {company.upper()} ===")

    # === Wczytanie danych z pliku Excel ===
    if not os.path.exists(args.input):
        raise FileNotFoundError(f"❌ Nie znaleziono pliku: {args.input}")

    df = pd.read_excel(args.input)
    if df.empty:
        raise ValueError("❌ Plik wejściowy jest pusty!")

    logging.info(f"✅ Wczytano {len(df)} rekordów z pliku {args.input}")

    adresy_z_bazy = get_addresses_from_db()
    logging.info(f"[DB] Załadowano {len(adresy_z_bazy)} adresów z tabeli merchanci.")

    # === Mapowanie kolumn (na wypadek różnych nazw) ===
    column_aliases = {
        "kwota netto": "Netto",
        "wartość netto": "Netto",
        "netto (pln)": "Netto",
        "kwota vat": "VAT",
        "wartość vat": "VAT",
        "vat (pln)": "VAT",
        "kwota brutto": "Brutto",
        "wartość brutto": "Brutto",
        "brutto (pln)": "Brutto",
    }
    df.columns = [column_aliases.get(c.lower().strip(), c) for c in df.columns]
    df["NIP"] = df["NIP"].astype(str).apply(clean_nip)

    # === Uzupełnij brakujące kolumny, jeśli trzeba ===
    for col in ["Netto", "VAT", "Brutto"]:
        if col not in df.columns:
            df[col] = 0

    # === Tworzenie raportów XLSX per kontrahent ===
    logging.info("[RAPORT] Tworzenie raportów XLSX per kontrahent...")
    xlsx_map = export_grouped_excels(df, out_dir="raporty_xlsx")

    # === Przygotowanie danych do wystawienia faktur ===
    df["Netto"] = pd.to_numeric(df["Netto"], errors="coerce").fillna(0)
    grouped = df.groupby(["NIP", "Kontrahent"], as_index=False)["Netto"].sum()

    # NIP-y z prowizją 2% (reszta 3%)
    SPECIAL_2PROC = {"6020134043"}
    grouped["stawka_proc"] = grouped["NIP"].apply(lambda x: 0.02 if str(x) in SPECIAL_2PROC else 0.03)

    grouped["amount_net"] = (grouped["Netto"] * grouped["stawka_proc"]).round(2)
    grouped["amount_gross"] = (grouped["amount_net"] * 1.23).round(2)

    items = []
    for _, r in grouped.iterrows():
        nip_clean = str(r["NIP"]).strip()
        items.append({
            "buyer_name": str(r["Kontrahent"]).strip(),
            "buyer_tax_no": nip_clean,
            "buyer_address": adresy_z_bazy.get(nip_clean, ""),  # <-- tu dopisujemy adres z bazy
            "amount_net": str(r["amount_net"]),
            "amount_gross": str(r["amount_gross"]),
        })

    logging.info(f"[FAKTUROWNIA] Przygotowano {len(items)} faktur do wystawienia.")

    # === Wystawianie faktur przez API Fakturownia ===
    dept_id = DEPARTMENT_ID.get(company, 1732019)
    wyniki = dodaj_faktury(company, items, dept_id)
    sukcesy = sum(1 for w in wyniki if w.get("ok"))
    logging.info(f"[FAKTUROWNIA] Wystawiono {sukcesy} faktur.")

    # === Zapis do bazy danych ===
    if args.save_db:
        logging.info("[DB] Zapis faktur do bazy danych...")
        zapisz_faktury_do_bazy(df, company)
        zapisz_powiazania_do_bazy(df, wyniki, company)

    # === Pobieranie wystawionych faktur z Fakturowni ===
    if args.invoices_only:
        today = date.today().isoformat()
        logging.info(f"[POBIERANIE] Pobieram faktury z dnia {today}...")
        get_faktur(date_from=today, date_to=today)

    logging.info("=== Zakończono działanie programu ===")
