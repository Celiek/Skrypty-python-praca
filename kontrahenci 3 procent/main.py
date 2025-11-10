import logging
import os
from argparse import ArgumentParser
from datetime import date, datetime
import pandas as pd
from dotenv import load_dotenv

from db_ops import (
    insert_new_invoices_from_xlsx,
    zapisz_faktury_prowizje,
    zapisz_powiazania,
    get_addresses_from_db,
    sprawdz_powielone_faktury,
    get_names_from_db_for_nips,
)
from fakturownia_api import get_faktur, dodaj_faktury
from reports import export_grouped_excels
from utils import clean_nip, db_conn, clean_df


# ===== logging =====
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ===== env =====
load_dotenv()
if not os.getenv("API_KEY"):
    raise RuntimeError("[API] Brak API_KEY w pliku .env!")


DEPARTMENT_ID = {
    "shumee": 1732019,
    "greatstore": 1705454,
    "extrastore": 1705460,
}

SPECIAL_2PROC = {"6020134043"}  # NIP-y na 2%


def parse_issue_date(arg_val: str | None) -> date:
    if not arg_val:
        return date.today()
    try:
        return datetime.strptime(arg_val, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError(f"[CLI] Niepoprawny format daty: {arg_val}. Użyj RRRR-MM-DD.")


# ✅ GŁÓWNA POPRAWKA — poprawne parsowanie dat z Excela = dd.mm.yyyy
def parse_date_series(s: pd.Series) -> pd.Series:
    """Wymuszone polskie daty dd.mm.yyyy"""
    return pd.to_datetime(s.astype(str), errors="coerce", dayfirst=True)


def build_items_from_merchants_and_invoices(df_faktury, df_merch, adresy_z_bazy):

    df_faktury = df_faktury.copy()
    df_merch = df_merch.copy()

    # ✅ FIX: prawidłowe parsowanie Data wystawienia
    df_faktury["Data wystawienia"] = parse_date_series(df_faktury["Data wystawienia"])
    df_faktury["NIP"] = df_faktury["NIP"].astype(str).apply(clean_nip)

    # ✅ FIX: prawidłowe parsowanie daty w filtrze
    if "Od kiedy prowizja 3%" in df_merch.columns:
        df_merch["Od kiedy prowizja 3%"] = parse_date_series(df_merch["Od kiedy prowizja 3%"])
    else:
        df_merch["Od kiedy prowizja 3%"] = pd.NaT

    df_merch["NIP"] = df_merch["NIP"].astype(str).apply(clean_nip)

    # Kwoty
    for c in ["Netto", "VAT", "Brutto"]:
        if c in df_faktury.columns:
            df_faktury[c] = pd.to_numeric(df_faktury[c], errors="coerce").fillna(0)

    # Sprawdź poprawność dat (debug)
    logging.info("[DEBUG] Zakres dat wystawienia: %s → %s",
                 df_faktury["Data wystawienia"].min(),
                 df_faktury["Data wystawienia"].max())

    # Tylko kontrahenci aktywni
    today = pd.Timestamp(date.today())
    aktywni = df_merch[df_merch["Od kiedy prowizja 3%"].isna() |
                       (df_merch["Od kiedy prowizja 3%"] <= today)]

    if aktywni.empty:
        logging.warning("[FILTER] Brak kontrahentów spełniających kryteria 'Od kiedy prowizja 3%'.")
        return []

    # mapy
    nazwy_map = dict(zip(aktywni["NIP"], aktywni.get("Nazwa", pd.Series([""] * len(aktywni)))))
    email_map = dict(zip(aktywni["NIP"], aktywni.get("email", pd.Series([""] * len(aktywni)))))
    start_map = dict(zip(aktywni["NIP"], aktywni["Od kiedy prowizja 3%"]))

    # nazwy z DB
    names_db = get_names_from_db_for_nips(list(start_map.keys()))

    items = []

    for nip, start_dt in start_map.items():
        if pd.isna(start_dt):
            continue

        orig_start = start_dt

        # Logika przesunięcia
        if start_dt.day > 1:
            start_dt = (start_dt + pd.offsets.MonthEnd(0)).replace(day=1) + pd.offsets.MonthBegin(1)
            shifted = True
        else:
            start_dt = start_dt.replace(day=1)
            shifted = False

        sub = df_faktury[
            (df_faktury["NIP"] == nip) &
            (df_faktury["Data wystawienia"] >= start_dt)
        ]

        logging.info(f"[DEBUG] Faktury po {start_dt.date()} dla NIP {nip}: {len(sub)}")

        if sub.empty:
            logging.warning(f"[SKIP] Brak faktur dla NIP {nip} po {start_dt.date()}")
            continue

        suma_netto = float(sub["Netto"].sum())
        if suma_netto <= 0:
            continue

        stawka = 0.02 if str(nip) in SPECIAL_2PROC else 0.03
        amount_net = round(suma_netto * stawka, 2)
        amount_gross = round(amount_net * 1.23, 2)

        buyer_name = (
            names_db.get(nip)
            or str(nazwy_map.get(nip, "")).strip()
            or (str(sub["Kontrahent"].iloc[0]).strip() if "Kontrahent" in sub.columns else "")
        )

        items.append({
            "buyer_name": buyer_name,
            "buyer_tax_no": nip,
            "buyer_email": (str(email_map.get(nip, "")).strip() or None),
            "buyer_address": adresy_z_bazy.get(str(nip), ""),
            "amount_net": f"{amount_net:.2f}",
            "amount_gross": f"{amount_gross:.2f}",
        })

    logging.info(f"[BUILD] Przygotowano {len(items)} kontrahentów do fakturowania.")
    return items



# ===================== MAIN ======================

if __name__ == "__main__":
    parser = ArgumentParser(description="Automatyzacja faktur prowizyjnych 3% / 2%")
    parser.add_argument("input", help="Plik XLSX z fakturami cząstkowymi")
    parser.add_argument("-c", "--company", required=True, help="Nazwa spółki")
    parser.add_argument("--invoices-only", action="store_true")
    parser.add_argument("--save-db", action="store_true")
    parser.add_argument("--filter-xlsx", help="Plik XLSX z listą kontrahentów")
    parser.add_argument("--issue-date", dest="issue_date")
    parser.add_argument("--report-only",action="store_true",
        help="Tylko generuj raporty XLSX — bez wystawiania faktur i bez zapisu do bazy"
    )
    args = parser.parse_args()

    company = args.company.lower().strip()
    logging.info(f"=== Uruchamianie dla spółki: {company.upper()} ===")

    df = pd.read_excel(args.input)
    df = clean_df(df)

    # ✅ FIX: parsowanie wszystkich dat w fakturach
    if "Data wystawienia" in df.columns:
        df["Data wystawienia"] = parse_date_series(df["Data wystawienia"])

    df["NIP"] = df["NIP"].astype(str).apply(clean_nip)

    adresy_z_bazy = get_addresses_from_db()

    # filtr powielonych
    with db_conn() as conn:
        duplikaty = sprawdz_powielone_faktury(conn, df)

    df = df[~df["NIP"].isin(duplikaty)]

    for col in ["Netto", "VAT", "Brutto"]:
        if col not in df.columns:
            df[col] = 0

    export_grouped_excels(df, spolka=company, out_root="raporty_xlsx")

    if args.report_only:
        logging.info("[RAPORT] ✅ Zakończono — wygenerowano tylko raporty XLSX (bez wystawiania faktur).")
        raise SystemExit(0)

    # ─────────────────────────────────────────────
    # BUILDER ITEMS
    # ─────────────────────────────────────────────

    if args.filter_xlsx:
        kontrahenci_df = pd.read_excel(args.filter_xlsx)
        kontrahenci_df["NIP"] = kontrahenci_df["NIP"].astype(str).apply(clean_nip)

        # ✅ FIX — poprawne parsowanie dat w filtrze
        if "Od kiedy prowizja 3%" in kontrahenci_df.columns:
            kontrahenci_df["Od kiedy prowizja 3%"] = parse_date_series(
                kontrahenci_df["Od kiedy prowizja 3%"]
            )

        items = build_items_from_merchants_and_invoices(df, kontrahenci_df, adresy_z_bazy)
    else:
        raise RuntimeError("Musisz podać --filter-xlsx (lista kontrahentów).")

    logging.info(f"[FAKTUROWNIA] Do wystawienia: {len(items)} faktur")

    insert_new_invoices_from_xlsx(args.input, args.company)

    issue_date = parse_issue_date(args.issue_date)
    dept_id = DEPARTMENT_ID.get(company, 1732019)

    wyniki = dodaj_faktury(company, items, dept_id, issue_date)

    zapisz_faktury_prowizje(wyniki, args.company)
    zapisz_powiazania(df, wyniki)

    logging.info("=== Zakończono działanie programu ===")
