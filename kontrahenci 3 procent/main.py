import logging
import os
from argparse import ArgumentParser
from datetime import date, datetime

import pandas as pd
from dotenv import load_dotenv

from db_ops import (
    get_addresses_from_db,
    get_names_from_db_for_nips,
    save_source_invoices,
    filter_new_source_invoices,
    mark_as_used_by_ids,
    get_source_ids_for_df,
)
from fakturownia_api import dodaj_faktury, get_faktur
from reports import export_grouped_excels
from utils import clean_nip, clean_df, parse_date_series

# =====================================================
# LOGGING
# =====================================================

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

log_file = os.path.join(
    LOG_DIR,
    f"prowizje_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s:%(lineno)d | %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

logging.info(f"[START] Log zapisany do pliku: {log_file}")

# ENV
load_dotenv()
if not os.getenv("API_KEY"):
    raise RuntimeError("[API] Brak API_KEY w pliku .env!")

DEPARTMENT_ID = {
    "shumee": 1705441,
    "greatstore": 1705454,
    "extrastore": 1705460,
    "tsm3": 1732019
}

SPECIAL_2PROC = {"6020134043"}  # 2%

COMPANY_SUFFIX = {
    "shumee": ("/SM",),
    "greatstore": ("/GS",),
    "extrastore": ("/EX",),
    "tsm3": ("/TSM3",),
}

# =====================================================
# HELPERS
# =====================================================

def parse_issue_date(arg_val: str | None) -> date:
    return datetime.strptime(arg_val, "%Y-%m-%d").date() if arg_val else date.today()

def build_items_from_merchants_and_invoices(
    df_faktury: pd.DataFrame,
    df_merch: pd.DataFrame,
    adresy_z_bazy: dict[str, str],
):
    """
    1) Filtruje faktury wg dat startu 3%
    2) Zwraca:
       - items → do Fakturowni (agregat per NIP)
       - df_for_reports → TYLKO faktury użyte do prowizji (wiersze)
    """

    logging.info("[INFO] Rozpoczynam filtrowanie faktur do prowizji...")

    df_faktury = df_faktury.copy()
    df_merch = df_merch.copy()

    # daty + NIP
    df_faktury["Data wystawienia"] = parse_date_series(df_faktury["Data wystawienia"])
    df_faktury["NIP"] = df_faktury["NIP"].astype(str).apply(clean_nip)

    df_merch["NIP"] = df_merch["NIP"].astype(str).apply(clean_nip)
    if "Od kiedy prowizja 3%" in df_merch.columns:
        df_merch["Od kiedy prowizja 3%"] = parse_date_series(df_merch["Od kiedy prowizja 3%"])
    else:
        df_merch["Od kiedy prowizja 3%"] = pd.NaT

    # kwoty
    for col in ["Netto", "VAT", "Brutto"]:
        df_faktury[col] = df_faktury[col].astype(str).str.replace(",", ".", regex=False)
        df_faktury[col] = pd.to_numeric(df_faktury[col], errors="coerce").fillna(0)

    today_ts = pd.Timestamp(date.today())
    min_start = pd.Timestamp(2025, 1, 1)

    df_merch_valid = df_merch[
        df_merch["Od kiedy prowizja 3%"].notna()
        & (df_merch["Od kiedy prowizja 3%"] >= min_start)
    ].copy()

    nazwy_map = dict(zip(df_merch_valid["NIP"], df_merch_valid.get("Nazwa", "")))
    email_map = dict(zip(df_merch_valid["NIP"], df_merch_valid.get("email", "")))
    start_map = dict(zip(df_merch_valid["NIP"], df_merch_valid["Od kiedy prowizja 3%"]))

    names_db = get_names_from_db_for_nips(list(start_map.keys()))

    items: list[dict] = []
    accepted_frames: list[pd.DataFrame] = []

    for nip, start_dt in start_map.items():
        fv = df_faktury[df_faktury["NIP"] == nip]
        if fv.empty:
            continue

        # --- wyznaczenie daty od której liczymy prowizję ---
        if start_dt.day == 1:
            effective_start = pd.Timestamp(start_dt.year, start_dt.month, 1)
        else:
            next_month = start_dt.month + 1
            next_year = start_dt.year
            if next_month == 13:
                next_month = 1
                next_year += 1
            effective_start = pd.Timestamp(next_year, next_month, 1)

        current_month = today_ts.month
        current_year = today_ts.year

        accepted = []
        for _, row in fv.iterrows():
            d = row["Data wystawienia"]
            if pd.isna(d):
                continue
            if d < effective_start:
                continue
            if d.year == current_year and d.month == current_month:
                continue
            accepted.append(row)

        sub = pd.DataFrame(accepted)
        if sub.empty:
            continue

        accepted_frames.append(sub)

        suma_netto = float(sub["Netto"].sum())
        if suma_netto <= 0:
            continue

        stawka = 0.02 if str(nip) in SPECIAL_2PROC else 0.03

        buyer_name = (
            names_db.get(nip)
            or str(nazwy_map.get(nip, "")).strip()
            or str(sub["Kontrahent"].iloc[0]).strip()
        )

        items.append({
            "buyer_name": buyer_name,
            "buyer_tax_no": nip,
            "buyer_email": (str(email_map.get(nip, "")).strip() or None),
            "buyer_address": adresy_z_bazy.get(str(nip), ""),
            "amount_net": f"{round(suma_netto * stawka, 2):.2f}",
            "amount_gross": f"{round(suma_netto * stawka * 1.23, 2):.2f}",
        })

    if accepted_frames:
        accepted_all_df = pd.concat(accepted_frames, ignore_index=True)
    else:
        accepted_all_df = pd.DataFrame(columns=df_faktury.columns)

    logging.info(f"[DEBUG] Łącznie zaakceptowanych wierszy do raportu: {len(accepted_all_df)}")
    return items, accepted_all_df


# =====================================================
# MAIN
# =====================================================

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("input")
    parser.add_argument("-c", "--company", required=True)
    parser.add_argument("--filter-xlsx", required=True)
    parser.add_argument("--issue-date")
    parser.add_argument("--report-only", action="store_true")
    parser.add_argument("--wystaw", action="store_true")
    parser.add_argument("--save-invoices", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--save-db", action="store_true")

    args = parser.parse_args()
    company = args.company.lower().strip()

    if args.dry_run:
        logging.warning("=== DRY-RUN AKTYWNY ===")

    # =====================================================
    # 1️⃣ WCZYTANIE XLSX
    # =====================================================
    df = clean_df(pd.read_excel(args.input))
    df["Data wystawienia"] = parse_date_series(df["Data wystawienia"])
    df["NIP"] = df["NIP"].astype(str).apply(clean_nip)

    for col in ["Netto", "VAT", "Brutto"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    if df.empty:
        logging.info("[DONE] Brak faktur wejściowych.")
        raise SystemExit(0)

    # =====================================================
    # 2️⃣ MERCHANCI
    # =====================================================
    merch = pd.read_excel(args.filter_xlsx)
    merch["NIP"] = merch["NIP"].astype(str).apply(clean_nip)
    merch["Od kiedy prowizja 3%"] = parse_date_series(
        merch.get("Od kiedy prowizja 3%")
    )

    addresses = get_addresses_from_db()

    # =====================================================
    # 3️⃣ LOGIKA PROWIZJI (JEDNO WYWOŁANIE ❗)
    # =====================================================

    items_all, df_for_reports = build_items_from_merchants_and_invoices(
        df,
        merch,
        addresses
    )

    if df_for_reports.empty:
        logging.info("[DONE] Brak faktur kwalifikujących się do prowizji.")
        raise SystemExit(0)

    # =====================================================
    # FILTR DB (ANTI-JOIN)
    # =====================================================
    df_new = filter_new_source_invoices(df_for_reports, company)

    logging.info(
        f"[DB-FILTER] wejście={len(df_for_reports)} | "
        f"nowe={len(df_new)} | "
        f"już_w_DB={len(df_for_reports) - len(df_new)}"
    )

    if df_new.empty:
        logging.info("[SKIP] Wszystkie faktury są już w DB — NIE WYSTAWIAM FV.")
        raise SystemExit(0)

    # =====================================================
    # 5️⃣ ITEMS → TYLKO NOWE NIP-y
    # =====================================================
    allowed_nips = set(df_new["NIP"].astype(str))
    items = [
        it for it in items_all
        if clean_nip(it["buyer_tax_no"]) in allowed_nips
    ]

    if not items:
        logging.info("[SKIP] Brak items do wystawienia.")
        raise SystemExit(0)

    # =====================================================
    # 6️⃣ RAPORTY
    # ====================================================
    export_grouped_excels(
        df = df_new,
        spolka=company,
        out_root="raporty_xlsx"
    )
    if args.report_only:
        logging.info("[REPORT-ONLY] Zakończono na etapie raportów.")
        raise SystemExit(0)

    # =====================================================
    # 7️⃣ ZAPIS DO DB (ŹRÓDŁOWE)
    # =====================================================
    if args.save_db and not args.dry_run:
        save_source_invoices(df_new, company)
    else:
        logging.info("[INFO] Pominięto zapis do DB.")

    # =====================================================
    # 8️ FAKTUROWNIA + UPDATE DB
    # =====================================================
    if args.wystaw:
        if args.dry_run:
            logging.info("[DRY-RUN] Pominięto wystawianie FV.")
        else:
            if not args.issue_date:
                raise ValueError("--issue-date jest wymagane przy --wystaw")

            issue_date = parse_issue_date(args.issue_date)
            dept_id = DEPARTMENT_ID.get(company)
            if not dept_id:
                raise ValueError(f"Brak department_id dla {company}")

            wyniki = dodaj_faktury(
                company,
                items,
                dept_id,
                issue_date
            )

            source_ids_by_nip = get_source_ids_for_df(df_new, company)

            for w in wyniki:
                if not w.get("ok"):
                    continue
                nip = int(clean_nip(w["nip"]))
                fid = int(w["id"])
                fno = w.get("number")

                ids = source_ids_by_nip.get(nip, [])
                if not ids:
                    logging.warning(f"[DB] Brak id_faktury do oznaczenia dla NIP={nip}")
                    continue

                mark_as_used_by_ids(ids, fid, fakturownia_numer=fno)

    # =====================================================
    # 9 ARCHIWUM PDF
    # =====================================================
    if args.save_invoices:
        suffix = COMPANY_SUFFIX.get(company)
        if not suffix:
            raise ValueError(f"Brak COMPANY_SUFFIX dla {company}")

        get_faktur(
            args.issue_date or date.today().isoformat(),
            datetime.today().isoformat(),
            suffix
        )

    logging.info("[DONE] Cały proces zakończony poprawnie.")