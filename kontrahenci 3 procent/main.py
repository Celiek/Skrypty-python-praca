import logging
import os
from argparse import ArgumentParser
from datetime import date, datetime

import pandas as pd
from dotenv import load_dotenv

from db_ops import (
    insert_new_invoices_from_xlsx,
    get_addresses_from_db,
    get_names_from_db_for_nips,
)
from fakturownia_api import dodaj_faktury, get_faktur
from reports import export_grouped_excels
from utils import clean_nip, db_conn, clean_df, parse_date_series

# ===== logging =====
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ===== env =====
load_dotenv()
if not os.getenv("API_KEY"):
    raise RuntimeError("[API] Brak API_KEY w pliku .env!")

DEPARTMENT_ID = {
    "shumee": 1705441,
    "greatstore": 1705454,
    "extrastore": 1705460,
}
SPECIAL_2PROC = {"6020134043"}  # 2%

# w zalężności od podanej nazwy spółki pobierane są
# faktury tylko dla jednej spółki
COMPANY_SUFFIX = {
    "shumee": ("/SM", "/TSM3"),
    "greatstore": ("/GS",),
    "extrastore": ("/EX",)
}

def parse_issue_date(arg_val: str | None) -> date:
    if not arg_val:
        return date.today()
    return datetime.strptime(arg_val, "%Y-%m-%d").date()


# ============================================================
# GŁÓWNA FUNKCJA LOGIKI
# ============================================================
def build_items_from_merchants_and_invoices(df_faktury, df_merch, adresy_z_bazy):
    logging.info("[INFO] Rozpoczynam filtrowanie faktur do prowizji...")

    df_faktury = df_faktury.copy()
    df_merch = df_merch.copy()

    # Konwersje dat i NIP
    df_faktury["Data wystawienia"] = parse_date_series(df_faktury["Data wystawienia"])
    df_faktury["NIP"] = df_faktury["NIP"].astype(str).apply(clean_nip)

    df_merch["NIP"] = df_merch["NIP"].astype(str).apply(clean_nip)
    if "Od kiedy prowizja 3%" in df_merch.columns:
        df_merch["Od kiedy prowizja 3%"] = parse_date_series(df_merch["Od kiedy prowizja 3%"])
    else:
        df_merch["Od kiedy prowizja 3%"] = pd.NaT

    # Konwersja kwot
    for col in ["Netto", "VAT", "Brutto"]:
        df_faktury[col] = (
            df_faktury[col].astype(str).str.replace(",", ".", regex=False)
        )
        df_faktury[col] = pd.to_numeric(df_faktury[col], errors="coerce").fillna(0)

    today_ts = pd.Timestamp(date.today())
    min_start = pd.Timestamp(2025, 1, 1)

    # Filtr kontrahentów z ważną datą startu
    df_merch_valid = df_merch[
        df_merch["Od kiedy prowizja 3%"].notna()
        & (df_merch["Od kiedy prowizja 3%"] >= min_start)
    ].copy()

    nazwy_map = dict(zip(df_merch_valid["NIP"], df_merch_valid.get("Nazwa", "")))
    email_map = dict(zip(df_merch_valid["NIP"], df_merch_valid.get("email", "")))
    start_map = dict(zip(df_merch_valid["NIP"], df_merch_valid["Od kiedy prowizja 3%"]))

    names_db = get_names_from_db_for_nips(list(start_map.keys()))
    logging.info(f"[DEBUG] nazwy pobrane z DB: {len(names_db)} rekordów")

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

        if fv.empty:
            logging.debug(f"[FV] {nip} → brak faktur w pliku wejściowym")
        else:
            logging.debug(f"[FV] {nip} → znaleziono {len(fv)} faktur w wejściu")

        for _, row in fv.iterrows():
            d = row["Data wystawienia"]

            if pd.isna(d):
                continue

            # 1) pomijamy wszystko przed effective_start
            if d < effective_start:
                continue

            # 2) aktualny miesiąc rozliczenia → nie liczymy
            if d.year == current_year and d.month == current_month:
                continue

            # 3) jeśli chcesz — tu możesz ewentualnie przywrócić filtr przyszłości
            # if d > today_ts:
            #     continue

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

    # Zbieramy wszystkie zaakceptowane wiersze do jednego DF
    if accepted_frames:
        accepted_all_df = pd.concat(accepted_frames, ignore_index=True)
    else:
        accepted_all_df = pd.DataFrame(columns=df_faktury.columns)

    logging.info(f"[DEBUG] Łącznie zaakceptowanych wierszy do raportu: {len(accepted_all_df)}")

    return items, accepted_all_df


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("input")
    parser.add_argument("-c", "--company", required=True)
    parser.add_argument("--filter-xlsx", required=True)
    parser.add_argument("--save-db", action="store_true")
    parser.add_argument("--report-only", action="store_true")
    parser.add_argument("--issue-date")
    parser.add_argument("--save-invoices", action="store_true")
    parser.add_argument("--wystaw",action="store_true")

    args = parser.parse_args()
    company = args.company.lower().strip()

    # 1️⃣ Wczytanie danych
    df = clean_df(pd.read_excel(args.input))
    df["Data wystawienia"] = parse_date_series(df["Data wystawienia"])
    df["NIP"] = df["NIP"].astype(str).apply(clean_nip)

    df["Netto"] = pd.to_numeric(df["Netto"], errors="coerce")
    df["VAT"] = pd.to_numeric(df["VAT"], errors="coerce")
    df["Brutto"] = pd.to_numeric(df["Brutto"], errors="coerce")

    df = df.dropna(subset=["Netto", "Brutto","VAT"])  # usuń wiersze-śmieci
    #
    # for col in ["Netto", "VAT", "Brutto"]:
    #     df[col] = (
    #         df[col]
    #         .astype(str)
    #         .str.replace(",", ".", regex=False)
    #         .str.replace(" ", "", regex=False)
    #     )
    #     df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    #
    # duplikaty_idx = []
    #
    # with db_conn() as conn, conn.cursor() as cur:
    #     cur.execute("""
    #         SELECT
    #             nip,
    #             numer_faktury,
    #             CAST(kwota_netto AS DECIMAL(12,2)) AS kwota_netto,
    #             CAST(kwota_vat AS DECIMAL(12,2)) AS kwota_vat,
    #             CAST(kwota_brutto AS DECIMAL(12,2)) AS kwota_brutto,
    #             nazwa_spolki
    #         FROM faktury_do_prowizji
    #         WHERE nazwa_spolki = %s
    #     """, (company,))
    #
    #     existing = set()
    #
    #     #Debug
    #     print(existing)
    #
    #     for nip, nr, net, vat, brut, spol in cur.fetchall():
    #         # zaokrąglamy do 2 miejsc, aby ignorować różnice 0.01 zł
    #         existing.add((
    #             str(nip),
    #             str(nr),
    #             round(float(net), 2),
    #             round(float(vat), 2),
    #             round(float(brut), 2),
    #         ))
    #
    # duplikaty_idx = []
    # for i, row in df.iterrows():
    #     key = (
    #         str(row["NIP"]),
    #         str(row["Numer dokumentu"]).strip(),
    #         round(float(row["Netto"]), 2),
    #         round(float(row["VAT"]), 2),
    #         round(float(row["Brutto"]), 2),
    #     )
    #     if key in existing:
    #         duplikaty_idx.append(i)
    #
    # if duplikaty_idx:
    #     logging.warning(f"[DUPLIKATY] Pominięto {len(duplikaty_idx)} duplikatów faktur wg DB.")
    #     df = df.drop(duplikaty_idx).reset_index(drop=True)
    # else:
    #     logging.info("[DUPLIKATY] Brak duplikatów w DB — wszystkie faktury nowe.")

    if df.empty:
        logging.info("[DONE] Brak faktur do dalszego przetwarzania.")
        exit(0)

    # Wczytanie listy kontrahentów (filtr prowizji)
    kontr_df = pd.read_excel(args.filter_xlsx)
    kontr_df["NIP"] = kontr_df["NIP"].astype(str).apply(clean_nip)
    kontr_df["Od kiedy prowizja 3%"] = parse_date_series(
        kontr_df["Od kiedy prowizja 3%"]
    )

    addresses = get_addresses_from_db()

    # Budowa FV prowizyjnych (lista items)
    items, df_for_reports = build_items_from_merchants_and_invoices(df, kontr_df, addresses)

    if not items:
        logging.info("[DONE] Brak FV prowizyjnych do wystawienia.")
        exit(0)
    else:
        print("[INFO] Można wygenerować faktury prowizyjne")

    # prostszy log wygenerowanych raportów

    nip_col = "NIP" if "NIP" in df_for_reports.columns else "Nip"
    logging.info(f"[REPORT] Raporty wygenerowane dla {len(df_for_reports[nip_col].unique())} kontrahentów.")

    # generowanie raportów
    export_grouped_excels(df_for_reports, spolka=company, out_root="raporty_xlsx")

    df_summary = df_for_reports.copy()

    # Dodaj kolumnę prowizji 3%
    df_summary["Kwota 3% Netto"] = (df_summary["Netto"].astype(float) * 0.03).round(2)

    # Kolumny docelowe i ich kolejność
    cols_summary = [
        "Data wystawienia",
        "Kontrahent",
        "NIP",
        "Numer dokumentu",
        "Netto",
        "VAT",
        "Brutto",
        "Kwota 3% Netto"
    ]

    df_summary = df_summary[cols_summary]

    # Ścieżka zapisu
    from datetime import date

    data_today = date.today().isoformat()
    dir_out = os.path.join("raporty_xlsx", company.lower(), data_today)
    os.makedirs(dir_out, exist_ok=True)
    path_summary = os.path.join(dir_out, "raport_zbiorczy.xlsx")

    df_summary.to_excel(path_summary, index=False)

    logging.info(f"[XLSX] Raport zbiorczy zapisano: {path_summary}")

    # Jeśli tylko raport → kończymy tutaj (bez Fakturowni, bez DB)
    if args.report_only:
        logging.info(
            "[REPORT-ONLY] Tylko wygenerowano raporty — bez wystawiania faktur."
        )
        logging.info("=== Zakończono zapis raportów faktur ===")

    # 7️⃣ Wystaw FV przez API jeśli musisz
    if args.wystaw:
        issue_date = parse_issue_date(args.issue_date)
        dept_id = DEPARTMENT_ID.get(company)
        wyniki = dodaj_faktury(company, items, dept_id, issue_date)

    # zapisz wystawione faktury na dysku
    # nawet jeśli ich nie wystawiasz
    if args.save_invoices:
        suffix = COMPANY_SUFFIX.get(args.company)

        if not suffix:
            raise ValueError(
                f"[ERROR-COMPANY] Nieznana spółka: {args.company}. "
                f"Dostępne: {', '.join(COMPANY_SUFFIX)}"
            )

        get_faktur(
            args.issue_date,
            datetime.today().isoformat(),
            suffix
        )


    # 8️⃣ Na sam koniec: zapisz brakujące faktury źródłowe
    logging.info("=== Zakończono działanie programu ===")
    logging.info("[DONE] Generowanie raportów zakończone.")
    exit(0)
