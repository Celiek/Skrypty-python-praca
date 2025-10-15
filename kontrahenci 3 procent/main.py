import logging
import os
import re
import smtplib
import ssl
from email.mime.application import MIMEApplication
from pathlib import Path

import unicodedata
from argparse import ArgumentParser
from datetime import datetime, timedelta, date
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from decimal import Decimal, ROUND_HALF_UP
from typing import List, Dict, Tuple, Optional
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter

import pandas as pd
import psycopg2
import requests
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor
import csv

# TODO
# Dodać wykorzystanie bazy danych do wyszukiwania fakturowania kontrahentów
# Dodać opcję zapisu faktur od kontrahentów do bazy danychq

#dodać logowanie duplikatów DONE

# zmienić nazwę pozycji DONE
# zmienić nazwę spółki DONE
# zmienić treść DONE
# tytuł : Faktura prowizyjna 3% od sprzedanych faktur - spółka supermerchant s.p.a DONE
# zmienić nagłówek wiadomości : Prowizja 3% od sprzedanych towarów DONE
# zmiana numeru faktury DONE
# nie faktury tylko dokumenty księgowe DONE
# usunąć logo DONE
# dodac tryb tylko wystaw faktury DONE

# TODO 2:
# zmienić stawkę z 3 na 2 % dla leobert DONE
# dodać wstawianie adresu kontrahenta na fakturę DONE

# =========================
# Konfiguracja / stałe
# =========================
load_dotenv()

API_KEY = os.getenv("API_KEY")
OUTPUT_ENCODING = os.getenv("OUTPUT_ENCODING", "utf-8-sig")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", ".")
os.makedirs(OUTPUT_DIR, exist_ok=True)

EMAIL_HTML_TEMPLATE =""" <!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>Faktura 3%</title>
</head>
<body style="margin:0; padding:0; background-color:#ffffff; font-family: Arial, sans-serif;">
  <!-- NAGŁÓWEK -->
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" align="center" style="max-width:600px; margin:auto; background:#f7fbfc;">
    <tr>
      <td align="center" style="padding:20px; font-size:20px; font-weight:bold; color:#000000;">
       Prowizja 3% od sprzedanych towarów
      </td>
    </tr>
    <tr>
      <td align="center" style="padding:0 20px 20px; font-size:14px; line-height:20px; color:#333333;">
        Szanowni Państwo,<br>
        Przesyłamy rozliczenie prowizyjne za ubiegły miesiąc wraz z zestawieniem faktur,których dotyczy prowizja.
        Faktury stanowią podstawę do rozliczenia prowizyjnego zgodnie z zaakceptowanym regulaminem współpracy Super Merchant.<br>
      </td>
    </tr>
  </table>

  <!-- STOPKA -->
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" align="center" style="max-width:600px; margin:auto;">
    <tr>
      <td align="center" style="padding:20px; font-size:14px; line-height:20px; color:#333333;">
        Jeśli masz jakieś pytania skontaktuj się z nami:<br>
        <a href="mailto:contact@supermerchant.base.com" style="color:#0077DA; text-decoration:none;">
          contact@supermerchant.base.com
        </a>
      </td>
    </tr>
  </table>

</body>
</html>"""

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}
# lista kontrahentów którzy są fakturowani na kwotę 2 procent
SPECIAL_2PROC_NIPS = {"6020134043"}

COMPANIES = {
    "shumee": {
        "name_addr": os.getenv("SHUMEE_NAME_ADDR", "Shumee Sp. z.o.o."),
        "name" : os.getenv("NAZWA","SHUMEE"),
        "nrb": os.getenv("SHUMEE_NRB", "07114011080000314718001007"),
        "bank_code": os.getenv("SHUMEE_BANK_CODE", "11401108"),
        "email": os.getenv("SHUMEE_EMAIL", "faktury@sm.base.com"),
        "server_host":os.getenv("SHUMEE_SERVER_HOST", "smtp.gmail.com"),
        "kontakt":os.getenv("SHUMEE_KONTAKT", "kontakt@shumee.pl"),
        "password":os.getenv("SHUMEE_PASS"),
        #"forbidden_names": ['MORELE.NET sp. z o.o','GLOBAL INCOME SPÓŁKA Z OGRANICZONĄ ODPOWIEDZIALNOŚCIĄ','MORELE.NET SPÓŁKA Z OGRANICZONĄ ODPOWIEDZIALNOŚCIĄ','LEOBERT SPÓŁKA Z OGRANICZONĄ ODPOWIEDZIALNOŚCIĄ SPÓŁKA KOMANDYTOWA']
    },
    "greatstore": {
        "name_addr": os.getenv("GREATSTORE_NAME_ADDR", "Greatstore Sp. z.o.o."),
        "name" : os.getenv("NAZWA","GREATSTORE"),
        "nrb": os.getenv("GREATSTORE_NRB", "18102055610000310200035501"),
        "bank_code": os.getenv("GREATSTORE_BANK_CODE", "10205561"),
        "email": os.getenv("GREATSTORE_EMAIL", "faktury@greatstore.pl"),
        "server_host":os.getenv("GREATSTORE_SERVER_HOST", "smtp.gmail.com"),
        "kontakt": os.getenv("GREATSTORE_KONTAKT", "kontakt@greatstore.pl"),
        "password":os.getenv("GREATSTORE_PASS"),
    },
    "extrastore": {
        "name_addr": os.getenv("EXTRASTORE_NAME_ADDR", "Extrastore Sp. z.o.o."),
        "name" : os.getenv("NAZWA","EXTRASTORE"),
        "nrb": os.getenv("EXTRASTORE_NRB", "05114020040000330280429939"),
        "bank_code": os.getenv("EXTRASTORE_BANK_CODE", "11402004"),
        "email": os.getenv("EXTRASTORE_EMAIL", "faktury_extra@shumee.pl"),
        "server_host":os.getenv("EXTRASTORE_SERVER_HOST", "smtp.gmail.com"),
        "kontakt": os.getenv("EXTRASTORE_KONTAKT", "kontakt@extrastore.pl"),
        "password":os.getenv("EXTRASTORE_PASS"),
    },
}

DEPARTMENT_ID = {
    "shumee": 1705441,
    "extrastore": 1705460,
    "greatstore": 1705454,
}

os.makedirs(OUTPUT_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

_WINDOWS_FORBIDDEN = set('<>:"/\\|?*')
_WINDOWS_RESERVED  = {
    "CON","PRN","AUX","NUL",
    *(f"COM{i}" for i in range(1,10)),
    *(f"LPT{i}" for i in range(1,10)),
}

# =========================
# Utils
# =========================
def db_conn():
    return psycopg2.connect(**DB_CONFIG)

def nip_digits(nip: str) -> str:
    cleaned = re.sub(r"\D", "", str(nip or ""))
    if len(cleaned) != 10:
        logging.warning("NIP ma nieprawidłową długość: %s → %s", nip, cleaned)
    return cleaned

def _slugify_filename(s: str, *, max_len: int = 60) -> str:
    """
    Tworzy bezpieczną nazwę pliku dla Windows/macOS/Linux:
    - podmienia niedozwolone znaki (w tym cudzysłów i apostrof) na '_',
    - zwija wielokrotne '_' w jedno,
    - usuwa kropki/spacje z końca,
    - unika nazw zarezerwowanych (CON/PRN/AUX/NUL/COM1../LPT1..).
    """
    if not s:
        return "plik"

    # normalizacja + usunięcie diakrytyków
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.strip()

    # wymiana wszystkich niedozwolonych na '_'
    s = "".join(("_" if ch in _WINDOWS_FORBIDDEN or ch in {"'", "`"} else ch) for ch in s)

    # przepuszczamy tylko [A-Za-z0-9_. -], resztę na '_'
    s = re.sub(r"[^A-Za-z0-9_. \-]", "_", s)

    # zwijanie wielokrotnych podkreślników/spacji
    s = re.sub(r"[ _]+", " ", s)
    s = s.replace(" ", "_")
    s = re.sub(r"_+", "_", s)

    # usunięcie kropek/spacji/podkreślników z początku/końca
    s = s.strip(" ._")

    # pusta po czyszczeniu?
    if not s:
        s = "plik"

    base_upper = s.upper()
    if base_upper in _WINDOWS_RESERVED:
        s = f"_{s}"

    # ograniczenie długości
    s = s[:max_len].rstrip(" ._")

    # jeszcze raz awaryjnie
    if not s:
        s = "plik"

    return s

def export_grouped_excels(df: pd.DataFrame, out_dir: str) -> dict[str, str]:
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    wanted = ["Kontrahent", "NIP", "Numer dokumentu", "Data", "Netto", "VAT", "Brutto"]
    cols = [c for c in wanted if c in df.columns]
    if not cols:
        raise ValueError("Brak kolumn do eksportu XLSX — sprawdź nazwy w DataFrame.")

    out_map: dict[str, str] = {}
    g = df.groupby("NIP", dropna=False, as_index=False)

    for nip, sub in g:
        nip_str = str(nip).strip()
        kontrahent = ""
        if "Kontrahent" in sub.columns and not sub["Kontrahent"].isna().all():
            kontrahent = str(sub["Kontrahent"].iloc[0] or "")

        sub = sub.copy()
        sub["Prowizja_3proc"] = (
            pd.to_numeric(sub["Netto"], errors="coerce").fillna(0) * 0.03
        ).round(2)

        # 🔹 suma prowizji — w każdej linii tej samej wartości
        suma_prowizji = sub["Prowizja_3proc"].sum().round(2)
        sub["Suma_prowizji"] = suma_prowizji

        # Zamiana kropek na przecinki (format PL)
        for col in ["Netto", "VAT", "Brutto", "Prowizja_3proc", "Suma_prowizji"]:
            if col in sub.columns:
                sub[col] = sub[col].apply(
                    lambda x: str(x).replace(".", ",") if pd.notna(x) and x != "" else ""
                )

        fname = f"{nip_str}_{_slugify_filename(kontrahent)}.xlsx"
        fpath = os.path.join(out_dir, fname)

        sub[cols + ["Prowizja_3proc", "Suma_prowizji"]].to_excel(
            fpath, index=False, sheet_name="Faktury"
        )
        out_map[nip_str] = os.path.abspath(fpath)
        logging.info("[XLSX] Zapisano raport kontrahenta: %s", fpath)

    return out_map


def prepare_recipients(rows_from_build: List[Dict], wyniki_faktur: List[Dict], attachments_by_nip: Dict[str, str]) -> pd.DataFrame:
    df_rows = pd.DataFrame(rows_from_build)
    df_rows.rename(columns={
        "buyer_tax_no": "nip",
        "buyer_email": "email",
        "buyer_name": "kontrahent"
    }, inplace=True)
    if not df_rows.empty:
        df_rows["nip"] = df_rows["nip"].astype(str).str.strip()

    df_res = pd.DataFrame(wyniki_faktur) if wyniki_faktur else pd.DataFrame([])
    if df_res.empty:
        df_res = pd.DataFrame(columns=["nip","link","ok"])
    else:
        df_res["nip"] = df_res["nip"].astype(str).str.strip()

    out = df_rows.merge(df_res[["nip","link","ok"]], on="nip", how="left")
    out = out.loc[out["ok"] == True].copy()

    # dołącz ścieżkę do CSV
    out["attachment_path"] = out["nip"].map(attachments_by_nip).fillna("")

    return out

def render_email_html(invoice_link: Optional[str], company_name: str) -> str:
    html = EMAIL_HTML_TEMPLATE
    link = invoice_link or "#"  # jeśli brak linku – pokaż przycisk bez odnośnika
    return html.replace("{INVOICE_LINK}", link).replace("{COMPANY_NAME}", company_name)

def _norm_doc_no(x: str) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip()
    s = re.sub(r"\s+", " ", s)
    return s.upper()

def find_duplicates(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    required = {"Numer dokumentu", "Netto", "VAT", "Brutto"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Brak kolumn: {', '.join(sorted(missing))}")
    d = df.copy()
    d["__doc_no_norm"] = df["Numer dokumentu"].map(_norm_doc_no)
    d["__netto_gr"] =(df["Netto"])
    d["__vat_gr"] = (df["VAT"])
    d["__brut_gr"] = (df["Brutto"])

    mdup = d.duplicated(subset=["__doc_no_norm", "__netto_gr", "__vat_gr", "__brut_gr"], keep="first")
    group_sizes = d.groupby(["__doc_no_norm", "__netto_gr", "__vat_gr", "__brut_gr"])["Numer dokumentu"].transform(
        "size")
    d["__is_dup_group"] = group_sizes > 1
    full_dup_groups = d.loc[d["__is_dup_group"]].copy()

    return d, full_dup_groups.sort_values(["__doc_no_norm", "__netto_gr", "__vat_gr", "__brut_gr"])

def handle_duplicates(df: pd.DataFrame, action="drop_keep_first", report_path: Optional[str] = None) -> pd.DataFrame:
    d, full_dups = find_duplicates(df)

    if not full_dups.empty:
        preview_cols = ["Numer dokumentu", "Netto", "VAT", "Brutto"]
        logging.warning("[DUP] Wykryto duplikaty:\n%s", full_dups[preview_cols].to_string(index=False))

        if report_path:
            full_dups[preview_cols].to_csv(report_path, index=False, encoding="utf-8-sig")
            logging.info("[DUP] Raport duplikatów zapisany: %s", report_path)

        if action in ("drop_keep_first", "drop_keep_last"):
            keep = "first" if action == "drop_keep_first" else "last"
            mask = d.duplicated(subset=["__doc_no_norm", "__netto_gr", "__vat_gr", "__brut_gr"], keep=keep)
            cleaned = df.loc[~mask].copy()
            logging.info("[DUP] Usunięto %d zduplikowanych wierszy (%s).", mask.sum(), action)
            return cleaned
        elif action == "warn":
            return df
        elif action == "error":
            raise ValueError("W pliku znajdują się duplikaty (patrz log powyżej).")
    else:
        logging.info("[DUP] Nie znaleziono duplikatów.")

    return df

def fetch_statusy_kontrahentow(nipy: List[str]) -> Dict[str, str]:
    """SELECT nip, status FROM merchanci WHERE nip IN (...)"""
    nums = [re.sub(r"\D", "", str(n)) for n in nipy if n]
    nums = [n for n in nums if n]
    if not nums:
        return {}
    placeholders = ",".join(["%s"] * len(nums))

    query = f"""
           SELECT nip, status
           FROM merchanci
           WHERE nip IN ({placeholders})
             AND (status = 'merchant' OR status = 'to-skomplikowane')
       """

    result = {}

    with db_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, tuple(nums))
        for row in cur.fetchall():
            result[str(row["nip"])] = row["status"]
    return result

# def fetch_emails(nipy) -> pd.DataFrame:
#     """Zwraca DF kolumny: nip, email (dla listy NIP-ów)."""
#     if isinstance(nipy, pd.Series):
#         nipy = nipy.dropna().astype(str).str.strip().unique().tolist()
#     elif isinstance(nipy, pd.DataFrame):
#         nipy = nipy["NIP"].dropna().astype(str).str.strip().unique().tolist()
#     elif isinstance(nipy, (list, tuple)):
#         nipy = [str(n).strip() for n in nipy if n]
#     else:
#         nipy = [str(nipy).strip()]
#
#     if not nipy:
#         print("Nie dosłałeś żadnych emaili !" + nipy)
#         return pd.DataFrame(columns=["nip", "email"])
#
#     query = "SELECT nip, email FROM merchanci WHERE nip = ANY(%s::bigint[])"
#     with db_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
#         cur.execute(query, (nipy,))
#         rows = cur.fetchall()
#     print("Emaile z bazy danych:")
#     print(rows)
#     return pd.DataFrame(rows)

def build_full_report(df: pd.DataFrame,
                      recipients_df: pd.DataFrame,
                      mail_results: list[dict],
                      attachments_by_nip: dict[str, str],
                      output_file: str):
    """
    Raport zbiorczy:
    - 1 wiersz per kontrahent (NIP)
    - dane: nazwa, nip, email, suma Netto/VAT/Brutto, ilość dokumentów, numery dokumentów, status maila
    - dodatkowy plik z sumą globalną
    """

    df = df.copy()
    df["NIP_clean"] = df["NIP"].astype(str).map(_only_digits)

    recipients_df = recipients_df.copy()
    recipients_df["nip_clean"] = recipients_df["nip"].astype(str).map(_only_digits)

    # agregacja dokumentów per NIP
    docs_grouped = (
        df.groupby("NIP_clean")
        .agg(
            Netto=("Netto", "sum"),
            VAT=("VAT", "sum"),
            Brutto=("Brutto", "sum"),
            ilosc_dokumentow=("Numer dokumentu", "count"),
            dokumenty=("Numer dokumentu", lambda x: " | ".join(map(str, x)))
        )
        .reset_index()
        .rename(columns={"NIP_clean": "nip_clean"})  # 🔹 DODAJ TO
    )

    # mail results -> DF
    mail_df = pd.DataFrame(mail_results or [])
    if not mail_df.empty:
        mail_df.rename(columns={"email": "Email", "ok": "Wyslano_OK"}, inplace=True)
    else:
        mail_df = pd.DataFrame(columns=["Email", "Wyslano_OK"])

    # scalanie
    raport = (
        recipients_df
        .merge(docs_grouped, on="nip_clean", how="left")
        .merge(mail_df[["Email", "Wyslano_OK"]], left_on="email", right_on="Email", how="left")
    )

    # załączniki
    att_clean = {_only_digits(k): v for k, v in (attachments_by_nip or {}).items()}
    raport["attachment_path"] = raport["nip_clean"].map(att_clean).fillna("")

    # zaokrąglenia
    for c in ["Netto", "VAT", "Brutto"]:
        raport[c] = pd.to_numeric(raport[c], errors="coerce").fillna(0).round(2)
    # zapis raportu szczegółowego
    base, ext = os.path.splitext(output_file)
    raport_path = f"{base}_full{ext or '.csv'}"
    raport.to_csv(raport_path, index=False, encoding=OUTPUT_ENCODING, sep=";")
    logging.info("[SAVE] Raport pełny: %s", raport_path)

    # raport zbiorczy (sumy globalne)
    summary = {
        "suma_netto": raport["Netto"].sum(),
        "suma_vat": raport["VAT"].sum(),
        "suma_brutto": raport["Brutto"].sum(),
        "suma_dokumentow": raport["ilosc_dokumentow"].sum(),
        "ilosc_kontrahentow": raport.shape[0],
        "data": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    summary_path = f"{base}_summary{ext or '.csv'}"
    pd.DataFrame([summary]).to_csv(summary_path, index=False, encoding=OUTPUT_ENCODING, sep=";")
    logging.info("[SAVE] Raport zbiorczy (sumy globalne): %s", summary_path)

    return raport, raport_path, summary_path

def build_recipients_report_only(df, recipients_df, mail_results, attachments_by_nip, output_file):
    # oczyszczanie nipu
    df = df.copy()
    recipients_df = recipients_df.copy()

    df["NIP_clean"] = df["NIP"].astype(str).map(_only_digits)
    recipients_df["nip_clean"] = recipients_df["nip"].astype(str).map(_only_digits)

    # 2 agregacja po nipie
    recipients_unique = (
        recipients_df
        .sort_values(["nip_clean", "email"])              # deterministycznie
        .drop_duplicates(subset=["nip_clean"], keep="first")
    )

    # sumyz pliku wejściowego tylko dla NIP-ów z pliku odbiorców
    sums = (
        df[df["NIP_clean"].isin(recipients_unique["nip_clean"])]
        .groupby("NIP_clean", as_index=False)
        .agg(Netto=("Netto", "sum"),
             VAT=("VAT", "sum"),
             Brutto=("Brutto", "sum"))
    )

    # załączniki ozyszczanie kluczy w mapie i zamiana na DF, żeby merge był jednoznaczny
    att_clean = { _only_digits(k): v for k, v in (attachments_by_nip or {}).items() }
    att_df = (pd.DataFrame({
                "nip_clean": list(att_clean.keys()),
                "attachment_path": list(att_clean.values())
             })
             if att_clean else pd.DataFrame(columns=["nip_clean","attachment_path"])
    )

    # 5) Wyniki wysyłki (po emailu) – też nie duplikuj
    mail_df = pd.DataFrame(mail_results or [])
    if not mail_df.empty:
        mail_df.rename(columns={"email": "Email", "ok": "Wyslano_OK"}, inplace=True)
        mail_df = (mail_df
                   .sort_values(["Email"])
                   .drop_duplicates(subset=["Email"], keep="last"))  # ostatni status wysłania emaila
    else:
        mail_df = pd.DataFrame(columns=["Email","Wyslano_OK"])

    # 6) Złóż raport:
    # recipients_unique (nip, email, kontrahent, link, attachment_path?)
    # + sums (Netto, VAT, Brutto)
    # + att_df (attachment_path – nadpisze to z recipients, jeśli było puste)
    # + mail_df (Wyslano_OK, Blad)
    raport_recipients = (
        recipients_unique
        .merge(sums, left_on="nip_clean", right_on="NIP_clean", how="left")
        .merge(att_df, on="nip_clean", how="left", suffixes=("", "_from_dict"))
        .merge(mail_df[["Email", "Wyslano_OK"]], left_on="email", right_on="Email", how="left")
    )

    # Jeśli w recipients już był attachment_path, nie nadpisuj go pustym z att_df
    if "attachment_path_from_dict" in raport_recipients.columns:
        raport_recipients["attachment_path"] = raport_recipients["attachment_path"].where(
            raport_recipients["attachment_path"].notna() & (raport_recipients["attachment_path"] != ""),
            raport_recipients["attachment_path_from_dict"]
        )
        raport_recipients.drop(columns=["attachment_path_from_dict"], inplace=True)

    # Porządki i zaokrąglenia
    raport_recipients["Netto"] = raport_recipients["Netto"].fillna(0).round(2)
    raport_recipients["VAT"] = raport_recipients["VAT"].fillna(0).round(2)
    raport_recipients["Brutto"] = raport_recipients["Brutto"].fillna(0).round(2)

    # Usuń pomocnicze kolumny i dublujący się Email
    raport_recipients.drop(columns=[c for c in ["NIP_clean", "NIP", "Email"] if c in raport_recipients.columns],
                           inplace=True, errors="ignore")

    # 7) Zapis – tylko dla recipients, jeden wiersz per NIP
    base, ext = os.path.splitext(output_file)
    out_path = f"{base}_recipients{ext or '.csv'}"
    raport_recipients.to_csv(out_path, index=False, encoding=OUTPUT_ENCODING, sep=";")
    logging.info("[SAVE] Raport tylko dla recipients (1 wiersz = 1 NIP): %s", out_path)

    return raport_recipients, out_path

# =========================
# Fakturownia helpers
# =========================
def lista_faktur_sm3() -> List[dict]:
    """Zwraca faktury z bieżącego miesiąca, których number kończy się na '/sm3'."""
    url = "https://shumee.fakturownia.pl/invoices.json"
    params = {
        "period": "this_month",
        "api_token": API_KEY,
        "page": 1,
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    return [f for f in data if str(f.get("number", "")).endswith("/SM","/GS","/EX")]

def _to_decimal(val: str) -> Decimal:
    """Konwersja string/liczby na Decimal z kropką jako separatorem."""
    s = str(val).strip().replace(",", ".")
    try:
        return Decimal(s)
    except:
        return Decimal("0.00")

def build_invoice_rows(df: pd.DataFrame, recipients_df: Optional[pd.DataFrame] = None) -> List[Dict]:
    """
    Przygotowuje rekordy do wystawienia faktur:
    - agreguje po NIP,
    - liczy stawkę 3% netto i brutto zaokrąglenie po rzędach,
    - email bierze z recipients_df (jeśli jest podany).
    """

    df["Netto"] = (
        df["Netto"]
        .astype(str)
        .str.replace(",", ".", regex=False)
        .map(lambda x: Decimal(x) if x not in ("", "nan", "None", "") else Decimal("0"))
    )

    grouped = (
        df.groupby("NIP", as_index=False)
        .agg({"Netto": "sum", "Kontrahent": "first"})
    )

    grouped = grouped[grouped["Netto"] > 0]

    # obliczanie wartości netto i brutto na fakturze
    grouped["stawka_proc"] = grouped["NIP"].astype(str).apply(
        lambda nip: Decimal("0.02") if nip in SPECIAL_2PROC_NIPS else Decimal("0.03")
    )
    grouped["stawka_netto"] = grouped["Netto"].apply(
        lambda x: Decimal(x).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    ) * grouped["stawka_proc"]

    grouped["stawka_brutto_3p"] = [
        (n * Decimal("1.22") if p == Decimal("0.02") else n * Decimal("1.23")).quantize(Decimal("0.0001"),
                                                                                        rounding=ROUND_HALF_UP)
        for n, p in zip(grouped["stawka_netto"], grouped["stawka_proc"])
    ]

    # 4) Przygotowanie wyników
    rows = []
    for _, r in grouped.iterrows():
        email = None
        if recipients_df is not None:
            rec = recipients_df.loc[recipients_df["nip"] == str(r["NIP"])]
            if not rec.empty:
                email = rec["email"].iloc[0]

        rows.append({
            "buyer_name": str(r["Kontrahent"]).strip(),
            "buyer_tax_no": str(r["NIP"]).strip(),
            "buyer_email": email,
            "amount_net": str(r["stawka_netto_3p"]),  # typ decimal ( dokłądność do 4 miejsc po przecinku)
            "amount_gross": str(r["stawka_brutto_3p"]),
        })

    # DEBUG
    print("=== DEBUG build_invoice_rows ===")
    for _, r in grouped.iterrows():
        print(
            f"NIP={r['NIP']} | Netto SUM={r['Netto']} | "
            f"stawka 3% netto={r['stawka_netto_3p']} | "
            f"stawka 3% brutto={r['stawka_brutto_3p']}"
        )
    print("================================")

    return rows

def get_invoice_public_url(invoice_id: int, api_key: str) -> Optional[str]:
    """
    Zwraca publiczny link (np. 'view_url' lub 'public_url') do faktury w Fakturowni.
    """
    url = f"https://shumee.fakturownia.pl/invoices/{invoice_id}.json"
    try:
        r = requests.get(url, params={"api_token": api_key}, timeout=30)
        r.raise_for_status()
        data = r.json()

        for key in ("view_url", "public_url", "print_url", "download_url"):
            if data.get(key):
                return data[key]
    except Exception as e:
        logging.error("Nie udało się pobrać linku do faktury id=%s: %s", invoice_id, e)
    return None


def send_Email(spolka: str,
               recipents_df: pd.DataFrame,
               *,
               subject: Optional[str] = None,
               dry_run: bool = False) -> list[dict]:

    cfg = get_spolka_config(spolka)
    from_addr = cfg["email"]
    password = cfg["password"]
    # na czas debugowania zmieniono hosta na localhost
    host = cfg["server_host"]
    #host = "127.0.0.1"
    # na czas debugowania zmieniono port na 1025
    port = int(os.getenv("SMTP_PORT","465"))
    #port  = 1025

    # na rzecz debugownaia zakomentowane
    use_ssl = os.getenv("SMTP_USE_SSL", "1") == "1"
    #use_ssl = False

    subject = f"Faktura prowizyjna 3% od sprzedanych faktur" + cfg["name_addr"]

    company_name = cfg.get("name") or spolka.upper()
    if not subject:
        subject = f"Faktura prowizyjna 3% od sprzedanych faktur" + cfg["name_addr"]

    results = []

    if recipents_df is None or recipents_df.empty:
        logging.info("[MAIL] Brak odbiorców do wysyłki.")
        return results

    context = ssl.create_default_context()
    server = None

    try:
        if use_ssl:
            server = smtplib.SMTP_SSL(host=host, port=port, context=context, timeout=60)
            server.ehlo()
        else:
            server = smtplib.SMTP(host=host, port=port, timeout=60)
            server.ehlo()
            server.starttls(context=context)
            server.ehlo()

        if os.getenv("SMTP_DEBUG", "0") == "1":
            server.set_debuglevel(1)

        if from_addr and password:
            server.login(from_addr, password)

        for row in recipents_df.itertuples(index=False):
            email_to = (getattr(row, "email", None) or "").strip()
            invoice_link = (getattr(row, "link", None) or getattr(row, "invoice_link", None) or "").strip()
            kontrahent = (getattr(row, "kontrahent", "") or "").strip()

            if not email_to:
                results.append({"email": None, "ok": False, "error": "Brak adresu email"})
                logging.warning("[SKIP] %s pominięty – brak adresu", email_to or "-")
                continue
            if not invoice_link:
                results.append({"email": email_to, "ok": False, "error": "Brak linku do faktury"})
                logging.warning("[SKIP] %s pominięty – brak linku do faktury", email_to)
                continue

            # składanie wiadomości
            html_body = render_email_html(invoice_link, company_name)
            msg = MIMEMultipart()  # mixed
            msg["Subject"] = subject
            msg["From"] = from_addr
            msg["To"] = email_to

            alt = MIMEMultipart("alternative")
            alt.attach(MIMEText(html_body, "html", "utf-8"))
            msg.attach(alt)

            paths = []
            ap_list = getattr(row, "attachment_paths", None)
            if isinstance(ap_list, list):
                paths.extend([p for p in ap_list if p])

            ap_single = (getattr(row, "attachment_path", "") or "").strip()
            if ap_single:
                paths.append(ap_single)

            ap_strlist = getattr(row, "attachment_paths_str", None)
            if isinstance(ap_strlist, str) and ap_strlist.strip():
                paths.extend([p.strip() for p in ap_strlist.split(";") if p.strip()])

            #  Dołączainie obu plików
            for p in paths:
                if not p:
                    continue
                if not os.path.isfile(p):
                    logging.warning("[MAIL] Załącznik nie istnieje: %s", p)
                    continue
                with open(p, "rb") as f:
                    part = MIMEApplication(f.read())
                filename = os.path.basename(p)
                part.add_header("Content-Disposition", "attachment", filename=filename)
                msg.attach(part)

            # 4) Wyślij
            try:
                server.sendmail(from_addr, [email_to], msg.as_string())
                logging.info("[MAIL] OK -> %s (kontrahent: %s)", email_to, kontrahent or "-")
                results.append({"email": email_to, "ok": True})
            except Exception as e:
                logging.error("[MAIL] BŁĄD -> %s: %s", email_to, e)
                results.append({"email": email_to, "ok": False, "error": str(e)})
    finally:
        if server:
            try:
                server.quit()
            except Exception:
                pass

    try:
        pd.DataFrame(results).to_csv("mail_debug.csv", index=False, sep=";", encoding="utf-8-sig")
        logging.info("[DEBUG] Zapisano szczegóły maili do mail_debug.csv")
    except Exception as e:
        logging.warning("[DEBUG] Nie udało się zapisać mail_debug.csv: %s", e)

    return results

def build_recipients_send_only(df: pd.DataFrame,
                               recipients_list: Optional[pd.DataFrame],
                               attachments_by_nip: Dict[str, str]) -> pd.DataFrame:
    """
    Zwraca DF z kolumnami: nip, email, kontrahent, link, attachment_path
    - scalając dane z pliku faktur (df) i pliku recipients (NIP,email,...).
    """
    if recipients_list is None or recipients_list.empty:
        raise ValueError("Brak pliku recipients – nie ma skąd wziąć maili!")

    rl = recipients_list.copy()
    rl["nip_clean"] = rl["nip"].astype(str).apply(_only_digits)

    base = df.copy()
    base["nip_clean"] = base["NIP"].astype(str).apply(_only_digits)

    merged = base.merge(rl, on="nip_clean", how="inner")
    kontrahent_col = "Kontrahent" if "Kontrahent" in merged.columns else (
        "kontrahent" if "kontrahent" in merged.columns else None)

    out = pd.DataFrame({
        "nip": merged["nip_clean"].astype(str),
        "email": merged["email"].astype(str),
        "kontrahent": merged[kontrahent_col].astype(str) if kontrahent_col else "",
        "link": (
            merged["link"] if "link" in merged.columns else pd.Series([""] * len(merged), index=merged.index)).astype(
            str),
    })

    out["attachment_path"] = out["nip"].map(attachments_by_nip).fillna("")
    out = out[out["email"].str.contains(r"@")]
    return out



def dodaj_faktury(spolka: str, items: List[Dict], sell_date: Optional[str] = None) -> List[Dict]:

    if spolka not in DEPARTMENT_ID:
        raise ValueError(f"Nieznana spółka: {spolka}")
    dept_id = DEPARTMENT_ID[spolka]

    today = datetime.today()
    payment_to = today + timedelta(days=14)
    miesiace = {1:"styczeń",2:"luty",3:"marzec",4:"kwiecień",5:"maj",6:"czerwiec",
                7:"lipiec",8:"sierpień",9:"wrzesień",10:"październik",11:"listopad",12:"grudzień"}
    poprzedni = today - relativedelta(months=1)

    url = "https://shumee.fakturownia.pl/invoices.json"
    headers = {"Accept": "application/json", "Content-Type": "application/json"}

    results = []
    with requests.Session() as s:
        s.headers.update(headers)
        for it in items:
            payload = {
                "api_token": API_KEY,
                "invoice": {
                    "kind": "vat",
                    "number": None,
                    "sell_date":  (sell_date or today.strftime("%Y-%m-%d")),
                    "issue_date": today.strftime("%Y-%m-%d"),
                    "payment_to": payment_to.strftime("%Y-%m-%d"),
                    "buyer_name":   it["buyer_name"],
                    "buyer_tax_no": it["buyer_tax_no"],
                    "department_id": DEPARTMENT_ID[spolka],
                    **({"buyer_email": it["buyer_email"]} if it.get("buyer_email") else {}),
                    "positions": [{
                        "name": f"Prowizja 3% od sprzedanych towarów za okres {miesiace[poprzedni.month]} {today.year}",
                        "tax": 23,
                        "total_price_gross": it["amount_gross"],
                        "quantity": 1
                    }]
                }
            }
            try:
                r = s.post(url, json=payload, timeout=30)
                if 200 <= r.status_code < 300:
                    data = r.json()
                    inv_id = data.get("id")
                    link = get_invoice_public_url(inv_id, API_KEY) if inv_id else None
                    results.append({"nip": it["buyer_tax_no"], "ok": True, "id": inv_id, "link": link})
                else:
                    results.append({"nip": it["buyer_tax_no"], "ok": False,
                                    "error": f"{r.status_code} {r.text[:500]}"})
            except Exception as e:
                results.append({"nip": it["buyer_tax_no"], "ok": False, "error": str(e)})
    return results

def read_recipients_list(path: str) -> pd.DataFrame:
    """
    Oczekuje pliku XLSX/CSV z kolumnami:
      - NIP (wymagane)
      - email (wymagane)
      - link (opcjonalne; gdy chcesz podać własny link do faktury / pliku)
      - Kontrahent (opcjonalnie – dla logów)

    Zwraca DF: [nip(str), email(str), link(str|''), kontrahent(str|'')]
    """
    if not path:
        raise ValueError("Ścieżka do pliku z listą kontrahentów jest pusta.")
    ext = os.path.splitext(path)[1].lower()
    if ext in (".xlsx", ".xls"):
        df = pd.read_excel(path)
    else:
        df = pd.read_csv(path, sep=None, engine="python")  # autodetect sep

    required = {"NIP", "email"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Brak kolumn w pliku odbiorców: {', '.join(sorted(missing))}")

    out = pd.DataFrame({
        "nip": df["NIP"].astype(str).str.replace(r"\D", "", regex=True).str.strip(),
        "email": df["email"].astype(str).str.strip(),
        "link": df.get("link", pd.Series([""]*len(df))).astype(str).str.strip(),
        "kontrahent": df.get("Kontrahent", pd.Series([""]*len(df))).astype(str).str.strip(),
    })

    # prosta walidacja
    out = out[out["nip"].str.len() == 10]
    out = out[out["email"].str.contains(r"@")]
    out = out.drop_duplicates(subset=["nip"], keep="first")

    # print("NIPY")
    # print(df)
    return out

# sanityzacja ciągów znaków
# używana do generowania bezpiecznych nazw dla pdf-ów
def _safe_name(name: str) -> str:
    """Prosty sanitizator nazwy pliku."""
    name = name.strip() or "plik"
    # usuń znaki niedozwolone w Windows/Linux/Mac
    name = re.sub(r'[<>:"/\\|?*]+', "_", name)
    # uniknij kropek/spacji na końcu
    return name.strip(" .")[:150]

# kod do pobierania faktur w formie pdf z fakturowni
# do wysyłania jako załącznik w emailach
def get_faktur():
    url = "https://shumee.fakturownia.pl/invoices.json"
    link_pdf = "https://shumee.fakturownia.pl/invoices/{invoice_id}.pdf"

    all_invoices = []
    page = 1
    while True:
        params = {
            "api_token": os.getenv("API_KEY"),
            "page": page,
            "per_page": 100,
            "period": "this_month"   # tylko bieżący miesiąc
        }
        r = requests.get(url, params=params, timeout=30)
        if r.status_code != 200:
            logging.error("[PDF] Błąd pobierania faktur (kod %s): %s", r.status_code, r.text[:200])
            break
        data = r.json()
        if not data:
            break
        all_invoices.extend(data)
        logging.info("[PDF] pobrano stronę %s → %s faktur", page, len(data))
        page += 1

    # filtrowanie po numerach kończących się na sm3/gs3/es3
    suffixes = ("SM", "GS", "EX")
    filtered = [inv for inv in all_invoices if str(inv.get("number", "")).endswith(suffixes)]
    logging.info("[PDF] znaleziono %s faktur z sufiksami %s", len(filtered), suffixes)

    out_dir = os.path.join("faktury", date.today().isoformat())
    os.makedirs(out_dir, exist_ok=True)

    pobrane_faktury = []
    for inv in filtered:
        inv_id = inv.get("id")
        number = inv.get("number") or f"id_{inv_id}"
        kontrahent = inv.get("buyer_name")
        nip = inv.get("buyer_tax_no")

        filename = _safe_name(f"{kontrahent}_{nip}_{number}") + ".pdf"
        out_path = os.path.join(out_dir, filename)
        pdf_url = link_pdf.format(invoice_id=inv_id)
        params = {"api_token": os.getenv("API_KEY")}

        try:
            with requests.get(pdf_url, params=params, stream=True, timeout=60) as r:
                r.raise_for_status()
                with open(out_path, "wb") as f:
                    for chunk in r.iter_content(8192):
                        if chunk:
                            f.write(chunk)
            logging.info("[PDF] OK: %s → %s", number, out_path)
            pobrane_faktury.append({"id": inv_id, "path": out_path, "ok": True, "buyer_tax_no": nip})
        except Exception as e:
            logging.error("[PDF] BŁĄD pobierania %s: %s", number, e)
            pobrane_faktury.append({"id": inv_id, "path": None, "ok": False, "buyer_tax_no": nip, "error": str(e)})

    return all_invoices, pobrane_faktury

def get_spolka_config(spolka: str) -> dict:
    klucz = spolka.strip().lower()
    try:
        return COMPANIES[klucz]
    except KeyError:
        raise ValueError(
            f"Nieznana firma: {klucz}. Dozwolone: {', '.join(COMPANIES)}"
        )

def combine_attachments(csv_map: dict[str, str], pdf_map: dict[str, list[str]]) -> dict[str, list[str]]:
    all_nips = set(csv_map) | set(pdf_map)
    out = {}
    for nip in all_nips:
        files = []
        if csv_map.get(nip):
            files.append(csv_map[nip])
        files.extend(pdf_map.get(nip, []))
        out[nip] = files
    return out

def build_pdf_map(pobrane_faktury: list[dict]) -> dict[str, list[str]]:
    pdf_map: dict[str, list[str]] = {}
    for r in pobrane_faktury:
        if not r.get("ok"):
            continue
        nip = _only_digits(r.get("buyer_tax_no"))
        path = r.get("path")
        if not nip or not path:
            continue
        pdf_map.setdefault(nip, []).append(path)
    return pdf_map

def _only_digits(s: str) -> str:
    return re.sub(r"\D", "", str(s or "")).strip()

def export_duplicates_report(df: pd.DataFrame, out_path: str):
    _, full_dups = find_duplicates(df)
    if full_dups.empty:
        print("[DUP] Brak duplikatów – raport nie został utworzony.")
        return
    cols = ["Numer dokumentu", "Netto", "VAT", "Brutto"]
    full_dups[cols].to_csv(out_path, index=False, encoding="utf-8")
    print(f"[DUP] Raport duplikatów zapisany: {out_path}")

# =========================
# Główna logika
# =========================

#Dodać wysyłanie samych faktur jako załącznik DONE
# dodać samo wysyłanie bez generowania faktur DONE
# dodać opcję sprawdzania białej listy podatników DONE po stronie bazy danych
# dodać wczytywanie i sprawdzanie listy jako drugiego pliku z listą kontrahentów DONE
def czytaj_plik(
    file: str,
    *,
    spolka: str,
    key: str,
    output_file: Optional[str] = None,
    send_only: bool = False,
    invoices_only: bool = False,
    recipients_file: Optional[str] = None,
    dry_run: bool = False,
    sell_date: Optional[str] = None,   # 🔹 nowy parametr
) -> Optional[pd.DataFrame]:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 1) Wczytaj główny plik (źródłowe faktury)
    df = pd.read_excel(file)
    if df is None or df.empty:
        raise ValueError("Pusty DataFrame – sprawdź plik wejściowy.")
    df["NIP"] = df["NIP"].astype(str).map(_only_digits)

    # 2) Odbiorcy (jeśli jest plik recipients)
    rec_list = None
    allowed_nips = None
    if recipients_file:
        rec_list = read_recipients_list(recipients_file)
        if not rec_list.empty:
            rec_list["nip_clean"] = rec_list["nip"].astype(str).map(_only_digits)
            allowed_nips = set(rec_list["nip_clean"])
            df["nip_clean"] = df["NIP"].astype(str).map(_only_digits)
            df = df[df["nip_clean"].isin(allowed_nips)].copy()
            logging.info("[INFO] Po filtrze recipients zostało %d wierszy.", len(df))

    # 3) TRYB SEND-ONLY (bez faktur, tylko maile)
    if send_only:
        if rec_list is None or rec_list.empty:
            raise ValueError("--send-only wymaga pliku --recipients z adresami email.")
        recipients_df = build_recipients_send_only(df, rec_list, {})
        logging.info("[SEND-ONLY] Odbiorców: %d", len(recipients_df))
        mail_results = send_Email(spolka, recipients_df, subject=None, dry_run=dry_run)
        logging.info("[MAIL] OK: %d, BŁĘDY: %d",
                     sum(1 for r in mail_results if r.get("ok")),
                     sum(1 for r in mail_results if not r.get("ok")))
        return df

    # 4) Budowanie faktur
    rows = build_invoice_rows(df, rec_list)
    logging.info("[INFO] Do wystawienia faktur: %d rekordów.", len(rows))
    wyniki = dodaj_faktury(spolka, rows, sell_date=sell_date)
    ok_cnt = sum(1 for w in wyniki if w["ok"])
    bad_cnt = len(wyniki) - ok_cnt
    logging.info("[FAKTURY] OK: %d, BŁĘDY: %d", ok_cnt, bad_cnt)
    for w in wyniki:
        if not w["ok"]:
            logging.error("   NIP=%s → %s", w["nip"], w.get("error"))

    # 5) TRYB INVOICES-ONLY (tylko faktury, bez maili)
    if invoices_only:
        os.environ["INVOICES_ONLY"] = "1"

        if not rows:
            logging.warning("[INVOICES-ONLY] Brak kontrahentów do wystawienia faktur (po filtrze).")
            return df

        logging.info("[INVOICES-ONLY] Wystawiono faktury – pobieram PDF-y z Fakturowni...")

        # pobiera wystawione faktury z fakturowni
        all_invoices, pobrane_faktury = get_faktur()
        pdf_map = build_pdf_map(pobrane_faktury)

        logging.info("[INVOICES-ONLY] Pobrano %d faktur PDF, zapisano w folderze 'faktury/'.", len(pdf_map))

        # budowanie raportu per kontrahent
        raport_dir = "raporty_xlsx"
        xlsx_map = export_grouped_excels(df, out_dir=raport_dir)
        logging.info("[EXPORT] Zapisano raporty kontrahentów do folderu: %s", raport_dir)

        all_attach_map = combine_attachments(xlsx_map, pdf_map)

        # zapis raportu zbiorczego
        if output_file:
            base, ext = os.path.splitext(output_file)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            out_path = f"{base}_{timestamp}{ext or '.xlsx'}"
            pd.DataFrame(wyniki).to_excel(out_path, index=False)
            logging.info("[SAVE] Raport faktur zapisany: %s", out_path)

        logging.info("[TRYB] Zakończono po wystawieniu i pobraniu faktur (bez wysyłki maili).")
        return df

    # 6) TRYB STANDARDOWY (faktury + wysyłka maili)
    filtered, pobrane_faktury = get_faktur()
    xlsx_map = export_grouped_excels(df, out_dir="raporty_xlsx")
    pdf_map = build_pdf_map(pobrane_faktury)
    all_attach_map = combine_attachments(xlsx_map, pdf_map)

    recipients_df = prepare_recipients(rows, wyniki, {})
    recipients_df["attachment_paths"] = recipients_df["nip"].map(all_attach_map)
    mail_results = send_Email(spolka, recipients_df, subject=None, dry_run=dry_run)

    logging.info("[MAIL] OK: %d, BŁĘDY: %d",
                 sum(1 for r in mail_results if r.get("ok")),
                 sum(1 for r in mail_results if not r.get("ok")))

    if output_file:
        base, ext = os.path.splitext(output_file)
        pd.DataFrame(wyniki).to_csv(output_file, index=False, encoding=OUTPUT_ENCODING)
        logging.info("[SAVE] Raport faktur i maili: %s", output_file)

    return df

# =========================
# CLI
# =========================
if __name__ == "__main__":
    if not API_KEY:
        raise RuntimeError("Brak API_KEY w .env lub zmiennych środowiskowych.")

    parser = ArgumentParser(description="Generowanie i/lub wysyłka faktur 3%")
    parser.add_argument("input", help="Ścieżka do pliku XLSX z danymi (źródłowe faktury)")
    parser.add_argument("-c", "--company", required=True, choices=sorted(COMPANIES.keys()),
                        help=f"Firma (nadawca): {', '.join(sorted(COMPANIES.keys()))}")
    parser.add_argument("-o", "--output", help="Ścieżka do raportu wynikowego CSV", default=None)
    parser.add_argument("--send-only", action="store_true",
                        help="Wyślij maile bez generowania faktur (link może być z pliku odbiorców lub pusty).")
    parser.add_argument("--recipients", help="Plik XLSX/CSV z listą kontrahentów (NIP,email[,link][,Kontrahent]).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Nie wysyłaj przez SMTP – zapisz wiadomości jako .eml w OUTPUT_DIR/eml_debug")
    parser.add_argument("--sell-date", help="Data sprzedaży (YYYY-MM-DD) przekazywana do API Fakturownia", default=None)
    parser.add_argument("--invoices-only", action="store_true",
                        help="Wystawia faktury, ale nie wysyła maili.")
    args = parser.parse_args()

    czytaj_plik(
        file=args.input,
        spolka=args.company,
        key=args.company,
        output_file=args.output,
        send_only=args.send_only,
        invoices_only=args.invoices_only,
        recipients_file=args.recipients,
        dry_run=args.dry_run,
        sell_date=args.sell_date
    )

# python main.py dane.xlsx -c shumee --invoices-only
# --sell-date 2025-09-30
