import logging
import os
import re
import smtplib
import ssl
from email.mime.application import MIMEApplication
from pathlib import Path

import unicodedata
from argparse import ArgumentParser
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from decimal import Decimal, ROUND_HALF_UP
from typing import List, Dict, Tuple, Optional

import pandas as pd
import psycopg2
import requests
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

# =========================
# Konfiguracja / stałe
# =========================
load_dotenv()

API_KEY = os.getenv("API_KEY")
OUTPUT_ENCODING = os.getenv("OUTPUT_ENCODING", "utf-8-sig")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", ".")

EMAIL_HTML_TEMPLATE ="""
       <!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>Faktura 3%</title>
</head>
<body style="margin:0; padding:0; background-color:#ffffff; font-family: Arial, sans-serif;">

  <!-- LOGO -->
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" align="center" style="max-width:600px; margin:auto;">
    <tr>
      <td align="center" style="padding:20px;">
        <img src="https://www.dropbox.com/scl/fi/f93ozwqr05vvjh9eul8mb/logo.png?rlkey=fh25yqh7w4t1rp5kawx78va3k&st=dp1boi0r&raw=1" alt="Super Merchant" width="120" border="0" style="display:block;">
      </td>
    </tr>
  </table>

  <!-- NAGŁÓWEK -->
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" align="center" style="max-width:600px; margin:auto; background:#f7fbfc;">
    <tr>
      <td align="center" style="padding:20px; font-size:20px; font-weight:bold; color:#000000;">
        Miesiąc dobiegł końca!
      </td>
    </tr>
    <tr>
      <td align="center" style="padding:0 20px 20px; font-size:14px; line-height:20px; color:#333333;">
        Poprzedni okres rozliczeniowy dobiegł końca,<br>
        poniżej znajdziesz link do pobrania faktury 3% za sprzedane artykuły,<br>
        a w załączniku listę faktur, na podstawie których została ona wystawiona.
      </td>
    </tr>
    <tr>
      <td align="center" style="padding:20px;">
        <a href="{INVOICE_LINK}" target="_blank" style="background-color:#0077DA; color:#ffffff; text-decoration:none; padding:12px 24px; border-radius:4px; font-size:14px; font-weight:bold; display:inline-block;">
          Pobierz fakturę
        </a>
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
    <tr>
      <td align="center" style="padding:10px;">
        <!-- Social icons -->
        <a href="https://facebook.com/TwojaStrona" target="_blank" style="margin:0 5px;">
          <img src="https://raw.githubusercontent.com/Celiek/Skrypty-python-praca/refs/heads/main/img/image-4.png" alt="Facebook" width="32" border="0" style="display:inline-block;">
        </a>
        <a href="https://twitter.com/" target="_blank" style="margin:0 5px;">
          <img src="https://raw.githubusercontent.com/Celiek/Skrypty-python-praca/refs/heads/main/img/image-5.png" alt="Twitter" width="32" border="0" style="display:inline-block;">
        </a>
        <a href="https://linkedin.com/" target="_blank" style="margin:0 5px;">
          <img src="https://raw.githubusercontent.com/Celiek/Skrypty-python-praca/refs/heads/main/img/image-6.png" alt="LinkedIn" width="32" border="0" style="display:inline-block;">
        </a>
        <a href="https://instagram.com/" target="_blank" style="margin:0 5px;">
          <img src="https://raw.githubusercontent.com/Celiek/Skrypty-python-praca/refs/heads/main/img/image-7.png" alt="Instagram" width="32" border="0" style="display:inline-block;">
        </a>
      </td>
    </tr>
  </table>

</body>
</html>

    """

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

COMPANIES = {
    "shumee": {
        "name_addr": os.getenv("SHUMEE_NAME_ADDR", "Shumee Sp. z.o.o. ..."),
        "name" : os.getenv("NAZWA","SHUMEE"),
        "nrb": os.getenv("SHUMEE_NRB", "07114011080000314718001007"),
        "bank_code": os.getenv("SHUMEE_BANK_CODE", "11401108"),
        "email": os.getenv("SHUMEE_EMAIL", "faktury@shumee.pl"),
        "server_host":os.getenv("SHUMEE_SERVER_HOST", "localhost"),
        "kontakt":os.getenv("SHUMEE_KONTAKT", "kontakt@shumee.pl"),
        "password":os.getenv("SHUMEE_PASS"),
    },
    "greatstore": {
        "name_addr": os.getenv("GREATSTORE_NAME_ADDR", "Greatstore Sp. z.o.o. ..."),
        "name" : os.getenv("NAZWA","GREATSTORE"),
        "nrb": os.getenv("GREATSTORE_NRB", "18102055610000310200035501"),
        "bank_code": os.getenv("GREATSTORE_BANK_CODE", "10205561"),
        "email": os.getenv("SHUMEE_EMAIL", "faktury@greatstore.pl"),
        "server_host":os.getenv("GREAT_SERVER_HOST", "imap.serwer1694120.home.pl"),
        "kontakt": os.getenv("SHUMEE_KONTAKT", "kontakt@greatstore.pl"),
        "password":os.getenv("GREATSTORE_PASS"),
    },
    "extrastore": {
        "name_addr": os.getenv("EXTRASTORE_NAME_ADDR", "Extrastore Sp. z.o.o. ..."),
        "name" : os.getenv("NAZWA","EXTRASTORE"),
        "nrb": os.getenv("EXTRASTORE_NRB", "05114020040000330280429939"),
        "bank_code": os.getenv("EXTRASTORE_BANK_CODE", "11402004"),
        "email": os.getenv("SHUMEE_EMAIL", "faktury_extra@shumee.pl"),
        "server_host":os.getenv("EXTRA_SERVER_HOST", "imap.serwer1694120.home.pl"),
        "kontakt": os.getenv("SHUMEE_KONTAKT", "kontakt@greatstore.pl"),
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
    s = re.sub(r"[ _]+", " ", s)      # najpierw łączymy w pojedyncze spacje
    s = s.replace(" ", "_")           # potem spacje -> podkreślniki
    s = re.sub(r"_+", "_", s)

    # usunięcie kropek/spacji/podkreślników z początku/końca
    s = s.strip(" ._")

    # pusta po czyszczeniu?
    if not s:
        s = "plik"

    # unikamy nazw zarezerwowanych (bez rozszerzenia)
    base_upper = s.upper()
    if base_upper in _WINDOWS_RESERVED:
        s = f"_{s}"

    # ograniczenie długości
    s = s[:max_len].rstrip(" ._")

    # jeszcze raz awaryjnie
    if not s:
        s = "plik"

    return s

def export_grouped_csvs(df: pd.DataFrame,out_dir: str, *, encoding: str ="utf-8-sig") -> Dict[str,str]:
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    wanted = ["Kontrahent", "NIP", "Numer dokumentu", "Data", "Netto", "VAT", "Brutto"]
    cols = [c for c in wanted if c in df.columns]
    if not cols:
        raise ValueError("Brak kolumn do eksportu CSV — sprawdź nazwy w DataFrame.")

    out_map: Dict[str, str] = {}
    g = df.groupby("NIP", dropna=False, as_index=False)
    for nip, sub in g:
        nip_str = str(nip).strip()
        kontrahent = ""
        if "Kontrahent" in sub.columns and not sub["Kontrahent"].isna().all():
            kontrahent = str(sub["Kontrahent"].iloc[0] or "")

        fname = f"{nip_str}_{_slugify_filename(kontrahent)}.csv"
        fpath = os.path.join(out_dir, fname)
        sub[cols].to_csv(fpath, index=False, encoding=encoding, sep=";")
        out_map[nip_str] = os.path.abspath(fpath)

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

def _money2(x) -> float:
    """Zaokrąglenie kwoty do 2 miejsc (HALF_UP), zwraca float do JSON."""
    return float(Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

def _norm_doc_no(x: str) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip()
    s = re.sub(r"\s+", " ", s)
    return s.upper()

def handle_duplicates(df: pd.DataFrame, action: str = "warn") -> pd.DataFrame:
    before = len(df)
    df2 = df.drop_duplicates(subset=["NIP", "Numer dokumentu"], keep="first")
    after = len(df2)
    if action == "warn" and before != after:
        logging.info("[DUP] Usunięto %d duplikatów. Zostało %d rekordów.", before - after, after)
    return df2

def fetch_statusy_kontrahentow(nipy: List[str]) -> Dict[str, str]:
    """SELECT nip, status FROM merchanci WHERE nip IN (...)"""
    nums = [re.sub(r"\D", "", str(n)) for n in nipy if n]
    nums = [n for n in nums if n]
    if not nums:
        return {}
    placeholders = ",".join(["%s"] * len(nums))
    query = f"SELECT nip, status FROM merchanci WHERE nip IN ({placeholders})"
    result = {}
    with db_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, tuple(nums))
        for row in cur.fetchall():
            result[str(row["nip"])] = row["status"]
    return result

def fetch_emails(nipy) -> pd.DataFrame:
    """Zwraca DF kolumny: nip, email (dla listy NIP-ów)."""
    if isinstance(nipy, pd.Series):
        nipy = nipy.dropna().astype(str).str.strip().unique().tolist()
    elif isinstance(nipy, pd.DataFrame):
        nipy = nipy["NIP"].dropna().astype(str).str.strip().unique().tolist()
    elif isinstance(nipy, (list, tuple)):
        nipy = [str(n).strip() for n in nipy if n]
    else:
        nipy = [str(nipy).strip()]

    if not nipy:
        print("Nie dosłałeś żadnych emaili !" + nipy)
        return pd.DataFrame(columns=["nip", "email"])

    query = "SELECT nip, email FROM merchanci WHERE nip = ANY(%s::bigint[])"
    with db_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, (nipy,))
        rows = cur.fetchall()
    print("Emaile z bazy danych:")
    print(rows)
    return pd.DataFrame(rows)

def build_recipients_report_only(df, recipients_df, mail_results, attachments_by_nip, output_file):
    # 1) Oczyść NIP wszędzie do samych cyfr
    df = df.copy()
    recipients_df = recipients_df.copy()

    df["NIP_clean"] = df["NIP"].astype(str).map(_only_digits)
    recipients_df["nip_clean"] = recipients_df["nip"].astype(str).map(_only_digits)

    # 2) Zrób unikalną listę odbiorców per NIP (jeśli było kilka maili dla jednego NIP – zostaw pierwszy)
    recipients_unique = (
        recipients_df
        .sort_values(["nip_clean", "email"])              # deterministycznie
        .drop_duplicates(subset=["nip_clean"], keep="first")
    )

    # 3) Sumy z pliku wejściowego tylko dla NIP-ów z recipients
    sums = (
        df[df["NIP_clean"].isin(recipients_unique["nip_clean"])]
        .groupby("NIP_clean", as_index=False)
        .agg(Netto=("Netto", "sum"),
             VAT=("VAT", "sum"),
             Brutto=("Brutto", "sum"))
    )

    # 4) Attachmenty – oczyść klucze w mapie i zamień na DF, żeby merge był jednoznaczny
    att_clean = { _only_digits(k): v for k, v in (attachments_by_nip or {}).items() }
    att_df = (pd.DataFrame({
                "nip_clean": list(att_clean.keys()),
                "attachment_path": list(att_clean.values())
             })
             if att_clean else pd.DataFrame(columns=["nip_clean","attachment_path"])
    )

    # 5) Wyniki wysyłki (po emailu) – też deduplikuj
    mail_df = pd.DataFrame(mail_results or [])
    if not mail_df.empty:
        mail_df.rename(columns={"email": "Email", "ok": "Wyslano_OK"}, inplace=True)
        mail_df = (mail_df
                   .sort_values(["Email"])
                   .drop_duplicates(subset=["Email"], keep="last"))  # ostatni status wygrywa
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
    return [f for f in data if str(f.get("number", "")).endswith("/sm3")]

def build_invoice_rows(df: pd.DataFrame) -> List[Dict]:
    """
    Przygotowuje rekordy do wystawienia faktur:
    - agreguje po NIP (Netto sum, Kontrahent first),
    - liczona stawka 3% netto i brutto (VAT 23%),
    - dociąga email z DB.
    Zwraca listę dict: {buyer_name, buyer_tax_no, buyer_email, amount_gross}
    """
    # jak się spartoli to wywalić to
    #df["NIP"] = _norm_doc_no(df["NIP"])

    grouped = (
        df.groupby("NIP", as_index=False)
          .agg({"Netto": "sum", "Kontrahent": "first"})
    )
    grouped["stawka_netto_3p"]  = grouped["Netto"] * 0.03
    grouped["stawka_brutto_3p"] = grouped["stawka_netto_3p"] * 1.23

    emails_df = fetch_emails(grouped["NIP"])
    emails_df["nip"] = emails_df["nip"].astype(str)
    grouped["NIP"]   = grouped["NIP"].astype(str)
    merged = grouped.merge(emails_df, left_on="NIP", right_on="nip", how="left")

    rows = []
    for _, r in merged.iterrows():
        rows.append({
            "buyer_name":   str(r["Kontrahent"]).strip(),
            "buyer_tax_no": str(r["NIP"]).strip(),
            "buyer_email":  (str(r["email"]).strip() if pd.notna(r.get("email")) else None),
            "amount_gross": _money2(r["stawka_brutto_3p"]),
        })
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


    company_name = cfg.get("name") or spolka.upper()
    if not subject:
        subject = f"{company_name} - faktura"

    results = []

    if recipents_df is None or recipents_df.empty:
        logging.info("[MAIL] Brak odbiorców do wysyłki.")
        return results

    context = ssl.create_default_context()
    server = None

    try:
        if use_ssl:
            server = smtplib.SMTP_SSL(host=host, port=port, context=context, timeout = 60)
            if os.getenv("SMTP_DEBUG", "0") == "1":
                server.set_debuglevel(1)
            server.ehlo()
        else:
            server = smtplib.SMTP(host=host, port=port, timeout=60)
            server.ehlo()
            # odkomentować po debugowaniu
            server.starttls(context=context)
            server.ehlo()

        # zakomentowane na rzecz debugowania
        if from_addr and password:
            server.login(from_addr, password)
            server.ehlo()

        for row in recipents_df.itertuples(index = False):
            email_to = (getattr(row, "email", None) or "").strip()
            invoice_link = (getattr(row, "link", None) or getattr(row, "invoice_link", None) or "").strip()
            kontrahent = (getattr(row, "kontrahent", "") or "").strip()
            attach_path = (getattr(row, "attachment_path", "") or "").strip()

            if not email_to:
                results.append({"email": None, "ok": False, "error": "Brak adresu email"})
                logging.warning("[SKIP] %s pominięty – %s", email_to or "-",
                                "brak adresu" if not email_to else "brak linku")
                continue
            if not invoice_link:
                results.append({"email": email_to, "ok": False, "error": "Brak linku do faktury"})
                continue

            html_body = render_email_html(invoice_link, company_name)
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = from_addr
            msg["To"] = email_to

            alt = MIMEMultipart("alternative")
            alt.attach(MIMEText(html_body, "html", "utf-8"))
            msg.attach(alt)

            # ZAŁĄCZNIK (jeśli jest)
            if attach_path and os.path.isfile(attach_path):
                with open(attach_path, "rb") as f:
                    part = MIMEApplication(f.read())
                # ładna nazwa pliku
                filename = os.path.basename(attach_path)
                part.add_header("Content-Disposition", "attachment", filename=filename)
                msg.attach(part)

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

    pd.DataFrame(results).to_csv("mail_debug.csv", index=False, sep=";", encoding="utf-8-sig")
    logging.info("[DEBUG] Zapisano szczegóły maili do mail_debug.csv")
    return results

def build_recipients_send_only(df: pd.DataFrame,
                               recipients_list: Optional[pd.DataFrame],
                               attachments_by_nip: Dict[str, str]) -> pd.DataFrame:
    """
    Zwraca DF z kolumnami: nip, email, kontrahent, link, attachment_path
    - email/link: z listy odbiorców jeśli podana; inaczej email z DB (fetch_emails), link pusty
    """
    # agregacja po NIP jak w build_invoice_rows – tylko dla nazwy
    base = (df.groupby("NIP", as_index=False)
              .agg({"Kontrahent": "first"}))
    base["NIP"] = base["NIP"].astype(str)

    base["NIP"] = base["NIP"].apply(nip_digits)

    # maile: albo z listy, albo z bazy
    if recipients_list is not None and not recipients_list.empty:
        rl = recipients_list.copy()
        rl["nip"] = rl["nip"].astype(str)
        merged = base.merge(rl, left_on="NIP", right_on="nip", how="left")
        # fallback – jeśli w liście brak emaila, dociągnij z DB
        need = merged["email"].isna() | (merged["email"].astype(str).str.strip() == "")
        if need.any():
            emails_db = fetch_emails(merged.loc[need, "NIP"])
            emails_db["nip"] = emails_db["nip"].astype(str)
            merged = merged.merge(emails_db, left_on="NIP", right_on="nip", how="left", suffixes=("", "_db"))
            merged["email"] = merged["email"].fillna(merged["email_db"])
        merged["link"] = merged["link"].fillna("")
        out = pd.DataFrame({
            "nip": merged["NIP"].astype(str),
            "email": merged["email"].astype(str),
            "kontrahent": merged["Kontrahent"].astype(str),
            "link": merged["link"].astype(str),
        })
    else:
        emails_db = fetch_emails(base["NIP"])
        emails_db["nip"] = emails_db["nip"].astype(str)
        merged = base.merge(emails_db, left_on="NIP", right_on="nip", how="left")
        out = pd.DataFrame({
            "nip": merged["NIP"].astype(str),
            "email": merged["email"].astype(str),
            "kontrahent": merged["Kontrahent"].astype(str),
            "link": pd.Series([""]*len(merged))
        })

    out["attachment_path"] = out["nip"].map(attachments_by_nip).fillna("")
    # filtrowanie: tylko z mailem
    out = out[out["email"].str.contains(r"@")]
    return out


def dodaj_faktury(spolka: str, items: List[Dict]) -> List[Dict]:
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
                    "sell_date":  today.strftime("%Y-%m-%d"),
                    "issue_date": today.strftime("%Y-%m-%d"),
                    "payment_to": payment_to.strftime("%Y-%m-%d"),
                    "buyer_name":   it["buyer_name"],
                    "buyer_tax_no": it["buyer_tax_no"],
                    "department_id": DEPARTMENT_ID[spolka],
                    **({"buyer_email": it["buyer_email"]} if it.get("buyer_email") else {}),
                    "positions": [{
                        "name": f"płatność za usługę za okres {miesiace[poprzedni.month]}",
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
    return out


def get_spolka_config(spolka: str) -> dict:
    klucz = spolka.strip().lower()
    try:
        return COMPANIES[klucz]
    except KeyError:
        raise ValueError(
            f"Nieznana firma: {klucz}. Dozwolone: {', '.join(COMPANIES)}"
        )

def _only_digits(s: str) -> str:
    return re.sub(r"\D", "", str(s or "")).strip()

# =========================
# Główna logika
# =========================

#Dodać wysyłanie samych faktur jako załącznik,
# dodać samo wysyłanie bez generowania faktur
# dodać opcję sprawdzania białej listy podatników
# dodać wczytywanie i sprawdzanie listy jako drugiego pliku z listą kontrahentów
def czytaj_plik(
    file: str,
    *,
    spolka: str,
    key: str,
    output_file: Optional[str] = None,
    send_only: bool = False,                 # NEW
    recipients_file: Optional[str] = None,   # NEW
    dry_run: bool = False,                   # NEW
) -> Optional[pd.DataFrame]:

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    att_dir = os.path.join(OUTPUT_DIR, f"zalaczniki_{ts}")

    # 1) wczytanie głównego pliku
    df = pd.read_excel(file)
    if df is None or df.empty:
        raise ValueError("Pusty DataFrame – sprawdź plik wejściowy.")

    df["NIP"] = df["NIP"].astype(str).map(_only_digits)

    # załączniki CSV per NIP
    attachments_by_nip = export_grouped_csvs(df, att_dir, encoding=OUTPUT_ENCODING)
    attachments_by_nip = {_only_digits(k): v for k, v in (attachments_by_nip or {}).items()}

    # 2) czyszczenia
    df = df.replace("", pd.NA)
    df = handle_duplicates(df, action="warn")

    mask_empty_nip = df["NIP"].isna() | (df["NIP"].astype(str).str.strip() == "")
    if mask_empty_nip.any():
        out = os.path.join(OUTPUT_DIR, f"brak_nipu_{ts}.csv")
        df.loc[mask_empty_nip].to_csv(out, index=False, encoding=OUTPUT_ENCODING)
        logging.warning("[WARN] Pominięto %d wierszy z pustym NIP-em → %s", int(mask_empty_nip.sum()), out)
    df = df.loc[~mask_empty_nip].copy()

    status_map = fetch_statusy_kontrahentow(df["NIP"].unique())
    mask_prem = df["NIP"].astype(str).apply(
        lambda nip: (status_map.get(re.sub(r"\D", "", str(nip)), "") or "").lower() == "premerchant"
    )
    if mask_prem.any():
        out = os.path.join(OUTPUT_DIR, f"premerchant_{ts}.csv")
        df.loc[mask_prem].to_csv(out, index=False, encoding=OUTPUT_ENCODING)
        logging.warning("[WARN] Pominięto %d wierszy PREMERCHANT → %s", int(mask_prem.sum()), out)
    df = df.loc[~mask_prem].copy()

    if df.empty:
        logging.info("[INFO] Po filtracjach brak wierszy.")
        return df

    # 3) ŚCIEŻKA A: SEND-ONLY (bez Fakturowni)
    if send_only:
        # 3.1 wczytaj listę kontrahentów (jeśli podano)
        rec_list = read_recipients_list(recipients_file) if recipients_file else None
        rec_list.columns =rec_list.columns.str.strip()
        rec_list["nip"] = rec_list["nip"].apply(nip_digits)

        # 3.2 zbuduj listę odbiorców do wysyłki (TU MUSI BYĆ SEND_ONLY)
        recipients_df = build_recipients_send_only(df, rec_list, attachments_by_nip)
        logging.info("[SEND-ONLY] Odbiorców: %d", len(recipients_df))

        # 3.3 wyślij maile
        mail_results = send_Email(spolka, recipients_df, subject=None, dry_run=dry_run)
        mail_ok = sum(1 for r in mail_results if r.get("ok"))
        mail_bad = len(mail_results) - mail_ok
        logging.info("[MAIL] OK: %d, BŁĘDY: %d", mail_ok, mail_bad)

        # 3.4 sumy z pliku źródłowego (cały plik)
        suma_netto = float(df["Netto"].sum())
        suma_vat = float(df["VAT"].sum())
        suma_brutto = float(df["Brutto"].sum())
        summary = {
            "ok": mail_ok,
            "bledy": mail_bad,
            "razem_wiadomosci": len(mail_results),
            "suma_netto": suma_netto,
            "suma_vat": suma_vat,
            "suma_brutto": suma_brutto,
            "data": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        if output_file:
            base, ext = os.path.splitext(output_file)
            if not ext:
                ext = ".csv"

            # 3.5 raport zbiorczy
            summary_path = f"{base}_raport{ext}"
            pd.DataFrame([summary]).to_csv(summary_path, index=False, encoding=OUTPUT_ENCODING)
            logging.info("[SAVE] Raport zbiorczy: %s", summary_path)

            # 3.6 normalizacja NIP (same cyfry) po obu stronach
            df["NIP"] = df["NIP"].astype(str).str.replace(r"\D", "", regex=True)
            recipients_df["nip"] = recipients_df["nip"].astype(str).str.replace(r"\D", "", regex=True)

            # 3.7 raport **tylko dla recipients** (per NIP)
            raport_recipients, _ = build_recipients_report_only(
                df=df,
                recipients_df=recipients_df,
                mail_results=mail_results,
                attachments_by_nip=attachments_by_nip,
                output_file=f"{base}_recipients{ext}"
            )

            # (opcjonalne) dopięcie statusu wysyłki po e-mailu – jeśli chcesz mieć go też w raporcie:
            mail_df = pd.DataFrame(mail_results).rename(columns={"email": "Email", "ok": "Wyslano_OK"})
            if "Email" in raport_recipients.columns:
                raport_recipients = raport_recipients.merge(
                    mail_df[["Email", "Wyslano_OK"]],
                    on="Email",
                    how="left"
                )
                raport_recipients.to_csv(f"{base}_recipients{ext}", index=False, encoding=OUTPUT_ENCODING, sep=";")

            # 3.8 zapisz surową listę odbiorców (to co realnie wysyłaliśmy)
            recipients_df.to_csv(output_file, index=False, encoding=OUTPUT_ENCODING)
            logging.info("[SAVE] Zapisano raport odbiorców (surowy): %s", output_file)

        return df

        return df

    # 4) ŚCIEŻKA B: standard – wystaw faktury i wyślij linki
    rows = build_invoice_rows(df)
    logging.info("[INFO] Do wystawienia faktur: %d rekordów.", len(rows))

    wyniki = dodaj_faktury(spolka, rows)
    ok_cnt  = sum(1 for w in wyniki if w["ok"])
    bad_cnt = len(wyniki) - ok_cnt
    logging.info("[FAKTURY] OK: %d, BŁĘDY: %d", ok_cnt, bad_cnt)
    for w in wyniki:
        if not w["ok"]:
            logging.error("   NIP=%s → %s", w["nip"], w.get("error"))

    recipients_df = prepare_recipients(rows, wyniki, attachments_by_nip)
    mail_results = send_Email(spolka, recipients_df, subject=None, dry_run=dry_run)
    mail_ok = sum(1 for r in mail_results if r.get("ok"))
    mail_bad = len(mail_results) - mail_ok
    logging.info("[MAIL] OK: %d, BŁĘDY: %d", mail_ok, mail_bad)

    suma_netto = df["Netto"].sum()
    suma_vat = df["VAT"].sum()
    suma_brutto = df["Brutto"].sum()

    summary = {
        "ok": mail_ok,
        "bledy": mail_bad,
        "razem_wiadomosci": len(mail_results),
        "suma_netto": float(suma_netto),
        "suma_vat": float(suma_vat),
        "suma_brutto": float(suma_brutto),
        "data": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    if output_file:
        base, ext = os.path.splitext(output_file)
        summary_path = f"{base}_raport{ext}"
        pd.DataFrame([summary]).to_csv(summary_path, index=False, encoding=OUTPUT_ENCODING)
        logging.info("[SAVE] Raport zbiorczy: %s", summary_path)

        pd.DataFrame(wyniki).to_csv(output_file, index=False, encoding=OUTPUT_ENCODING)
        logging.info("[SAVE] Zapisano raport: %s", output_file)

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

    # NEW:
    parser.add_argument("--send-only", action="store_true",
                        help="Wyślij maile bez generowania faktur (link może być z pliku odbiorców lub pusty).")
    parser.add_argument("--recipients", help="Plik XLSX/CSV z listą kontrahentów (NIP,email[,link][,Kontrahent]).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Nie wysyłaj przez SMTP – zapisz wiadomości jako .eml w OUTPUT_DIR/eml_debug")
    args = parser.parse_args()

    czytaj_plik(
        file=args.input,
        spolka=args.company,
        key=args.company,
        output_file=args.output,
        send_only=args.send_only,
        recipients_file=args.recipients,
        dry_run=args.dry_run
    )

