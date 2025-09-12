import logging
import pandas as pd
from argparse import ArgumentParser, BooleanOptionalAction
from typing import Optional
from dotenv import load_dotenv
import smtplib,ssl
import os
import re
from datetime import datetime,timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import psycopg2
from pandas.core.interchange.dataframe_protocol import DataFrame
from psycopg2.extras import RealDictCursor,execute_values


# Program odczytuje dane z pliku xlsx i wysyłą dane do fakturowni
# potem pobiera dane z fakturowni (może)
# wysyła emaile z fakturami do listy kontrahentów z plików

#TODO
# 1. ODczytywać, serializować i oczysczać dane z pliku xlsx DONE
# 2. Generować plik z raportami dla każdego kontrahenta
# 3. Generować fakturę na fakturowni (pobierać z api linki dla każdego z kontrahentów)
# 3. Wysyłać email z fakturą i raportem do klienta


####
# Konfiguracja i pomniejsze narzędzia
####

API_KEY = os.getenv('API_KEY')
OUTPUT_ENCODING = os.getenv("OUTPUT_ENCODING")

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

OUTPUT_DIR = os.getenv("OUTPUT_DIR",".")
os.makedirs(OUTPUT_DIR,exist_ok=True)

COMPANIES = {
    "shumee": {
        "name_addr": os.getenv("SHUMEE_NAME_ADDR", 'Shumee Sp. z.o.o. aleja 1 Maja 31/33 lok. 6 90-739 Łódź Nr konta: 07 1140 1108 0000 3147 1800 1007 NIP:7252140827'),
        "nrb":       os.getenv("SHUMEE_NRB",       "07114011080000314718001007"),
        "bank_code": os.getenv("SHUMEE_BANK_CODE", "11401108"),
    },
    "greatstore": {
        "name_addr": os.getenv("GREATSTORE_NAME_ADDR", 'Greatstore Sp. z.o.o. aleja 1 Maja 31/33 lok. 6 90-739 NR konta: 35 1140 1108 0000 3639 6100 1006 Łódź NIP:7252291331 '),
        "nrb":       os.getenv("GREATSTORE_NRB",       "18102055610000310200035501"),
        "bank_code": os.getenv("GREATSTORE_BANK_CODE", "10205561"),
    },
    "extrastore": {
        "name_addr": os.getenv("EXTRASTORE_NAME_ADDR", 'Extrastore Sp. z.o.o. aleja 1 Maja 31/33 lok. 6 90-739 Łódź NIP 7252302342 Nr konta: 05 1140 2004 0000 3302 8042 9939'),
        "nrb":       os.getenv("EXTRASTORE_NRB",       "05114020040000330280429939"),
        "bank_code": os.getenv("EXTRASTORE_BANK_CODE", "11402004"),  # 8 cyfr
    },
}
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def _norm_doc_no(x: str) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip()
    s = re.sub(r"\s+", " ", s)
    s = s.upper()
    return s

def serializacja_dat(x) -> str:
    """YYYYMMDD; obsługuje datetime/Timestamp, serial Excela oraz popularne stringi."""
    if isinstance(x, (datetime, pd.Timestamp)):
        return pd.to_datetime(x).strftime("%Y%m%d")

    if isinstance(x, (int, float)) and not pd.isna(x):
        # Excel 1900-date system (z "leap bug") → origin=1899-12-30
        try:
            return pd.to_datetime(x, unit="D", origin="1899-12-30").strftime("%Y%m%d")
        except Exception:
            pass

    if isinstance(x, str):
        x = x.strip()
        for fmt in ("%d.%m.%Y", "%d/%m/%Y", "%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y"):
            try:
                return datetime.strptime(x, fmt).strftime("%Y%m%d")
            except ValueError:
                continue

    raise ValueError(f"Nieobsługiwany format daty: {x!r}")

def db_conn():
    return psycopg2.connect(**DB_CONFIG)

def validate_df(
    df: pd.DataFrame,
    *,
    date_col: str = "Data wpływu",
    netto_col: str = "Netto",
    vat_col: str = "VAT",
    brutto_col: str = "Brutto",
    tol: float = 0.01,
    on_error: str = "skip",   # 'skip' | 'keep' | 'raise'
) -> tuple[pd.DataFrame, list[dict]]:
    required = {date_col, netto_col, vat_col, brutto_col}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Brak kolumn: {', '.join(sorted(missing))}")
    d = df.copy()
    error_log: list[dict] = []

    def _to_num_cell(x):
        if pd.isna(x):
            return pd.NA
        s = str(x).strip()
        s = (s.replace("\u00A0", "")
               .replace("\u202F", "")
               .replace(" ", "")
               .replace("−", "-")
               .replace("–", "-")
               .replace("—", "-"))
        if s.startswith("(") and s.endswith(")"):
            s = "-" + s[1:-1]
        if s.endswith("-") and s.count("-") == 1:
            s = "-" + s[:-1]
        if "," in s and "." in s:
            if s.rfind(",") > s.rfind("."):
                s = s.replace(".", "").replace(",", ".")
            else:
                s = s.replace(",", "")
        elif "," in s:
            s = s.replace(",", ".")
        try:
            return float(s)
        except Exception:
            return pd.NA

    d["_netto_num"]  = d[netto_col].apply(_to_num_cell)
    d["_vat_num"]    = d[vat_col].apply(_to_num_cell)
    d["_brutto_num"] = d[brutto_col].apply(_to_num_cell)

    # --- log nie-liczb ---
    for col_name, num_col, tag in [
        (netto_col,  "_netto_num",  "bad_number_netto"),
        (vat_col,    "_vat_num",    "bad_number_vat"),
        (brutto_col, "_brutto_num", "bad_number_brutto"),
    ]:
        mask = d[num_col].isna()
        for idx in d.index[mask]:
            error_log.append({
                "type": tag,
                "row": int(idx),
                "doc": str(d.loc[idx].get("Numer dokumentu", "")),
                "value": d.loc[idx, col_name],
                "msg": f"{col_name}: nie-liczbowe/NaN"
            })

    # --- ujemne wartości -> tylko LOG (nie błąd blokujący) ---
    for num_col, orig_col, tag in [
        ("_netto_num",  netto_col,  "negative_netto"),
        ("_vat_num",    vat_col,    "negative_vat"),
        ("_brutto_num", brutto_col, "negative_brutto"),
    ]:
        mask = (d[num_col] < 0).fillna(False)
        for idx in d.index[mask]:
            error_log.append({
                "type": tag,
                "row": int(idx),
                "doc": str(d.loc[idx].get("Numer dokumentu", "")),
                "value": d.loc[idx, orig_col],
                "msg": f"{orig_col}: wartość ujemna (korekta)"
            })

    # --- spójność kwot ---
    diff = (d["_brutto_num"] - (d["_netto_num"] + d["_vat_num"])).abs()
    mask_sum_mismatch = (diff > tol).fillna(False)
    for idx in d.index[mask_sum_mismatch]:
        error_log.append({
            "type": "sum_mismatch",
            "row": int(idx),
            "doc": str(d.loc[idx].get("Numer dokumentu", "")),
            "netto": d.loc[idx, netto_col],
            "vat": d.loc[idx, vat_col],
            "brutto": d.loc[idx, brutto_col],
            "diff": float(diff.loc[idx]),
            "msg": f"Niespójność sumy > {tol}"
        })

    # --- data -> __data_str ---
    def _safe_date(val):
        try:
            return serializacja_dat(val)
        except Exception:
            return None

    d["__data_str"] = d[date_col].map(_safe_date)
    mask_bad_date = d["__data_str"].isna()
    for idx in d.index[mask_bad_date]:
        error_log.append({
            "type": "bad_date",
            "row": int(idx),
            "doc": str(d.loc[idx].get("Numer dokumentu", "")),
            "value": d.loc[idx, date_col],
            "msg": "Błąd serializacji daty"
        })


    any_error = (
        d["_netto_num"].isna()  |
        d["_vat_num"].isna()    |
        d["_brutto_num"].isna() |
        mask_sum_mismatch       |
        mask_bad_date
    )

    if on_error == "skip":
        d = d.loc[~any_error].copy()
    elif on_error == "raise":
        if any_error.any():
            raise ValueError(f"Wykryto błędy walidacji w {int(any_error.sum())} wierszach.")
    elif on_error != "keep":
        raise ValueError("validate_df.on_error ∈ {'skip','keep','raise'}")

    # nadpisz kolumny na floaty
    d[netto_col]  = d["_netto_num"].astype(float)
    d[vat_col]    = d["_vat_num"].astype(float)
    d[brutto_col] = d["_brutto_num"].astype(float)

    d.drop(columns=[c for c in d.columns if c.startswith("_") and c != "__data_str"],
           inplace=True, errors="ignore")
    return d, error_log

def export_error_log(error_log: list[dict], out_csv_path: str):
    """Pełny log do jednego CSV + osobne pliki per-typ."""
    if not error_log:
        print("[VALID] Brak błędów – nic nie eksportuję.")
        # zamiast return -> pozwól funkcji się zakończyć
        return None

    df_all = pd.DataFrame(error_log)
    os.makedirs(os.path.dirname(out_csv_path) or ".", exist_ok=True)
    df_all.to_csv(out_csv_path, index=False, encoding="utf-8-sig")
    print(f"[VALID] Pełny log błędów zapisany: {out_csv_path}")

def find_duplicates(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    required = {"Numer dokumentu", "Netto", "VAT", "Brutto"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Brak kolumn: {', '.join(sorted(missing))}")

    d = df.copy()
    d["__doc_no_norm"] = df["Numer dokumentu"].map(_norm_doc_no)
    d["__netto_gr"] = (df["Netto"])
    d["__vat_gr"]   = (df["VAT"])
    d["__brut_gr"]  = (df["Brutto"])

    mdup = d.duplicated(subset=["__doc_no_norm", "__netto_gr", "__vat_gr", "__brut_gr"], keep="first")
    group_sizes = d.groupby(["__doc_no_norm", "__netto_gr", "__vat_gr", "__brut_gr"])["Numer dokumentu"].transform("size")
    d["__is_dup_group"] = group_sizes > 1
    full_dup_groups = d.loc[d["__is_dup_group"]].copy()

    return d, full_dup_groups.sort_values(["__doc_no_norm", "__netto_gr", "__vat_gr", "__brut_gr"])

def nip_digits(nip: str) -> str:
    cleaned = re.sub(r"\D", "", str(nip or ""))
    if len(cleaned) != 10:
        print(f"[WARN] NIP ma nieprawidłową długość: {nip} → {cleaned}")
    return cleaned

def fetch_statusy_kontrahentow(nipy: list[str]) -> dict[str, str]:
    """Pobiera statusy dla wielu NIP-ów w jednym zapytaniu."""
    nip_nums = [int(nip_digits(n)) for n in nipy if nip_digits(n)]
    if not nip_nums:
        return {}
    placeholders = ",".join(["%s"] * len(nip_nums))
    query = f"SELECT nip, status FROM Merchanci WHERE nip IN ({placeholders})"
    result = {}
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, tuple(nip_nums))
            for row in cur.fetchall():
                result[str(row["nip"])] = row["status"]
    return result

def fetch_emails(df :pd.DataFrame) -> pd.DataFrame:
    nipy = df["nip"].unique().toList
    if not nipy:
        return pd.DataFrame(columns=["nip", "email"])

    query = """
           SELECT nip, email
           FROM merchanci
           WHERE nip = ANY(%s)
       """

    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (nipy,))
            rows = cur.fetchall()

    return pd.DataFrame(rows)

# Metoda usuwania duplikatów
def handle_duplicates(df: pd.DataFrame, action: str = "warn") -> pd.DataFrame:
    before = len(df)
    df = df.drop_duplicates(subset=["NIP", "Numer dokumentu"], keep="first")
    after = len(df)

    if action == "warn" and before != after:
        print(f"[INFO] Usunięto {before - after} duplikatów. Zostało {after} rekordów.")

    return df


def export_duplicates_report(df: pd.DataFrame, out_path: str):
    _, full_dups = find_duplicates(df)
    if full_dups.empty:
        print("[DUP] Brak duplikatów – raport nie został utworzony.")
        return
    cols = ["Numer dokumentu", "Netto", "VAT", "Brutto"]
    full_dups[cols].to_csv(out_path, index=False, encoding="utf-8")
    print(f"[DUP] Raport duplikatów zapisany: {out_path}")

# Główna część logiki

# Wysyłanie emaili
def send_email(df: pd.DataFrame):
    emails_df = fetch_emails(df)

    for _, row in emails_df.iterrows():
        nip = row["nip"]
        email = row["email"]
        if email:
            print(f"Wysyłam mail do NIP={nip}, email={email}")
            sender_email = os.getenv("SENDER_EMAIL")

            message = MIMEMultipart("alternative")
            message["Subject"] = "multipart test"
            message["From"] = email
            message["To"] = "testowy_test@test.cvl"

            text = """\
                Treść testowego emaila
            """

            html = """\
            <html> 
                <body> 
                    <p>
                        Testowa treść pliku
                    </p>
                    <h1> Twój email</h1>
                </body>
            </html>
            """

            part1 =MIMEText(text, "plain")
            part2 = MIMEText(html,"html")

            message.attach(part1)
            message.attach(part2)

            with smtplib.SMTP("localhost", 1025) as server:
                server.sendmail("testowy_test@test.cvl", email, message.as_string())

def czytaj_plik(
        file:str,
        *,
        spolka: str,
        key: str,
        output_file: str | None = None,
) -> pd.DataFrame:


    conf= COMPANIES[key]
    nazwa_i_adres_zleceniodawcy = conf["name_addr"]
    nr_rozliczeniowy_zleceniodawcy = conf["bank_code"]
    company_rachyunek = conf["nrb"]

    #wybór spółki na którą faktura ma być wystawiona
    klucz = spolka.lower()
    if klucz not in COMPANIES:
        raise ValueError(f"Nieznana firma {spolka} popraw to")

    # wczytywanie pliku + ususwanie duplikatów + usuwanie pustych rzędów
    df = pd.read_excel(file)

    if df is None:
        raise ValueError("DataFrame jest None – sprawdź czy poprawnie wczytałeś dane!")
    df = df.replace("", pd.NA)
    df = handle_duplicates(df,action="warn")

    # serializacja nipu
    df["NIP"] = df["NIP"].apply(nip_digits)
    suma_stawki = df.groupby("NIP")[["Netto","VAT","Brutto"]].sum().reset_index()
    if suma_stawki.empty:
        print("dataframe jest pusty")
    print("stawki zsumowane")
    print(suma_stawki)

    # Timestamp
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    #  walidacja ujemnych kwot na fakturach:
    mask_negative = (df["Netto"] < 0) | (df["VAT"] < 0) | (df["Brutto"] < 0)
    if mask_negative.any():
        print(f"[WARN] Pomijam {int(mask_negative.sum())} wierszy z ujemnymi kwotami (zapisano raport).")
        df.loc[mask_negative].to_csv(os.path.join(OUTPUT_DIR, f"ujemne_{ts}.csv"), index=False, encoding="utf-8-sig")
    df = df.loc[~mask_negative].copy()

    status_map = fetch_statusy_kontrahentow(df["NIP"].unique())
    mask_prem = df["NIP"].astype(str).apply(
        lambda nip: (status_map.get(re.sub(r"\D", "", str(nip)), "") or "").lower() == "premerchant"
    )
    if mask_prem.any():
        print(f"[WARN] Pomijam {int(mask_prem.sum())} wierszy od kontrahentów PREMERCHANT (zapisano raport).")
        df.loc[mask_prem].to_csv(os.path.join(OUTPUT_DIR, f"premerchant_{ts}.csv"), index=False, encoding="utf-8-sig")
    df = df.loc[~mask_prem].copy()

    send_email(df["NIP"])

    if df.empty:
        print("[INFO] Po filtracji brak poprawnych wierszy (po PREMERCHANT).")
        return df

if __name__ == "__main__":
    parser = ArgumentParser(description="Automatyczne generowanie faktur do kontrahentów 3% za poprzedni miesiąc")
    parser.add_argument("input", help="Ścieżka do xlsx z danymi do faktur")
    parser.add_argument("-c", "--company", required=True, choices=sorted(COMPANIES.keys()),
                        help=f"Firma (nadawca): {', '.join(sorted(COMPANIES.keys()))}")
    parser.add_argument("-o","--output",help="nazwa pliku oo którego ma zostać zapisany ...")

    args = parser.parse_args()

    czytaj_plik(
        file=args.input,
        spolka=args.company,
        key=args.company,
        output_file = args.output

    )

