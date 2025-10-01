import os
import re
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import BigInteger, String
from dotenv import load_dotenv

# --- proste normalizatory ---
def norm_nip(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    digits = "".join(ch for ch in str(x) if ch.isdigit())
    return int(digits) if digits else None

def norm_email(x, maxlen=250):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = str(x).strip().lower()
    if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", s):
        return None
    return s[:maxlen]

def pick(df, *names):
    """Zwróć pierwszą istniejącą kolumnę z podanych nazw"""
    for n in names:
        if n in df.columns:
            return df[n]
    return pd.Series([None] * len(df), index=df.index)

def update_emails_from_excel(excel_path: str, db_url: str, update_mode: str = "fill_missing"):
    """
    update_mode:
      - "fill_missing": ustaw email tylko gdy m.mail IS NULL
      - "overwrite":    ustaw email gdy inny niż obecny (IS DISTINCT FROM)
    """
    assert update_mode in {"fill_missing", "overwrite"}

    engine = create_engine(db_url, future=True)

    # 1) wczytaj Excela
    df = pd.read_excel(excel_path)

    # 2) wyciągnij NIP + email
    out = pd.DataFrame()
    out["nip"] = pick(df, "NIP", "nip").map(norm_nip)
    out["merchant_mail"] = pick(df, "📧 Merchant mail", "merchant_mail", "Email", "email").map(norm_email)

    # 3) zostaw tylko poprawne
    out = out.dropna(subset=["nip", "merchant_mail"])
    out = out.drop_duplicates(subset=["nip"], keep="last").reset_index(drop=True)

    if out.empty:
        print("Brak poprawnych par (nip, email) do aktualizacji.")
        return

    # 4) staging
    with engine.begin() as con:
        con.execute(text("DROP TABLE IF EXISTS email_upd_stage;"))

    out.to_sql(
        "email_upd_stage",
        engine,
        if_exists="replace",
        index=False,
        dtype={"nip": BigInteger(), "merchant_mail": String(250)},
        method="multi",
        chunksize=2000,
    )

    TARGET_COL = "email" 

    # 5) UPDATE ... FROM
    if update_mode == "fill_missing":
        cond = f"m.{TARGET_COL} IS NULL"
    else:  # overwrite
        cond = f"m.{TARGET_COL} IS DISTINCT FROM s.merchant_mail"

    sql = text(f"""
        UPDATE merchanci AS m
        SET {TARGET_COL} = s.merchant_mail
        FROM email_upd_stage AS s
        WHERE m.nip = s.nip
        AND s.merchant_mail IS NOT NULL
        AND {cond};
    """)


    with engine.begin() as con:
        res = con.execute(sql)
        try:
            print(f"Zaktualizowano wierszy: {res.rowcount}")
        except Exception:
            pass
        con.execute(text("DROP TABLE IF EXISTS email_upd_stage;"))

# --- uruchomienie bezpośrednie ---
if __name__ == "__main__":
    load_dotenv()  # czyta DB_URL z .env
    DB_URL = os.getenv("DB_URL")
    EXCEL_PATH = os.getenv("EXCEL_PATH", "clickup_tasks_clean.xlsx")

    update_emails_from_excel(EXCEL_PATH, DB_URL, update_mode="fill_missing")
