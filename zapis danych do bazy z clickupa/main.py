# Write a full corrected script to a file for the user to download
from textwrap import dedent

import os
import re
import sys
import time
import traceback
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import create_engine, text, Table, MetaData
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.types import DateTime, Integer, BigInteger, String
from dotenv import load_dotenv

#################################
# USTAWIENIA PROGRAMU / UTILITIES
#################################

load_dotenv()
DB_URL = os.getenv("DB_URL")
LOCAL_TZ = ZoneInfo("Europe/Warsaw")


# instrukcja : po pobraniu pliku xlsx z clickupa zmienić nazwę kolumny w pliku ze status(2) na 
# relation status i dopiero wtedy plik zostanie przyjęty 

def setup_logging():
    logger = logging.getLogger("loader")
    if logger.handlers:
        return logger
    logger.setLevel(logging.DEBUG)  # do pliku: DEBUG
    os.makedirs("logs", exist_ok=True)
    fh = RotatingFileHandler("logs/run.log", maxBytes=5_000_000, backupCount=5, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s:%(lineno)d - %(message)s"))
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()  # konsola
    ch.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    ch.setLevel(logging.WARNING)  # w konsoli tylko WARNING+
    logger.addHandler(fh); logger.addHandler(ch)

    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.pool").setLevel(logging.WARNING)

    return logger

logger = setup_logging()

def install_short_excepthook(max_lines: int = 40):
  
    def _hook(exc_type, exc, tb):
        logger.exception("Unhandled exception", exc_info=(exc_type, exc, tb))
        te = traceback.TracebackException(exc_type, exc, tb)
        lines = list(te.format())
        short = "".join(lines[:max_lines]) + "\\n... (skrócono; pełny log: logs/run.log)\\n"
        sys.stderr.write(short)
    sys.excepthook = _hook

install_short_excepthook()

# ustawienia pandas
pd.set_option('future.no_silent_downcasting', True)

# --- KONFIG ---
EXCEL_PATH = os.getenv("EXCEL_PATH", "clickup_tasks_clean3.xlsx")

DB_URL = os.getenv("DB_URL")
LOCAL_TZ = ZoneInfo("Europe/Warsaw")
CHUNK_SIZE = 1000  # batch UPSERT

# REGEX DO EMOJI
_EMOJI_RE = re.compile(
    "["
    "\\U0001F600-\\U0001F64F"  # emotikony
    "\\U0001F300-\\U0001F5FF"  # symbole/piktogramy
    "\\U0001F680-\\U0001F6FF"  # transport/mapy
    "\\U0001F700-\\U0001F77F"
    "\\U0001F780-\\U0001F7FF"
    "\\U0001F800-\\U0001F8FF"
    "\\U0001F900-\\U0001F9FF"
    "\\U0001FA00-\\U0001FA6F"
    "\\U0001FA70-\\U0001FAFF"
    "\\u2600-\\u26FF"          # znaki pogody/astr.
    "\\u2700-\\u27BF"          # dingbats
    "\\u2B00-\\u2BFF"
    "\\u2300-\\u23FF"
    "\\u200D"                  # zero-width joiner
    "\\uFE0E-\\uFE0F"          # selektory wariantów
    "]+"
)

def strip_emoji(s: str) -> str:
    return _EMOJI_RE.sub("", s)

def _none_if_empty(x: str | None):
    if x is None:
        return None
    s = str(x).strip()
    return None if s == "" or s.lower() in {"nan","none","null"} else s

def norm_text(x, maxlen=None):
    s = _none_if_empty(x)
    if s is None: return None
    s = strip_emoji(str(s))
    s = re.sub(r"\\s+", " ", s).strip()
    return s[:maxlen] if maxlen else s

def norm_int(x):
    if x is None or (isinstance(x,float) and pd.isna(x)): return None
    try:
        s = str(x).strip()
        if s == "": return None
        return int(float(s))
    except Exception:
        return None

def norm_email(x, maxlen=250):
    s = _none_if_empty(x)
    if s is None: return None
    s = s.strip().lower()
    if not re.match(r"^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$", s):
        return None
    return s[:maxlen]

def norm_phone(x, maxlen=250):
    s = _none_if_empty(x)
    if s is None: return None
    s = re.sub(r"[^\\d+]", "", s)
    s = re.sub(r"(?<=.)\\+", "", s)
    return s[:maxlen]

def norm_url(x, maxlen=100):
    s = _none_if_empty(x)
    if s is None: return None
    s = s.strip()
    if not re.match(r"^[a-z]+://", s, re.I):
        s = "http://" + s
    return s[:maxlen]

def norm_nip(x):
    s = _none_if_empty(x)
    if s is None:
        return None
    digits = "".join(ch for ch in str(s) if ch.isdigit())
    return int(digits) if digits else None

def pick(df, *names):
    for n in names:
        if n in df.columns:
            return df[n]
    return pd.Series([None]*len(df), index=df.index)

def norm_ts(x):
    if x is None or (isinstance(x, float) and pd.isna(x)): return None
    try:
        ts = pd.to_datetime(x, dayfirst=True, errors="coerce")
        if pd.isna(ts): return None
        if ts.tzinfo is None:
            ts = ts.tz_localize(LOCAL_TZ)
        else:
            ts = ts.tz_convert(LOCAL_TZ)
        return ts.to_pydatetime()
    except Exception:
        return None

EMOJI_RE = re.compile(
    "["
    "\\U0001F300-\\U0001FAFF"
    "\\u2600-\\u26FF\\u2700-\\u27BF\\u2B00-\\u2BFF\\u2300-\\u23FF"
    "\\u200D\\uFE0E-\\uFE0F"
    "]+"
)

def norm_bool(x):
    if x is None or (isinstance(x, float) and pd.isna(x)): return None 
    s = str(x).strip().lower() 
    true_set = {"1","t","true","y","yes","tak","x"} 
    false_set = {"0","f","false","n","no","nie"} 
    if s in true_set: return True 
    if s in false_set: return False 
    return None

def czyszczenie_z_emoji(s):
    if s is None: return None
    return EMOJI_RE.sub("", str(s))

def clean_df_emoji(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [czyszczenie_z_emoji(str(c)).strip() for c in df.columns]
    text_cols = df.select_dtypes(include=["object", "string"]).columns
    for c in text_cols:
        df[c] = df[c].map(lambda v: czyszczenie_z_emoji(v) if isinstance(v, str) else v)
    return df

DT_COLS = [
    "data_utworzenia",
    "data_aktualizacji",
    "next_meeting_date",
    "held_first_meeting",
    "regulations_email_date_sent",
    "regulations_acceptance_date",
]
INT_COLS    = ["base_account_type", "merchant_id", "merchant_bl_id", "relation_status"]
BIGINT_COLS = ["nip"]
BOOL_COLS   = ["regulations_acceptance", "mail_warunki", "warunki_akceptacja"]

def _sample(df, mask, cols, n=10):
    try:
        return df.loc[mask, cols].head(n).to_dict("records")
    except Exception:
        return []

def diagnose_df(df_in: pd.DataFrame):
    problems = {}

    # 1) DATETIME: string "NaT" lub nieparsowalne daty
    for c in (set(DT_COLS) & set(df_in.columns)):
        s = df_in[c]
        mask_nat_str   = s.astype(object).map(lambda v: isinstance(v, str) and v.strip().upper()=="NAT")
        mask_bad_parse = pd.to_datetime(s, errors="coerce").isna() & s.notna()
        bad = mask_nat_str | mask_bad_parse
        if bad.any():
            problems[c] = {
                "type": "datetime",
                "count": int(bad.sum()),
                "sample": _sample(df_in, bad, ["id", c]),
            }

    # 2) NUMERY: nie da się zrzutować na liczbę (a nie jest puste)
    for c in (set(INT_COLS + BIGINT_COLS) & set(df_in.columns)):
        s = df_in[c]
        mask_bad_num = pd.to_numeric(s, errors="coerce").isna() & s.notna()
        if mask_bad_num.any():
            problems[c] = {
                "type": "numeric",
                "count": int(mask_bad_num.sum()),
                "sample": _sample(df_in, mask_bad_num, ["id", c]),
            }

    # 3) BOOL: wartości spoza akceptowanych
    true_set  = {"1","t","true","y","yes","tak","x"}
    false_set = {"0","f","false","n","no","nie"}
    for c in (set(BOOL_COLS) & set(df_in.columns)):
        s = df_in[c].astype(object)
        def _ok(v):
            if v is None or (isinstance(v,float) and pd.isna(v)): return True
            if isinstance(v, bool): return True
            if isinstance(v, (int,)): return True
            if isinstance(v, str):
                vs = v.strip().lower()
                return vs in true_set or vs in false_set or vs==""  # pusty zostanie wyczyszczony wyżej
            return False
        mask_bad_bool = ~s.map(_ok)
        if mask_bad_bool.any():
            problems[c] = {
                "type": "bool",
                "count": int(mask_bad_bool.sum()),
                "sample": _sample(df_in, mask_bad_bool, ["id", c]),
            }

    # 4) NOT NULL dla 'status'
    if "status" in df_in.columns:
        mask_bad_status = df_in["status"].astype(object).map(lambda v: not (isinstance(v,str) and v.strip()))
        if mask_bad_status.any():
            problems["status"] = {
                "type": "not_null",
                "count": int(mask_bad_status.sum()),
                "sample": _sample(df_in, mask_bad_status, ["id", "status"]),
            }

    # LOG
    if problems:
        for col, info in problems.items():
            logger.error("DIAG: kolumna=%s, typ=%s, liczba_problemów=%s, próbka=%s",
                         col, info["type"], info["count"], info["sample"])
    else:
        logger.info("DIAG: brak oczywistych problemów typów w DF.")




def _sanitize_df_for_sql(df_sub: pd.DataFrame, dt_cols_upsert: set[str]) -> pd.DataFrame:
    df_sub = df_sub.copy()

    # 1) globalnie: "NaT"/"" -> None
    df_sub.replace({"NaT": None, "NAT": None, "nat": None, "": None}, inplace=True)

    # 2) daty: to_datetime + strefa + NaT->None + Timestamp->datetime
    for c in (dt_cols_upsert & set(df_sub.columns)):
        s = pd.to_datetime(df_sub[c], errors="coerce", utc=False)
        try:
            if getattr(s.dt, "tz", None) is None:
                s = s.dt.tz_localize(LOCAL_TZ)
            else:
                s = s.dt.tz_convert(LOCAL_TZ)
        except Exception:
            pass
        df_sub[c] = s.where(pd.notna(s), None).map(
            lambda v: v.to_pydatetime() if isinstance(v, pd.Timestamp) else v
        )

    # 3) globalnie: NaN/NaT w DataFrame -> None
    df_sub = df_sub.where(pd.notna(df_sub), None)

    # 4) jeśli został STRING 'NaT' – zaloguj i wyczyść
    mask_nat_str = df_sub.applymap(lambda v: isinstance(v, str) and v.strip().upper() == "NAT")
    if mask_nat_str.values.any():
        bad_cols = [c for c in df_sub.columns if mask_nat_str[c].any()]
        bad_ids  = df_sub.loc[mask_nat_str.any(axis=1), "id"].head(10).tolist() if "id" in df_sub.columns else []
        logger.error("STRING 'NaT' przed UPSERT; kolumny=%s, id=%s", bad_cols, bad_ids)
        df_sub = df_sub.mask(mask_nat_str, None)

    return df_sub

def wczytaj_plik():
    # --- kolumny wymagane wg schematu ---
    required_cols = [
        "id","nazwa","status","opis","data_utworzenia","data_aktualizacji",
        "przypisani","category","regulations_acceptance","merchant_group",
        "base_account_type","merchant_mail","nip","merchant_id","merchant_adres",
        "merchant_mail_fv","produkty_merchant","return_adres","telefon_merchant",
        "website_merchant","next_meeting_date","held_first_meeting","status_bar_shortcut",
        "mail_warunki","warunki_akceptacja","main_category","merchant_bl_id",
        "merchant_kam","regulations_email_date_sent","regulations_acceptance_date",
        "relation_status"
    ]

    # --- kolumny WYKLUCZONE z zapisu (staging + UPSERT) ---
    EXCLUDE_COLS = {"next_meeting_date", "held_first_meeting", "data_aktualizacji"}

    t0 = time.perf_counter()
    logger.info("Start wczytywania pliku: %s", EXCEL_PATH)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)
    logging.getLogger("sqlalchemy.pool").setLevel(logging.WARNING)

    df = pd.read_excel(EXCEL_PATH, dtype={"ID": str})
    logger.info("Wczytano arkusz: %d wierszy, %d kolumn", len(df), len(df.columns))
    df = clean_df_emoji(df)
    logger.debug("Kolumny po czyszczeniu: %s", df.columns.tolist())

    # --- df_norm: mapowanie kolumn i normalizacje ---
    df_norm = pd.DataFrame()
    df_norm["id"] = df["ID"].astype(str).str.strip()

    df_norm["nazwa"] = df["Nazwa"].apply(lambda v: norm_text(v,2000))
    df_norm["status"] = df["Status"].apply(lambda v: norm_text(v,55))
    df_norm["opis"] = df["Opis"].apply(lambda v: norm_text(v,200))
    df_norm["data_utworzenia"] = df["Data utworzenia"].apply(norm_ts)
    # df_norm["data_aktualizacji"] = df["Data aktualizacji"].apply(norm_ts) 
    df_norm["przypisani"] = df["Przypisani"].apply(lambda v: norm_text(v, 100))
    df_norm["category"] = df["Category"].apply(lambda v: norm_text(v, 400))
    df_norm["regulations_acceptance"] = df["Regulations accept"].apply(norm_bool)
    df_norm["merchant_group"] = df["Merchant Group"].apply(lambda v: norm_text(v, 300))
    df_norm["base_account_type"] = df["Base. Account Type"].apply(norm_int)

    df_norm["merchant_mail"] = df["Merchant mail"].apply(lambda v: norm_email(v, 50))
    df_norm["nip"] = df["NIP"].apply(norm_nip)
    df_norm["merchant_id"] = df["Merchant ID"].apply(norm_int)
    df_norm["merchant_adres"] = df["Merchant Adres"].apply(lambda v: norm_text(v, 250))
    df_norm["merchant_mail_fv"] = df["Merchant Mail FV"].apply(lambda v: norm_email(v, 250))
    df_norm["produkty_merchant"] = df["Produkty Merchanta"].apply(lambda v: norm_text(v, 500))
    df_norm["return_adres"] = df["Return Adres"].apply(lambda v: norm_text(v, 200))
    df_norm["telefon_merchant"] = df["Telefon"].apply(lambda v: norm_phone(v, 250))
    df_norm["website_merchant"] = df["Website"].apply(lambda v: norm_url(v, 100))

    # df_norm["next_meeting_date"]  = df["Next Meeting Date:"].apply(norm_ts)  # WYKLUCZONA
    # df_norm["held_first_meeting"] = df["Held first meeting"].apply(norm_ts)  # WYKLUCZONA

    df_norm["status_bar_shortcut"] = df["StatusBar Shortcut"].apply(lambda v: norm_text(v, 250))
    df_norm["mail_warunki"] = df["Mail warunki"].apply(norm_bool)
    df_norm["warunki_akceptacja"] = df["Warunki"].apply(norm_bool)
    df_norm["main_category"] = df["Main Category"].apply(lambda v: norm_text(v, 100))
    df_norm["merchant_bl_id"] = df["Merchant BL ID"].apply(norm_int)
    df_norm["merchant_kam"] = df["Merchant KAM"].apply(lambda v: norm_text(v, 100))

    df_norm["regulations_email_date_sent"] = df["Regulations email date sent"].apply(norm_ts)
    df_norm["regulations_acceptance_date"] = df["Regulations accept date"].apply(norm_ts)

    df_norm["relation_status"] = pick(
        df,
        "Regulations Accept Relation",
        "Relation status",
        "Relation_status"
    ).apply(norm_int)

    logger.info("Znormalizowano: %d wierszy, %d kolumn", len(df_norm), len(df_norm.columns))

    # --- usuń kolumny wykluczone i przygotuj listę do stagingu ---
    df_norm.drop(columns=EXCLUDE_COLS, inplace=True, errors="ignore")
    required_cols_effective = [c for c in required_cols if c not in EXCLUDE_COLS]
    for col in required_cols_effective:
        if col not in df_norm.columns:
            df_norm[col] = None
    df_norm = df_norm[required_cols_effective]

    # --- sanity clean typów ---
    # liczby
    for c in ["base_account_type", "merchant_id", "merchant_bl_id", "relation_status", "nip"]:
        if c in df_norm.columns:
            df_norm[c] = pd.to_numeric(df_norm[c], errors="coerce").where(pd.notna(df_norm[c]), None)

    # daty (tylko te, które zostały — bez wykluczonych)
    dt_cols = [
        "data_utworzenia",
        "regulations_email_date_sent",
        "regulations_acceptance_date",
    ]
    df_norm.replace({"NaT": None, "NAT": None, "nat": None, "": None}, inplace=True)
    for c in dt_cols:
        if c in df_norm.columns:
            s = pd.to_datetime(df_norm[c], errors="coerce", utc=False)
            try:
                if getattr(s.dt, "tz", None) is None:
                    s = s.dt.tz_localize(LOCAL_TZ)
                else:
                    s = s.dt.tz_convert(LOCAL_TZ)
            except Exception:
                pass
            df_norm[c] = s.where(pd.notna(s), None).map(
                lambda v: v.to_pydatetime() if isinstance(v, pd.Timestamp) else v
            )

    # bo status jest NOT NULL
    if "status" in df_norm.columns:
        df_norm["status"] = df_norm["status"].map(lambda s: s if (isinstance(s,str) and s.strip()) else "unknown")

    # --- typy do to_sql (tylko obecne kolumny) ---
    dtype_map_full = {
        "data_utworzenia": DateTime(timezone=True),
        "regulations_email_date_sent": DateTime(timezone=True),
        "regulations_acceptance_date": DateTime(timezone=True),
        "base_account_type": Integer(),
        "merchant_id": Integer(),
        "merchant_bl_id": Integer(),
        "relation_status": Integer(),
        "nip": BigInteger(),
        "status": String(55),
        "nazwa": String(2000),
        "opis": String(200),
        "przypisani": String(100),
        "category": String(400),
        "merchant_group": String(300),
        "merchant_mail": String(50),
        "merchant_adres": String(250),
        "merchant_mail_fv": String(250),
        "produkty_merchant": String(500),
        "return_adres": String(200),
        "telefon_merchant": String(250),
        "website_merchant": String(100),
        "status_bar_shortcut": String(250),
        "main_category": String(100),
        "merchant_kam": String(100),
    }
    dtype_map = {k: v for k, v in dtype_map_full.items() if k in df_norm.columns}

    engine = create_engine(DB_URL, future=True, echo=False)

    # --- staging: TRUNCATE i insert ---
    with engine.begin() as con:
        con.execute(text("TRUNCATE TABLE merchanci_staging;"))

    diagnose_df(df_norm)

    df_norm.to_sql(
        "merchanci_staging",
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=2000,
        dtype=dtype_map
    )

    # --- UPSERT do merchanci (bez EXCLUDE_COLS) ---
    meta = MetaData()
    t_main = Table("merchanci", meta, autoload_with=engine)

    db_cols = [c.name for c in t_main.columns]
    use_cols = [c for c in df_norm.columns if c in db_cols and c not in EXCLUDE_COLS]

    conflict_key = ["id"]
    update_cols = [c for c in use_cols if c not in conflict_key]

    # lista czasowych, które realnie idą do UPSERT-u
    dt_cols_upsert = {"data_utworzenia", "regulations_email_date_sent", "regulations_acceptance_date"} & set(use_cols)

    with engine.begin() as conn:
        for start in range(0, len(df_norm), CHUNK_SIZE):
            chunk = df_norm.iloc[start:start+CHUNK_SIZE][use_cols]
            chunk = _sanitize_df_for_sql(chunk, dt_cols_upsert)

            DT_COLS = [
    "data_utworzenia",
    "regulations_email_date_sent",
    "regulations_acceptance_date",
]
        INT_COLS    = ["base_account_type", "merchant_id", "merchant_bl_id", "relation_status"]
        BIGINT_COLS = ["nip"]

        # 0) globalnie: stringi 'NaT'/'NaN'/'None'/'' -> None
        chunk = chunk.applymap(
            lambda v: None if (isinstance(v, str) and v.strip().lower() in {"nat","nan","none",""}) else v
        )

        # 1) liczby -> null
        for c in set(INT_COLS + BIGINT_COLS) & set(chunk.columns):
            chunk[c] = pd.to_numeric(chunk[c], errors="coerce")
            chunk[c] = chunk[c].where(pd.notna(chunk[c]), None)

        # 2) daty: to_datetime + strefa + NaT->None + Timestamp->python datetime
        for c in set(DT_COLS) & set(chunk.columns):
            s = pd.to_datetime(chunk[c], errors="coerce", utc=False)
            try:
                if getattr(s.dt, "tz", None) is None:
                    s = s.dt.tz_localize(LOCAL_TZ)
                else:
                    s = s.dt.tz_convert(LOCAL_TZ)
            except Exception:
                pass
            chunk[c] = s.where(pd.notna(s), None).map(
                lambda v: v.to_pydatetime() if isinstance(v, pd.Timestamp) else v
            )

        # 3) ostatecznie: pandasowe NaT/NaN -> None (także gdy coś się prześlizgnęło)
        chunk = chunk.replace({pd.NaT: None}).where(pd.notna(chunk), None)

        # 4) kontrolny log – jeśli JAKIKOLWIEK 'NaT' (string) został
        _nat_mask = chunk.applymap(lambda v: isinstance(v, str) and v.strip().lower() == "nat")
        if _nat_mask.values.any():
            _bad_cols = [c for c in chunk.columns if _nat_mask[c].any()]
            _bad_ids  = chunk.loc[_nat_mask.any(axis=1), "id"].tolist()[:10]
            logger.error("STRING 'NaT' tuż przed UPSERT; kolumny=%s; id=%s", _bad_cols, _bad_ids)

            records = chunk.to_dict(orient="records")
            stmt = insert(t_main).values(records)
            stmt = stmt.on_conflict_do_update(
                index_elements=conflict_key,
                set_={col: getattr(stmt.excluded, col) for col in update_cols}
            )
            conn.execute(stmt)
            mask_nat_str = chunk.applymap(lambda v: isinstance(v, str) and v.strip().lower()=="nat")
            if mask_nat_str.values.any():
                bad_cols = [c for c in chunk.columns if mask_nat_str[c].any()]
                bad_ids  = chunk.loc[mask_nat_str.any(axis=1), "id"].tolist()[:10]
                logger.error("STRING 'NaT' tuż przed UPSERT; kolumny=%s; id=%s", bad_cols, bad_ids)


    print("Ok zaktualizowano bazę danych")

    with engine.begin() as con:
        nonnull_rel = con.execute(text(
            "SELECT COUNT(*) FROM merchanci WHERE relation_status IS NOT NULL"
        )).scalar_one()
        logger.info("Wiersze z ustawionym relation_status: %s", nonnull_rel)

        sample = con.execute(text(
            "SELECT id, status, relation_status "
            "FROM merchanci "
            "ORDER BY data_aktualizacji DESC NULLS LAST, id "
            "LIMIT 5"
        )).mappings().all()
        logger.info("Próbka po UPSERCIE: %s", [dict(r) for r in sample])

    elapsed = time.perf_counter() - t0
    logger.info("KONIEC: %.2f s (wierszy wejściowych: %d)", elapsed, len(df))

    with engine.begin() as con:
        con.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_merchanci_nip_notnull
            ON merchanci (nip)
            WHERE nip IS NOT NULL;
        """))

    sql = text("""
WITH ranked AS (
  SELECT
    s.*,
    ROW_NUMBER() OVER (
      PARTITION BY s.nip
      ORDER BY
        s.regulations_acceptance_date DESC NULLS LAST,
        s.regulations_email_date_sent DESC NULLS LAST,
        s.data_utworzenia DESC NULLS LAST,
        s.id DESC
    ) AS rn
  FROM merchanci_staging s
  WHERE s.nip IS NOT NULL
)
INSERT INTO merchanci AS m (
  id, nazwa, status, opis, data_utworzenia, przypisani, category,
  regulations_acceptance, merchant_group, base_account_type, merchant_mail,
  nip, merchant_id, merchant_adres, merchant_mail_fv, produkty_merchant,
  return_adres, telefon_merchant, website_merchant, status_bar_shortcut,
  mail_warunki, warunki_akceptacja, main_category, merchant_bl_id,
  merchant_kam, regulations_email_date_sent, regulations_acceptance_date,
  relation_status
)
SELECT
  r.id, r.nazwa, r.status, r.opis, r.data_utworzenia, r.przypisani, r.category,
  r.regulations_acceptance, r.merchant_group, r.base_account_type, r.merchant_mail,
  r.nip, r.merchant_id, r.merchant_adres, r.merchant_mail_fv, r.produkty_merchant,
  r.return_adres, r.telefon_merchant, r.website_merchant, r.status_bar_shortcut,
  r.mail_warunki, r.warunki_akceptacja, r.main_category, r.merchant_bl_id,
  r.merchant_kam, r.regulations_email_date_sent, r.regulations_acceptance_date,
  r.relation_status
FROM ranked r
WHERE r.rn = 1
ON CONFLICT (nip) DO NOTHING ;
""")

    with engine.begin() as con:
        con.execute(sql)


if __name__ == "__main__":
    wczytaj_plik()


path = "clickup_tasks_clean3.xlsx"

