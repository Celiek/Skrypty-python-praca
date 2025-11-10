import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import re

# ================= CONFIG =================

INPUT_XLSX = "za sprzedaż z października (1).xlsx"
OUTPUT_XLSX = "dane kontrahentow2.xlsx"

# Dane do PostgreSQL (podmień swoimi)
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "merchanci"
DB_USER = "gabriel"
DB_PASS = "lhj7r7nk7e"
TABLE = "merchanci"

# ===========================================
def clean_nip(nip: str) -> str:
    """Usuwa spacje, myślniki i znaki niecyfrowe z NIP."""
    return re.sub(r"\D", "", str(nip or "").strip())


def parse_address(adres: str) -> dict:
    """
    Rozbija adres w formacie:
    MIASTO|KOD|MIEJSCOWOŚĆ|ULICA I NUMER
    """
    if not adres or not isinstance(adres, str):
        return {
            "Ulica i nr": "",
            "Kod pocztowy": "",
            "Miejscowość": "",
            "Kraj": "Polska"
        }

    parts = adres.split("|")

    # Uzupełnij brakujące części, jeśli format krótszy
    while len(parts) < 4:
        parts.append("")

    miasto_raw, kod_raw, miejsc_raw, ulica_raw = [p.strip() for p in parts]

    # Ulica może być pusta lub "--"
    if ulica_raw in ["", "‐‐‐‐‐‐‐‐‐‐", "---", "--"]:
        ulica_raw = ""

    return {
        "Ulica i nr": ulica_raw,
        "Kod pocztowy": kod_raw,
        "Miejscowość": miejsc_raw,
        "Kraj": "Polska"
    }


def load_addresses_from_db() -> pd.DataFrame:
    """Pobiera: nip, adres, email z bazy."""
    sql = f"""
        SELECT
            nip,
            adres,
            COALESCE(email, email) AS email
        FROM {TABLE}
        WHERE nip IS NOT NULL AND adres IS NOT NULL;
    """

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    conn.close()

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=[
            "nip", "Ulica i nr", "Kod pocztowy",
            "Miejscowość", "Kraj", "E-mail klienta"
        ])

    df["nip"] = df["nip"].astype(str).map(clean_nip)

    parsed = df["adres"].map(parse_address).apply(pd.Series)

    return pd.concat([df["nip"], parsed, df["email"]], axis=1) \
             .rename(columns={"email": "E-mail klienta"})


def main():
    print("📥 Wczytywanie Excela wejściowego…")
    df_in = pd.read_excel(INPUT_XLSX, dtype=str)

    if "NIP" not in df_in.columns:
        raise ValueError("Brakuje kolumny 'NIP' w pliku wejściowym!")

    df_in["NIP"] = df_in["NIP"].map(clean_nip)

    print("🗄️ Pobieranie adresów z bazy…")
    df_db = load_addresses_from_db()

    print("🔗 Łączenie danych po NIP…")
    df_out = df_in.merge(
        df_db,
        left_on="NIP",
        right_on="nip",
        how="left"
    ).drop(columns=["nip"], errors="ignore")

    # końcowa kolejność kolumn
    extra_cols = ["Ulica i nr", "Kod pocztowy", "Miejscowość", "Kraj", "E-mail klienta"]
    ordered_cols = list(df_in.columns) + extra_cols
    df_out = df_out.reindex(columns=ordered_cols)

    print("💾 Zapis do pliku Excel…")
    df_out.to_excel(OUTPUT_XLSX, index=False)

    print(f"✅ GOTOWE! Plik zapisano jako: {OUTPUT_XLSX}")


if __name__ == "__main__":
    main()