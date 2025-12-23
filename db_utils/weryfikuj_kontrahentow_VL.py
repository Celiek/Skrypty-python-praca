import os
import re
import time
import requests
import pandas as pd
import psycopg2
from datetime import date
from dotenv import load_dotenv

# =====================================================
# KONFIGURACJA
# =====================================================
load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT", 5432),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

API_URL = "https://wl-api.mf.gov.pl/api/search/nips/"
CHECK_DATE = date.today().isoformat()
BATCH_SIZE = 30
OUT_FILE = "lista_sprawdzonych_kontrahentow.xlsx"

# =====================================================
# POMOCNICZE
# =====================================================
def clean_nip(nip: str) -> str | None:
    nip = re.sub(r"\D", "", str(nip))
    return nip if len(nip) == 10 else None


def chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]


# =====================================================
# POBIERANIE NIP-ÓW Z BAZY
# =====================================================
def load_nips_from_db() -> list[str]:
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("""
        SELECT DISTINCT nip
        FROM merchanci
        WHERE nip IS NOT NULL
    """)

    rows = cur.fetchall()
    conn.close()

    nips = []
    for (nip,) in rows:
        nip_clean = clean_nip(nip)
        if nip_clean:
            nips.append(nip_clean)

    return sorted(set(nips))


# =====================================================
# ZAPYTANIE DO BIAŁEJ LISTY
# =====================================================
def query_white_list(nips: list[str]) -> list[dict]:
    joined = ",".join(nips)
    url = f"{API_URL}{joined}?date={CHECK_DATE}"

    resp = requests.get(url, headers={"Accept": "application/json"}, timeout=30)

    if resp.status_code != 200:
        return [
            {"nip": nip, "status": f"API_ERROR_{resp.status_code}"}
            for nip in nips
        ]

    data = resp.json()

    results = []

    entries = data.get("result", {}).get("entries", [])

    for entry in entries:
        for subject in entry.get("subjects", []):
            results.append({
                "nip": subject.get("nip"),
                "status": subject.get("statusVat")
            })

    return results


# =====================================================
# MAIN
# =====================================================
def main():
    print("📥 Pobieram NIP-y z bazy…")
    nips = load_nips_from_db()
    print(f"✅ Znaleziono {len(nips)} NIP-ów")

    all_results = []

    for idx, batch in enumerate(chunked(nips, BATCH_SIZE), start=1):
        print(f"🔎 Sprawdzam paczkę {idx} ({len(batch)} NIP-ów)")
        try:
            res = query_white_list(batch)
            all_results.extend(res)
        except Exception as e:
            all_results.append({
                "NIP": ",".join(batch),
                "Komunikat": f"EXCEPTION: {e}"
            })

        time.sleep(0.3)  # lekkie throttling

    df = pd.DataFrame(all_results)
    df.to_excel(OUT_FILE, index=False)

    print(f"📊 Zapisano wynik do: {OUT_FILE}")


if __name__ == "__main__":
    main()
