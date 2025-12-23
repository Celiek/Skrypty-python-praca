import os
import time
import random
import re
import logging
import psycopg2
import pandas as pd

from dotenv import load_dotenv
from argparse import ArgumentParser
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from rapidfuzz import fuzz


# =====================================================
# LOGGING
# =====================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


# =====================================================
# ENV + DB
# =====================================================
load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    database=os.getenv("DB_NAME"),
    port=os.getenv("DB_PORT"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)


# =====================================================
# HELPERS
# =====================================================
def losowe_opoznienie(min_sec: float, max_sec: float):
    time.sleep(random.uniform(min_sec, max_sec))


def pobierz_dane(conn):
    df = pd.read_sql(
        """
        SELECT id, nip, nazwa
        FROM merchanci
        WHERE nip IS NOT NULL
        ORDER BY nip
        """,
        conn
    )
    return df


# =====================================================
# NORMALIZACJA NAZW
# =====================================================
REPLACEMENTS = {
    "SP Z O O": "SPOLKA Z OGRANICZONA ODPOWIEDZIALNOSCIA",
    "SP.Z O.O": "SPOLKA Z OGRANICZONA ODPOWIEDZIALNOSCIA",
    "SP. Z O.O.": "SPOLKA Z OGRANICZONA ODPOWIEDZIALNOSCIA",
    "SPÓŁKA": "SPOLKA",
}


def normalize_name(name: str) -> str:
    if not name:
        return ""

    name = name.upper()

    for k, v in REPLACEMENTS.items():
        name = name.replace(k, v)

    name = re.sub(r"[^\w\s]", " ", name)
    name = re.sub(r"\s+", " ", name)

    return name.strip()


# =====================================================
# SCRAPER GUS
# =====================================================
class RegonScraper:
    def __init__(self, chromedriver_path, headless=True):
        self.chromedriver_path = chromedriver_path
        self.headless = headless
        self.driver = None

    def __enter__(self):
        options = Options()
        if self.headless:
            options.add_argument("--headless=new")
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1920x1080")
            options.add_argument("--log-level=3")

        service = Service(self.chromedriver_path, log_path=os.devnull)
        self.driver = webdriver.Chrome(service=service, options=options)
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.driver:
            self.driver.quit()

    def scrape_nip(self, nip: str) -> str | None:
        d = self.driver
        d.get("https://wyszukiwarkaregon.stat.gov.pl/appBIR/index.aspx")

        losowe_opoznienie(0.4, 0.8)

        wait = WebDriverWait(d, 10)
        pole = wait.until(EC.element_to_be_clickable((By.ID, "txtNip")))

        try:
            pole.clear()
        except Exception:
            d.execute_script("arguments[0].value = '';", pole)

        pole.send_keys(nip)
        d.find_element(By.ID, "btnSzukaj").click()

        losowe_opoznienie(0.6, 1.3)

        rows = (
            d.find_elements(By.CLASS_NAME, "tabelaZbiorczaListaJednostekRow") +
            d.find_elements(By.CLASS_NAME, "tabelaZbiorczaListaJednostekAltRow")
        )

        if not rows:
            return None

        cells = [c.text.strip() for c in rows[0].find_elements(By.TAG_NAME, "td")]
        if len(cells) < 2:
            return None

        return cells[2]  # NAZWA PODMIOTU


# =====================================================
# MAIN LOGIC
# =====================================================
def sprawdz_nazwy(limit, delay_min, delay_max):
    df = pobierz_dane(conn)
    if limit:
        df = df.head(limit)

    wyniki = []

    CHROMEDRIVER_PATH = os.getenv(
        "CHROMEDRIVER_PATH",
        r"C:\tools\chromedriver-win64\chromedriver.exe"
    )

    log.info("START | rekordy=%s", len(df))

    with RegonScraper(CHROMEDRIVER_PATH, headless=True) as scraper:
        for _, row in df.iterrows():
            nip = re.sub(r"\D", "", str(row["nip"]))
            if len(nip) != 10:
                continue

            try:
                nazwa_gus = scraper.scrape_nip(nip)
            except Exception as e:
                log.error("BŁĄD GUS | NIP=%s | %s", nip, e)
                continue

            losowe_opoznienie(delay_min, delay_max)

            if not nazwa_gus:
                log.warning("BRAK W GUS | NIP=%s", nip)
                continue

            db_name = normalize_name(row["nazwa"])
            gus_name = normalize_name(nazwa_gus)

            score = fuzz.token_set_ratio(db_name, gus_name)

            if score < 90:
                wyniki.append({
                    "id": row["id"],
                    "nip": nip,
                    "nazwa_db": row["nazwa"],
                    "nazwa_gus": nazwa_gus,
                    "similarity": score,
                    "status": "ROZBIEŻNOŚĆ"
                })
                log.warning(
                    "ROZBIEŻNOŚĆ | NIP=%s | %.1f%%", nip, score
                )

    if wyniki:
        out = pd.DataFrame(wyniki)
        out.sort_values("similarity", inplace=True)
        out.to_excel("rozbieznosci_nazw_gus.xlsx", index=False)
        log.info("Zapisano %s rekordów do rozbieznosci_nazw_gus.xlsx", len(out))
    else:
        log.info("Brak rozbieżności")

    conn.close()


# =====================================================
# CLI
# =====================================================
def parse_args():
    p = ArgumentParser()
    p.add_argument("--limit", type=int)
    p.add_argument("--delay-min", type=float, default=0.6)
    p.add_argument("--delay-max", type=float, default=1.8)
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    sprawdz_nazwy(
        limit=args.limit,
        delay_min=args.delay_min,
        delay_max=args.delay_max,
    )
