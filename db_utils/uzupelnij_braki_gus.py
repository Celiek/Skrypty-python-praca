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
# CLEANERS
# =====================================================
DASHES_RE = re.compile(r"^[\-\u2010-\u2015\u2212]+$")

def clean_adres(adres: str) -> str:
    parts = [p.strip() for p in adres.split(",")]

    cleaned = []
    for p in parts:
        if not p:
            continue

        # ❌ usuń pola złożone wyłącznie z myślników (ASCII + Unicode)
        if DASHES_RE.fullmatch(p):
            continue

        cleaned.append(p)

    return ", ".join(cleaned)

# =====================================================
# HELPERS
# =====================================================
def losowe_opoznienie(min_sec: float, max_sec: float):
    time.sleep(random.uniform(min_sec, max_sec))

# merchanci z krótką nazwą:
def pobierz_dane(conn):
    return pd.read_sql(
        "select * from merchanci where LENGTH(adres) < 35;",
        conn
    )

CHROMEDRIVER_PATH = os.getenv(
    "CHROMEDRIVER_PATH",
    r"C:\tools\chromedriver-win64\chromedriver.exe"
)

# =====================================================
# SCRAPER
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
        losowe_opoznienie(0.3, 0.7)

        wait = WebDriverWait(d, 10)

        pole = wait.until(
            EC.element_to_be_clickable((By.ID, "txtNip"))
        )

        # zabezpieczenie na zwiechy GUS
        try:
            pole.clear()
        except Exception:
            d.execute_script("arguments[0].value = '';", pole)

        pole.send_keys(nip)

        d.find_element(By.ID, "btnSzukaj").click()
        losowe_opoznienie(0.5, 1.2)

        rows = (
            d.find_elements(By.CLASS_NAME, "tabelaZbiorczaListaJednostekRow") +
            d.find_elements(By.CLASS_NAME, "tabelaZbiorczaListaJednostekAltRow")
        )

        if not rows:
            return None

        cells = [c.text.strip() for c in rows[0].find_elements(By.TAG_NAME, "td")]
        if len(cells) < 9:
            return None

        adres = ", ".join([cells[6], cells[7], cells[8]])
        return clean_adres(adres)

# =====================================================
# MAIN LOGIC
# =====================================================
def update_adresy(dry_run, limit, delay_min, delay_max):
    df = pobierz_dane(conn)
    if limit:
        df = df.head(limit)

    log.info(
        "START | rekordy=%s | DRY_RUN=%s | delay=%.2f–%.2f",
        len(df), dry_run, delay_min, delay_max
    )

    with RegonScraper(CHROMEDRIVER_PATH, headless=True) as scraper:
        with conn.cursor() as cur:

            for _, row in df.iterrows():
                nip = re.sub(r"\D", "", str(row["nip"]))
                if len(nip) != 10:
                    continue

                adres = scraper.scrape_nip(nip)

                # ⏱️ delay z CLI
                losowe_opoznienie(delay_min, delay_max)

                if not adres:
                    log.warning("BRAK | NIP=%s", nip)
                    continue

                if dry_run:
                    log.info("DRY_RUN | NIP=%s | %s", nip, adres)
                else:
                    cur.execute(
                        "UPDATE merchanci SET adres=%s WHERE id=%s",
                        (adres,int(row["id"]))
                    )
                    log.info("ZAPISANO | NIP=%s", nip)

            if not dry_run:
                conn.commit()
                log.info("COMMIT")

# =====================================================
# CLI
# =====================================================
def parse_args():
    p = ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--limit", type=int)
    p.add_argument("--delay-min", type=float, default=0.6)
    p.add_argument("--delay-max", type=float, default=1.8)
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    update_adresy(
        dry_run=args.dry_run,
        limit=args.limit,
        delay_min=args.delay_min,
        delay_max=args.delay_max,
    )
