import os
import time
import random
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# === KONFIGURACJA ===
load_dotenv()
DB_URL = os.getenv("DB_URL")  # np. "postgresql+psycopg2://user:pass@host:5432/db"
CHROMEDRIVER_PATH = os.getenv("CHROMEDRIVER_PATH", r"C:\tools\chromedriver-win64\chromedriver.exe")

# === FUNKCJE POMOCNICZE ===
def losowe_opoznienie(min_sec=0.05, max_sec=0.15):
    time.sleep(random.uniform(min_sec, max_sec))

# === KLASA SCRAPERA ===
class RegonScraper:
    def __init__(self, chromedriver_path: str = CHROMEDRIVER_PATH, headless: bool = True):
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
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        service = Service(self.chromedriver_path, log_path=os.devnull)
        self.driver = webdriver.Chrome(service=service, options=options)
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.driver:
            self.driver.quit()

    def scrape_nip(self, nip: str) -> list[str]:
        d = self.driver
        d.get("https://wyszukiwarkaregon.stat.gov.pl/appBIR/index.aspx")
        losowe_opoznienie(0.05, 0.25)
        d.find_element(By.ID, "txtNip").clear()
        d.find_element(By.ID, "txtNip").send_keys(str(nip))
        d.find_element(By.ID, "btnSzukaj").click()
        losowe_opoznienie(0.15, 0.3)

        rows = d.find_elements(By.CLASS_NAME, "tabelaZbiorczaListaJednostekAltRow") + \
                d.find_elements(By.CLASS_NAME, "tabelaZbiorczaListaJednostekRow")

        if not rows:
            return []

        cells = rows[0].find_elements(By.TAG_NAME, "td")
        return [c.text.strip() for c in cells]


# === GŁÓWNA LOGIKA ===
def main():
    engine = create_engine(DB_URL)
    
    # 1️⃣ Pobierz wszystkie NIP-y z tabeli merchanci
    df = pd.read_sql("SELECT nip FROM merchanci WHERE nip IS NOT NULL", con=engine)
    print(f"📥 Znaleziono {len(df)} NIP-ów w bazie")

    # 2️⃣ Dla każdego NIP-u scrapuj adres i zapisz do bazy
    with RegonScraper(headless=True) as scraper:
        for idx, row in df.iterrows():
            nip = str(row["nip"]).strip()
            if not nip:
                continue

            cells = scraper.scrape_nip(nip)

            if cells and len(cells) >= 5:
                # zbuduj adres od 4. elementu (indeks 3)
                adres = ", ".join(cells[3:-1]).strip(" ,")
                print(f"✅ {nip}: {adres}")

                with engine.begin() as conn:
                    conn.execute(
                        text("UPDATE merchanci SET adres = :adres WHERE nip = :nip"),
                        {"adres": adres, "nip": nip}
                    )
            else:
                print(f"Nie znaleziono adresu dla NIP: {nip}")


            # mała przerwa między zapytaniami
            losowe_opoznienie(1, 2)

    print("🏁 Zakończono aktualizację adresów w bazie.")

# === URUCHOMIENIE ===
if __name__ == "__main__":
    main()