import os
import time
import random
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup


class RegonScraper:

    CHROMEDRIVER_PATH = os.getenv(
        "CHROMEDRIVER_PATH",
        r"C:\tools\chromedriver-win64\chromedriver.exe"
    )

    @staticmethod
    def losowe_opoznienie(min_sec=0.05, max_sec=0.1):
        time.sleep(random.uniform(min_sec, max_sec))

    def __init__(self, chromedriver_path: str = CHROMEDRIVER_PATH, headless: bool = True):
        self.chromedriver_path = chromedriver_path
        self.headless = headless
        self.driver = None

        # TE DWA POLA SŁUŻĄ DO ZAPISANIA HTML W PAMIĘCI
        self.html = None   # surowy HTML
        self.soup = None   # sparsowany BS4

    def __enter__(self):
        options = Options()

        if self.headless:
            options.add_argument("--headless=new")
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1920x1080")
            options.add_argument("--log-level=3")
            options.add_argument("--disable-logging")
            options.add_argument("--silent")
            options.add_experimental_option('excludeSwitches', ['enable-logging'])

        service = Service(self.chromedriver_path, log_path=os.devnull)
        self.driver = webdriver.Chrome(service=service, options=options)

        return self

    def __exit__(self, exc_type, exc, tb):
        if self.driver:
            self.driver.quit()

    def scrape_nip(self, nip: str) -> list[str]:
        d = self.driver

        # Załaduj stronę
        d.get("https://wyszukiwarkaregon.stat.gov.pl/appBIR/index.aspx")

        # Wprowadź NIP
        pole = d.find_element(By.ID, "txtNip")
        pole.clear()
        pole.send_keys(str(nip))

        # Kliknij szukaj
        d.find_element(By.ID, "btnSzukaj").click()
        self.losowe_opoznienie()

        # ✅ ZAPISZ HTML DO PAMIĘCI
        self.html = d.page_source
        self.soup = BeautifulSoup(self.html, "html.parser")

        # ✅ PARSUJEMY JUŻ TYLKO SELENIUM (tak jak było)
        rows = d.find_elements(By.CLASS_NAME, "tabelaZbiorczaListaJednostekAltRow") + \
               d.find_elements(By.CLASS_NAME, "tabelaZbiorczaListaJednostekRow")

        if not rows:
            return []

        cells = rows[0].find_elements(By.TAG_NAME, "td")
        return [c.text.strip() for c in cells]
    
def filter_wynik(wynik):
    # indeksy do usunięcia
    to_remove = {1, 2, 3, 5}

    # usuń pola po indeksach
    wynik = [v for i, v in enumerate(wynik) if i not in to_remove]

    # usuń ostatni element jeśli to tylko ------
    if wynik and wynik[-1].strip("-").strip() == "":
        wynik = wynik[:-1]

    return wynik

    
with RegonScraper(headless=False) as scraper:

    wynik_raw = scraper.scrape_nip("9451972201")
    wynik = filter_wynik(wynik_raw)
    print(wynik)
