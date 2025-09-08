import os
import random
import time
from datetime import datetime
from decimal import Decimal

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
## WERSJA NIEDOKOŃCZONA

# TODO
# zapis do pliku txt i kopia w excellu (DONE)
# sprawdzanie dubli w pliku
# sprawdzanie poprawności danych przez nip
# zaczytywanie danych do faktur z worda (DONE)
# zaczytać dane z bazy danych (DONE)
# "pobieranie" danych kontrahenta z gusu (DONE)
# zapisywanie wysłanych faktur do bazy danych
# zamiana skryptu w narzędzie cli
# weryfikacja istnienia kontrahenta w bazie danych kontrahentów a potem w bazie gusu

# =========================
# KOnfiguracje i narzędzia
# =========================

load_dotenv()
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

CHROMEDRIVER_PATH = os.getenv("CHROMEDRIVER_PATH", r"C:\tools\chromedriver-win64\chromedriver.exe")

OUTPUT_DIR = os.getenv("OUTPUT_DIR", ".")
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_TXT = os.path.join(OUTPUT_DIR, f"przelewy_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")


def losowe_opoznienie(min_sec = 0.05,max_sec = 1.435):
    czas = random.uniform(min_sec, max_sec)
    time.sleep(czas)

def scrapowanie(nip):
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920x1080")

    service = Service(r"C:\tools\chromedriver-win64\chromedriver.exe")
    driver = webdriver.Chrome(service=service)
    driver.get("https://example.com")

    try:
        driver.get("https://wyszukiwarkaregon.stat.gov.pl/appBIR/index.aspx")
        losowe_opoznienie()
        driver.find_element(By.ID, "txtNip").send_keys(str(nip))
        driver.find_element(By.ID, "btnSzukaj").click()
        losowe_opoznienie()

        rows = driver.find_elements(By.CLASS_NAME, "tabelaZbiorczaListaJednostekAltRow")
        if not rows:
            print("Brak wyników dla NIP:", nip)
            return []
        cells = rows[0].find_elements(By.TAG_NAME, "td")
        dane = [c.text.strip() for c in cells]
        # Bezpieczne cięcie: jeśli brakuje kolumn, zwróć co jest
        fragment = dane[5:9][::-1] if len(dane) >= 9 else dane
        return fragment
    finally:
        driver.quit()


# # nr identyfikacyjne banku do Debugowania
# banki = {1010:'NBP',
#          1022:'PKOBP',
#          1030:'Citi Handlowy',
#          1050:'ING Bank Śląski',
#          1090:'Santander Bank Polska',
#          1140:'mBank',
#          1160:'Millenium',
#          1240:'Pekao Sa',
#          1870:'Nest Bank',
#          1940:'Credit Agricole',
#          2030:'BNP Paribas',
#          2120:'Santander Consumer Bank',
#          2480:'VeloBank',
#          2490:'Alior Bank',
#          2790:'Raiffeisen Bank',
#          2901:"Aion Bank"}
#
# # 110 zwykły przelew
# # 210 polecenie zapłaty / Zapłaty Split
# # 410 polecenie przelewu zagranicznego
# # statyczne pole
# polecenie_zaplaty = '210'
#
# # format RRRRMMDD
# # z pliku :
# data_platnosci = '20250817'
# # kwota platnosci w groszach
# kwota_platnosci = '2500000'
# # kod przelewu to 8 cyfr z nr konta bankowego = kod banku + 4 następne cyfry
# nr_rozliczeniowy_banku_kontrahenta = '10205561'
# # zlecenie split static
# tryb_realizacji_platnosci = '210'
# # nasz nr rachunku
# nr_rachunku_zleceniodawcy ='84102055610000380200040857'
# # nr rachunku kontrachenta
# nr_rachunku_kontrahenta ='18102055610000310200035501'
# # rozdzielane przecinkiem
# nazwa_i_adres_zleceniodawcy='\"Super Merchant, Super Merchant S.A, al. 1 maja 31/33, 90-739 Łódź\"'
# # rozdzielane przecinkiem
# nazwa_i_adres_kontrahenta='\"ADITECH Sp. z.o.o,  ADITECH Sp. z.o.o, ul. Cicha 17, 37-514 Tuczempy Przemyśl \"'
#
# # związane z poleceniem zapłaty
# oplaty_i_prowizje = "0"
# # 8 cyfr po nr identyfikacyjnym
# nr_rozliczeniowy_banku_kontrahenta = "10205561"
# pole_13=""
# pole_14 =""
# # dla przelewu split wartość to 01
# klasyfikacja_polecenia = "01"
# informacja_klient_bank = "faktura XX/XX/XX"
#
# #NIP|NR faktury| Kwota Vat| Kwota Brutto
# szczegoly_platnosci = "1234567890| Faktura VAT 4974/2025|230|1230"


# zamiana formatu daty z DDMMYYYY na YYYYMMDD
def serializacja_dat(data):
    # Jeśli data jest obiektem datetime, nie trzeba parsować
    if isinstance(data, datetime):
        return data.strftime('%Y%m%d')

    # Próba parsowania różnych formatów tekstowych
    for fmt in ("%d.%m.%Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(data, fmt).strftime('%Y%m%d')
        except ValueError:
            continue

    raise ValueError(f"Nieobsługiwany format daty: {data}")

# sprawdzanie czy nr_konta kontrchenta jest w naszej bazie danych
def nr_konta_z_bazy(nip):

    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT nr_konta FROM merchant WHERE nip = %s", (str(nip),))
                row = cursor.fetchone()
        return row[0] if row else None
    except psycopg2.Error as e:
        print(f"błąd bazy danych: {e.pgerror or str(e)}")
        return None

# sprawdzanie czy podany nip ma przyporządkowany adres jeśli nie to go dodaje

def sprawdz_czy_istnieje_adres(nip: str):
    """
    Sprawdza, czy w tabeli merchant istnieje adres dla danego NIP.
    Zwraca:
        - string z adresem, jeśli istnieje
        - None, jeśli brak rekordu lub brak adresu
    """
    load_dotenv()

    db_config = {
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT"),
    }

    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT adres FROM merchant WHERE nip = %s",
                    (str(nip),)
                )
                wynik = cursor.fetchone()

        if wynik and wynik[0]:
            print(f"Adres dla NIP-u {nip} już istnieje w bazie.")
            return wynik[0]  # adres jako string
        else:
            print(f"Brak adresu dla NIP-u {nip} — trzeba pobrać.")
            return None

    except psycopg2.Error as e:
        print(f"Błąd bazy danych: {e.pgerror or str(e)}")
        return None
    except Exception as e:
        print(f"Nieoczekiwany błąd: {e}")
        return None

# odczyt pliku xlsx i zapis do pliku .txt
def oczyt_pliku(input_file: str):
    df = pd.read_excel(input_file)

    for _, row in df.iterrows():
        print(f"{row['Numer dokumentu']}, {row['Kontrahent']}")

        nazwa_i_adres_zleceniodawcy = '"Super Merchant, Super Merchant S.A, al. 1 maja 31/33, 90-739 Łódź"'
        nip = str(row['NIP']).strip()

        # Adres kontrahenta
        if not sprawdz_czy_istnieje_adres(nip):
            scraped = scrapowanie(nip)
            nazwa_i_adres_kontrahenta = "|".join(scraped) if scraped else ""
            if nazwa_i_adres_kontrahenta:
                # Zapis do bazy
                load_dotenv()
                conn = psycopg2.connect(
                    dbname=os.getenv("DB_NAME"),
                    user=os.getenv("DB_USER"),
                    password=os.getenv("DB_PASSWORD"),
                    host=os.getenv("DB_HOST"),
                    port=os.getenv("DB_PORT"),
                )
                with conn:
                    with conn.cursor() as cursor:
                        cursor.execute(
                            "UPDATE merchant SET adres = %s WHERE nip = %s",
                            (nazwa_i_adres_kontrahenta, nip),
                        )
                conn.close()
        else:
            # Jeśli adres już jest, też możemy spróbować go odświeżyć scrapem (opcjonalnie)
            scraped = scrapowanie(nip)
            nazwa_i_adres_kontrahenta = "|".join(scraped) if scraped else ""

        # Numer konta
        nr_konta_kontrahenta = nr_konta_z_bazy(nip) or "00000000000000000000000000"
        nr_konta_kontrahenta = str(nr_konta_kontrahenta).replace(" ", "")
        nr_rozliczeniowy_banku_kontrahenta = nr_konta_kontrahenta[2:8] if len(nr_konta_kontrahenta) >= 8 else ""

        # Daty i kwoty
        data_platnosci = serializacja_dat(row['Data wpływu'])
        from decimal import Decimal
        kwota_brutto = str(int(Decimal(str(row['Brutto'])) * 100))
        kwota_netto  = str(int(Decimal(str(row['Netto']))  * 100))
        kwota_vat    = str(int(Decimal(str(row['VAT']))    * 100))

        # Info klient-bank (<= 19 znaków)
        informacja_klient_bank = f"REF: {row['Numer dokumentu']}"
        if len(informacja_klient_bank) > 19:
            informacja_klient_bank = informacja_klient_bank[:19]

        # Szczegóły płatności jako string
        szczegoly_platnosci = f"/NIP/{nip}|/IDP/{row['Numer dokumentu']}|{kwota_vat}|{int(Decimal(str(row['Brutto']))*100)}"

        zapis(
            data_platnosci,
            kwota_brutto,
            "10205561",
            "0",
            "18102055610000310200035501",
            nr_konta_kontrahenta,
            nazwa_i_adres_zleceniodawcy,
            nazwa_i_adres_kontrahenta,
            nr_rozliczeniowy_banku_kontrahenta,
            szczegoly_platnosci,
            "01",
            informacja_klient_bank,
        )

# usuwanie dziwnych znaków z adresów
def sanityzacja(text:str) -> str:
    if text is None:
        return ""
    bad = ':*;‘“!+?|#,'  # usuwamy też przecinek (CSV)
    cleaned = "".join(c for c in str(text) if c not in bad)
    return " ".join(cleaned.split())


def sanityzacja_nipu(nip):
    return 0

def zapis(
        data_platnosci1,
        kwota_platnosci1,
        nr_rozliczeniowy_zleceniodawcy1,
        tryb_realizacji_platnosci1,
        nr_rachunku_zleceniodawcy1,
        nr_rachunku_kontrahenta1,
        nazwa_i_adres_zleceniodawcy1,
        nazwa_i_adres_kontrahenta1,
        nr_rozliczeniowy_banku_kontrahenta1,
        szczegoly_platnosci,
        klasyfikacja_polecenia1,
        informacja_klient_bank1,
          ):

    with open("test.txt", "w",encoding="iso8859_2",newline="") as f:
        dane = ",".join([
            "210",
            data_platnosci1,
            kwota_platnosci1,
            nr_rozliczeniowy_zleceniodawcy1,
            tryb_realizacji_platnosci1,
            nr_rachunku_zleceniodawcy1,
            nr_rachunku_kontrahenta1,
            sanityzacja(nazwa_i_adres_zleceniodawcy1),
            sanityzacja(nazwa_i_adres_kontrahenta1),
            "0",
            nr_rozliczeniowy_banku_kontrahenta1,
            sanityzacja(szczegoly_platnosci),
            "","",  # puste pola
            klasyfikacja_polecenia1,
            sanityzacja(informacja_klient_bank1)
        ])
        f.write(dane)
oczyt_pliku("plik_testowy.xlsx")
