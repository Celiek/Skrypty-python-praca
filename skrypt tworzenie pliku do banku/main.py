import requests
from datetime import datetime
from datetime import date
import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time
import random


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

def losowe_opoznienie(min_sec = 0.05,max_sec = 0.05):
    czas = random.uniform(min_sec, max_sec)
    time.sleep(czas)

def scrapowanie(nip):
    service = Service(r"C:\tools\chromedriver-win64\chromedriver.exe")
    driver = webdriver.Chrome(service=service)
    driver.get("https://example.com")

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920x1080")

    driver.get("https://wyszukiwarkaregon.stat.gov.pl/appBIR/index.aspx")
    losowe_opoznienie()
    nip_input = driver.find_element(By.ID, "txtNip")
    nip_input.clear()
    nip_input.send_keys(nip)

    szukaj_button = driver.find_element(By.ID,"btnSzukaj")
    szukaj_button.click()

    losowe_opoznienie()
    rows = driver.find_elements(By.CLASS_NAME, "tabelaZbiorczaListaJednostekAltRow")
    dane = []
    if not rows:
        print("Brak wyników dla podanego NIP:",nip)
    else:
        print(f"Wyniki dla NIP {nip}:")
        for row in rows:
            cells = row.find_elements(By.TAG_NAME, "td")
            dane = [cell.text.strip() for cell in cells]
            print(" ",dane)
    driver.quit()
    return dane[5:9][::-1]


# nr identyfikacyjne banku do Debugowania
banki = {1010:'NBP',
         1022:'PKOBP',
         1030:'Citi Handlowy',
         1050:'ING Bank Śląski',
         1090:'Santander Bank Polska',
         1140:'mBank',
         1160:'Millenium',
         1240:'Pekao Sa',
         1870:'Nest Bank',
         1940:'Credit Agricole',
         2030:'BNP Paribas',
         2120:'Santander Consumer Bank',
         2480:'VeloBank',
         2490:'Alior Bank',
         2790:'Raiffeisen Bank',
         2901:"Aion Bank"}

# 110 zwykły przelew
# 210 polecenie zapłaty / Zapłaty Split
# 410 polecenie przelewu zagranicznego
# statyczne pole
polecenie_zaplaty = '210'

# format RRRRMMDD
# z pliku :
data_platnosci = '20250817'
# kwota platnosci w groszach
kwota_platnosci = '2500000'
# kod przelewu to 8 cyfr z nr konta bankowego = kod banku + 4 następne cyfry
nr_rozliczeniowy_banku_kontrahenta = '10205561'
# zlecenie split static
tryb_realizacji_platnosci = '210'
# nasz nr rachunku
nr_rachunku_zleceniodawcy ='84102055610000380200040857'
# nr rachunku kontrachenta
nr_rachunku_kontrahenta ='18102055610000310200035501'
# rozdzielane przecinkiem
nazwa_i_adres_zleceniodawcy='\"Super Merchant, Super Merchant S.A, al. 1 maja 31/33, 90-739 Łódź\"'
# rozdzielane przecinkiem
nazwa_i_adres_kontrahenta='\"ADITECH Sp. z.o.o,  ADITECH Sp. z.o.o, ul. Cicha 17, 37-514 Tuczempy Przemyśl \"'

# związane z poleceniem zapłaty
oplaty_i_prowizje = "0"
# 8 cyfr po nr identyfikacyjnym
nr_rozliczeniowy_banku_kontrahenta = "10205561"
pole_13=""
pole_14 =""
# dla przelewu split wartość to 01
klasyfikacja_polecenia = "01"
informacja_klient_bank = "faktura XX/XX/XX"

#NIP|NR faktury| Kwota Vat| Kwota Brutto
szczegoly_platnosci = "1234567890| Faktura VAT 4974/2025|230|1230"

# zamiana formatu daty z DDMMYYYY na YYYYMMDD
def serializacja_dat(data):
    sformatowana_data = datetime.strptime(data,"%d/%m/%Y").strftime('%Y%m%d')
    return sformatowana_data

def nr_konta_z_bazy(nip):
    load_dotenv()

    db_config = {
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT"),
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD")
    }

    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("SELECT nr_konta from merchant where nip = {nip}",(nip,))
        data = cursor.fetchall()
        cursor.close()
        conn.close()

        if data:
            return data
        else:
            return "error brak danych dla tego nr nip w bazie danych"
    except psycopg2.Error as e:
        return f"błąd bazy danych: {e.pgerror or str(e)}"
    except Exception as e:
        return f"Nieoczekiwany błąd : {e}"

# sprawdzanie czy podany nip ma przyporządkowany adres jeśli nie to go dodaje
def sprawdz_czy_istnieje_adres(nip,adres):
    load_dotenv()

    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")

    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

    cursor = conn.cursor()

    cursor.execute("SELECT adres FROM merchant WHERE nip = %s", (nip,))
    wynik = cursor.fetchone()

    if wynik is None:
        print(f"Brak rekordu z NIP-em {nip} — możesz dodać nowy wpis, jeśli chcesz.")
        # Można tu dodać INSERT, jeśli chcesz tworzyć nowe rekordy
    elif wynik[0] is None or wynik[0].strip() == "":
        cursor.execute("UPDATE merchant SET adres = %s WHERE nip = %s", (adres, nip))
        conn.commit()
        print(f"Zaktualizowano adres dla NIP-u {nip}")
    else:
        print(f"Adres dla NIP-u {nip} już istnieje — pomijam aktualizację.")

    cursor.close()
    conn.close()

# odczyt pliku xlsx i zapus do pliku .txt
def oczyt_pliku(input_file):
    df = pd.read_excel(input_file)
    for index,row in df.iterrows():
        print(f"{row['Numer dokumentu']}, {row['Kontrahent']}")

        # dane firmy ( w doprecyzowaniu 3 spółki)


        # sprawdzanie czy istnieje podany nip w bazie gusu i naszej
        nip = row['NIP']
        nazwa_i_adres_kontrahenta = "|".join(scrapowanie(nip))
        sprawdz_czy_istnieje_adres(nip,nazwa_i_adres_kontrahenta)

        kwota_brutto = float(row['Brutto']) * 100

        nr_konta_kontrahenta = nr_konta_z_bazy(nip)
        nr_rozliczeniowy_banku_kontrahenta = nr_konta_kontrahenta[2:8]
        informacja_klient_bank = "REF: " + row['Numer dokumentu']
        sczegoly_kontrahenta = nip + '|' + row['Numer dokumentu'] + '|' + row['Vat'] + '|' + row['Brutto']

        zapis(row['Data wpływu'],kwota_brutto,"84102055610000380200040857",
              "210",nr_rozliczeniowy_banku_kontrahenta,
              nazwa_i_adres_kontrahenta,nr_konta_kontrahenta,nr_rozliczeniowy_banku_kontrahenta,
              "01",informacja_klient_bank)
oczyt_pliku("plik_testowy.xlsx")


# usuwanie dziwnych znaków z adresów
def sanityzacja(text):
    return ''.join(c for c in text if c not in ':*;‘“!+?|#')

def zapis(
        data_platnosci1,
        kwota_platnosci1,
        nr_rachunku_zleceniodawcy1,
        tryb_realizacji_platnosci1,
        nr_rachunku_kontrahenta1,
        nazwa_i_adres_zleceniodawcy1,
        nazwa_i_adres_kontrahenta1,
        nr_rozliczeniowy_banku_kontrahenta1,
        klasyfikacja_polecenia1,
        informacja_klient_bank1,
          ):

    with open("test.txt", "w",encoding="iso8859_2",newline="") as f:
        dane = ",".join([
            polecenie_zaplaty,
            data_platnosci1,
            kwota_platnosci1,
            nr_rachunku_zleceniodawcy1,
            tryb_realizacji_platnosci1,
            nr_rachunku_zleceniodawcy1,
            nr_rachunku_kontrahenta1,
            sanityzacja(nazwa_i_adres_zleceniodawcy1),
            sanityzacja(nazwa_i_adres_kontrahenta1),
            oplaty_i_prowizje,
            nr_rozliczeniowy_banku_kontrahenta1,
            sanityzacja(szczegoly_platnosci),
            "","",  # puste pola
            klasyfikacja_polecenia1,
            sanityzacja(informacja_klient_bank1)
        ])
        f.write(dane)
zapis()

