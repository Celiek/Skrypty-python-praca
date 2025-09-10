import pandas as pd
from argparse import ArgumentParser, BooleanOptionalAction
from typing import Optional
from dotenv import load_dotenv
import smtplib,ssl
import os
import re
from datetime import datetime,timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


# Program odczytuje dane z pliku xlsx i wysyłą dane do fakturowni
# potem pobiera dane z fakturowni (może)
# wysyła emaile z fakturami do listy kontrahentów z plików

####
# Konfiguracja i pomniejsze narzędzia
####

API_KEY = "K88WQTGPSBiuGdLgwHrc"


OUTPUT_DIR = os.getenv("OUTPUT_DIR",".")
os.makedirs(OUTPUT_DIR,exist_ok=True)

COMPANIES = {
    "shumee": {
        "name_addr": os.getenv("SHUMEE_NAME_ADDR", 'Shumee Sp. z.o.o. aleja 1 Maja 31/33 lok. 6 90-739 Łódź Nr konta: 07 1140 1108 0000 3147 1800 1007 NIP:7252140827'),
        "nrb":       os.getenv("SHUMEE_NRB",       "07114011080000314718001007"),
        "bank_code": os.getenv("SHUMEE_BANK_CODE", "11401108"),
    },
    "greatstore": {
        "name_addr": os.getenv("GREATSTORE_NAME_ADDR", 'Greatstore Sp. z.o.o. aleja 1 Maja 31/33 lok. 6 90-739 NR konta: 35 1140 1108 0000 3639 6100 1006 Łódź NIP:7252291331 '),
        "nrb":       os.getenv("GREATSTORE_NRB",       "18102055610000310200035501"),
        "bank_code": os.getenv("GREATSTORE_BANK_CODE", "10205561"),
    },
    "extrastore": {
        "name_addr": os.getenv("EXTRASTORE_NAME_ADDR", 'Extrastore Sp. z.o.o. aleja 1 Maja 31/33 lok. 6 90-739 Łódź NIP 7252302342 Nr konta: 05 1140 2004 0000 3302 8042 9939'),
        "nrb":       os.getenv("EXTRASTORE_NRB",       "05114020040000330280429939"),
        "bank_code": os.getenv("EXTRASTORE_BANK_CODE", "11402004"),  # 8 cyfr
    },
}


def get_email_db(email:str):
    return 0

def nip_digits(nip: str) -> str:
    cleaned = re.sub(r"\D", "", str(nip or ""))
    if len(cleaned) != 10:
        print(f"[WARN] NIP ma nieprawidłową długość: {nip} → {cleaned}")
    return cleaned


# Główna część logiki

# Wysyłanie emaili
def send_email(sender_email: str, file):
    port = 587
    smtp_server = "smtp.gmail.com"
    sender_email = os.getenv("SENDER_EMAIL")
    receiver_email = "z dataframea"
    password = os.getenv("PASSWORD")

    message = MIMEMultipart("alternative")
    message["Subject"] = "multipart test"
    message["From"] = sender_email
    message["To"] = receiver_email

    text = """\
        Treść testowego emaila
    """

    html = """\
    <html> 
        <body> 
            <p>
                Testowa treść pliku
            </p>
        </body>
    </html>
    """

    part1 =MIMEText(text, "plain")
    part2 = MIMEText(html,"html")

def czytaj_plik(
        file:str,
        *,
        spolka: str,
        key: str,
):
    # klucz = dane spółki
    # konfiguracja danych spółki do generowania emaili

    conf= COMPANIES[key]
    nazwa_i_adres_zleceniodawcy = conf["name_addr"]
    nr_rozliczeniowy_zleceniodawcy = conf["bank_code"]
    company_rachyunek = conf["nrb"]

    klucz = spolka.lower()
    if klucz not in COMPANIES:
        raise ValueError(f"Nieznana firma {spolka} popraw to")

    df = pd.read_excel(file)

    wymagane_kolumny ={"Data wystawienia","Netto","VAT","Brutto","Kontrahent","Numer dokumentu","NIP"}
    # odczyszczanie danych z plików
    df["NIP"] = df["NIP"].apply(nip_digits)
    suma_stawki = df.groupby("NIP")[["Netto","VAT","Brutto"]].sum().reset_index()

    print(suma_stawki)



if __name__ == "__main__":
    parser = ArgumentParser(description="Automatyczne generowanie faktur do kontrahentów 3% za poprzedni miesiąc")
    parser.add_argument("input", help="Ścieżka do xlsx z danymi do faktur")
    parser.add_argument("-c", "--company", required=True, choices=sorted(COMPANIES.keys()),
                        help=f"Firma (nadawca): {', '.join(sorted(COMPANIES.keys()))}")

    args = parser.parse_args()

    czytaj_plik(
        file=args.input,
        spolka=args.company,
        key=args.company,
    )

