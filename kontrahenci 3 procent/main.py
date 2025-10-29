import os, logging
import pandas as pd
from argparse import ArgumentParser
from fakturownia_api import get_faktur, dodaj_faktury, API_KEY
from reports import export_grouped_excels
from db_ops import zapisz_faktury_do_bazy, zapisz_powiazania_do_bazy

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
API_KEY = os.getenv("API_KEY", API_KEY)

if __name__ == "__main__":
    parser = ArgumentParser(description="Automatyzacja faktur 3% / 2%")
    parser.add_argument("input", help="Plik XLSX z fakturami cząstkowymi")
    parser.add_argument("-c", "--company", required=True)
    args = parser.parse_args()

    df = pd.read_excel(args.input)
    xlsx_map = export_grouped_excels(df)
    wyniki = dodaj_faktury(args.company, [], 1732019)
    zapisz_faktury_do_bazy(df, args.company)
    zapisz_powiazania_do_bazy(df, wyniki, args.company)
    get_faktur(date_from="2025-10-29", date_to="2025-10-29")
