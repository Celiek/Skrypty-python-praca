import os
import requests
from datetime import datetime, date
import hashlib
import json
import shutil
import py7zr
import psycopg2
from dotenv import load_dotenv
from typing import List, Dict
from psycopg2.extras import RealDictCursor
import re

# ====================
# Zmienne do programu
# ====================

data = datetime.today().strftime("%Y%m%d")
link_plik_płaski = "https://plikplaski.mf.gov.pl/pliki//" + data + ".7z"
path_to_zip_file = "/"

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

# ====================
# Funkcje pomocnicze
# ====================

def clean_nip(nip: str) -> str:
    """Zwraca NIP jako 10 cyfr"""
    return re.sub(r"\D", "", str(nip)).zfill(10)

def clean_konto(konto: str) -> str:
    """Zwraca nr konta jako 26 cyfr"""
    return re.sub(r"\D", "", str(konto)).zfill(26)

def get_file(url: str) -> str:
    """Pobiera plik płaski z serwera MF"""
    local_filename = url.split('/')[-1]
    with requests.get(url, stream=True) as r:
        with open(local_filename, 'wb') as f:
            shutil.copyfileobj(r.raw, f)
    return local_filename

def unzip():
    output_dir = ''
    path = str(data) + ".7z"
    plik = str(data) + ".json"
    if os.path.isfile(plik):
        print("[I] Plik jest już pobrany")
        return
    with py7zr.SevenZipFile(path, mode='r') as archive:
        archive.extractall(output_dir)
    print("Rozpakowano plik 7z")

def Sha512Hash1(nip: str, nr_konta: str, data: str, iters: int = 5000) -> str:
    to_hash = str(data) + nip + nr_konta
    h = hashlib.sha512(to_hash.encode("utf-8")).hexdigest()
    for _ in range(iters - 1):
        h = hashlib.sha512(h.encode("utf-8")).hexdigest()
    print(h)
    return h

def Sha512HashNIP(nip: str, data: str, iters: int = 5000) -> str:
    nip = clean_nip(nip)
    to_hash = data + nip
    h = hashlib.sha512(to_hash.encode("utf-8")).hexdigest()
    for _ in range(iters - 1):
        h = hashlib.sha512(h.encode("utf-8")).hexdigest()
    print(h)
    return h


def db_conn():
    return psycopg2.connect(**DB_CONFIG)

def data_from_db() -> Dict[str, str]:
    """Pobranie NIP + NRB z bazy (od razu wyczyszczone)"""
    query = """
    SELECT nip, nr_konta FROM merchanci WHERE nip IS NOT NULL AND nr_konta IS NOT NULL;
    """
    result = {}
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            for row in cur.fetchall():
                nip_str = clean_nip(row["nip"])
                konto_str = clean_konto(row["nr_konta"])
                result[nip_str] = konto_str
    return result

def group_maski_by_bank(maski: List[str]) -> Dict[str, List[str]]:
    grouped = {}
    for m in maski:
        bank_code = m[2:10]
        grouped.setdefault(bank_code, []).append(m)
    return grouped

def apply_mask(nr_konta: str, maska: str) -> str:
    result = []
    i = 0
    for m in maska:
        if m == 'X':
            result.append('X')
            i += 1
        elif m == 'Y':
            result.append(nr_konta[i])
            i += 1
        else:
            result.append(m)
            i += 1
    return "".join(result)

def load_flatfile(json_file: str):
    """Wczytuje plik płaski do pamięci"""
    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    naglowek = data.get("naglowek", {})

    # poprawne pole w pliku
    gen_date = (
        naglowek.get("dataGenerowaniaPliku")
        or naglowek.get("dataGenerowaniaDanych")
        or naglowek.get("data")
        or datetime.today().strftime("%Y%m%d")
    )
    gen_date = gen_date.replace("-", "")

    iters = int(naglowek.get("liczbaTransformacji", 5000))

    czynni = set(data.get("skrotyPodatnikowCzynnych", []))
    zwolnieni = set(data.get("skrotyPodatnikowZwolnionych", []))
    maski_map = group_maski_by_bank(data.get("maski", []))

    return gen_date, iters, czynni, zwolnieni, maski_map

# ====================
# Główna logika
# ====================

def sprawdz_kontrahentow(json_file: str):
    baza_danych = data_from_db()
    gen_date, iters, czynni, zwolnieni, maski_map = load_flatfile(json_file)

    znalezione = []

    for nip, konto in baza_danych.items():
        nip_clean = clean_nip(nip)
        konto_clean = clean_konto(konto)

        # 1) pełny NRB
        hash_value = Sha512Hash1(nip_clean, konto_clean, data=gen_date, iters=iters)
        if hash_value in czynni or hash_value in zwolnieni:
            znalezione.append((nip_clean, konto_clean))
            print(f"[✔] Znaleziono pełne: NIP={nip_clean}, Konto={konto_clean}")
            continue

        # 2) maski
        bank_code = konto_clean[2:10]
        znaleziono = False
        for maska in maski_map.get(bank_code, []):
            masked_account = apply_mask(konto_clean, maska)
            hash_value = Sha512Hash1(nip_clean, masked_account, data=gen_date, iters=iters)
            if hash_value in czynni or hash_value in zwolnieni:
                znalezione.append((nip_clean, konto_clean))
                print(f"[✔] Znaleziono z maską {maska}: NIP={nip_clean}, Konto={konto_clean}")
                znaleziono = True
                break

        if znaleziono:
            continue

        # 3) fallback: tylko NIP
        hash_value = Sha512HashNIP(nip_clean, data=gen_date, iters=iters)
        if hash_value in czynni or hash_value in zwolnieni:
            print(f"[✔] Znaleziono po samym NIP: {nip_clean} (bez konta)")
        else:
            print(f"[✘] Brak: NIP={nip_clean}, Konto={konto_clean}")

    print(f"\n[✓] Łącznie znalezionych: {len(znalezione)}")
    return znalezione

def main():
    # get_file(link_plik_płaski)
    # unzip()
    json_file = str(data) + ".json"
    sprawdz_kontrahentow(json_file)

if __name__ == "__main__":
    main()
