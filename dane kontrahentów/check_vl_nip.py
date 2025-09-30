import os
import requests
from datetime import datetime, timedelta, date
import zipfile
import hashlib
import json
import shutil

# ====================
# Zmienne do programu
# ====================

data = datetime.today().strftime("%Y%m%d")
link_plik_płaski = "https://plikplaski.mf.gov.pl/pliki//" +data + ".7z"
print(f"link do pliku płaskiego {link_plik_płaski}")

path_to_zip_file = "/"


def get_file(url):
    local_filename = url.split('/')[-1]
    with requests.get(url,stream = True) as r:
        with open(local_filename, 'wb') as f:
            shutil.copyfileobj(r.raw,f)
    return local_filename

# funkcja skończona, działa bez zarzutu
def Sha512Hash1(nip,nr_konta,data):
    # "schemat": "RRRRMMDDNNNNNNNNNNBBBBBBBBBBBBBBBBBBBBBBBBBB lub 
    # RRRRMMDDNNNNNNNNNN, gdzie R to cyfra roku, M – miesiąca, 
    # D - dnia daty generowania pliku, N to cyfra NIPu, a B to cyfra rachunku bankowego"
    if data is None:
        data = date.today().strftime("%Y%m%d")

    to_hash = str(data) + nip + nr_konta
    h = hashlib.sha512(to_hash.encode("utf-8")).hexdigest()
    for _ in range(4999):
        h = hashlib.sha512(h.encode("utf-8")).hexdigest()
    return h

def unzip():
    path = str(data) +  ".json"
    with zipfile.ZipFile(path,'r') as zip_ref:
        zip_ref.extractall("/")
        print("rozpakowano plik zip")

#wyszukuje hash w pliku płaskim: 
def find_hash_in_json(json_file: str, hash_value: str) -> bool:
    with open(json_file, "r", encoding="utf-8") as f:
        text = f.read()  # surowy tekst
    return hash_value in text

def main():
    print("pobieram plik .json")
    get_file(link_plik_płaski)

    print("rozpakowuje plik płaski")
    unzip()

    print(find_hash_in_json("20250930.json",Sha512Hash1("7252140827","71114011080000314718001019",data=data)))