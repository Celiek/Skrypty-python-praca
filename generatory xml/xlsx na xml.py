import pandas as pd
from lxml import etree
from datetime import datetime
import re

# ======================================================
# KONFIGURACJA
# ======================================================
XLSX_PATH = r"REJESTRY ZAKUP SHUMEE BRAKI OPTIMA (2) (1).xlsx"
XML_OUT = "rejestr_zakupu.xml"
NS = "http://www.comarch.pl/cdn/optima/offline"

# ======================================================
# FUNKCJE
# ======================================================

def txt(v):
    return "" if v is None else str(v).strip()

def parse_date(s):
    if not isinstance(s, str):
        return ""
    s = s.strip()
    if not s:
        return ""
    for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except:
            pass
    return ""

def to_decimal(val):
    if val is None:
        return "0.00"
    val = str(val).replace(",", ".").replace(" ", "")
    try:
        return f"{float(val):.2f}"
    except:
        return "0.00"

def clean_nip(nip):
    if nip is None:
        return ""
    return re.sub(r"\D", "", str(nip))

# ======================================================
# WCZYTANIE XLSX
# ======================================================
df = pd.read_excel(XLSX_PATH, dtype=str).fillna("")
print(f"📘 Wczytano {len(df)} rekordów.")

# ======================================================
# TWORZENIE XML — ROOT
# ======================================================
root = etree.Element("ROOT", nsmap={None: NS})

# ======================================================
# SEKCJA KONTRAHENCI
# ======================================================
kon_all = etree.SubElement(root, "KONTRAHENCI")
etree.SubElement(kon_all, "WERSJA").text = "2.00"
etree.SubElement(kon_all, "BAZA_ZRD_ID").text = "KSIG."
etree.SubElement(kon_all, "BAZA_DOC_ID").text = "KSIG."

# Unikatowi kontrahenci
unikalni = df.drop_duplicates(subset=["NIP"])

for _, row in unikalni.iterrows():
    k = etree.SubElement(kon_all, "KONTRAHENT")

    nip = clean_nip(row.get("NIP", ""))
    nazwa = txt(row.get("Kontrahent", ""))

    etree.SubElement(k, "ID_ZRODLA").text = ""
    etree.SubElement(k, "AKRONIM").text = nazwa
    etree.SubElement(k, "OPIS").text = ""
    etree.SubElement(k, "CHRONIONY").text = ""
    etree.SubElement(k, "RODZAJ").text = "dostawca"
    etree.SubElement(k, "KATEGORIA").text = ""
    etree.SubElement(k, "FORMA_PLATNOSCI").text = "Przelew"
    etree.SubElement(k, "KONTOODB").text = "201-I"
    etree.SubElement(k, "KONTODOST").text = "201-I"
    etree.SubElement(k, "MAX_ZWLOKA").text = ""
    etree.SubElement(k, "UPUST").text = ""

    adresy = etree.SubElement(k, "ADRESY")
    adr = etree.SubElement(adresy, "ADRES")

    etree.SubElement(adr, "STATUS").text = "aktualny"
    etree.SubElement(adr, "NAZWA1").text = nazwa
    etree.SubElement(adr, "ULICA").text = ""
    etree.SubElement(adr, "KOD_POCZTOWY").text = ""
    etree.SubElement(adr, "MIASTO").text = ""
    etree.SubElement(adr, "NIP").text = nip
    etree.SubElement(adr, "REGON").text = ""
    etree.SubElement(adr, "TELEFON").text = ""
    etree.SubElement(adr, "FAX").text = ""
    etree.SubElement(adr, "URL").text = ""
    etree.SubElement(adr, "EMAIL").text = ""

# ======================================================
# SEKCJA WALUTY
# ======================================================
wal = etree.SubElement(root, "WALUTY")
etree.SubElement(wal, "WERSJA").text = "2.00"
etree.SubElement(wal, "BAZA_ZRD_ID").text = "KSIG."
etree.SubElement(wal, "BAZA_DOC_ID").text = "KSIG."

# ======================================================
# SEKCJA REJESTRY_SPRZEDAZY_VAT (pusta, wymagana)
# ======================================================
spr = etree.SubElement(root, "REJESTRY_SPRZEDAZY_VAT")
etree.SubElement(spr, "WERSJA").text = "2.00"
etree.SubElement(spr, "BAZA_ZRD_ID").text = "KSIG."
etree.SubElement(spr, "BAZA_DOC_ID").text = "KSIG."

# ======================================================
# SEKCJA REJESTRY_ZAKUPU_VAT
# ======================================================
zak_all = etree.SubElement(root, "REJESTRY_ZAKUPU_VAT")
etree.SubElement(zak_all, "WERSJA").text = "2.00"
etree.SubElement(zak_all, "BAZA_ZRD_ID").text = "KSIG."
etree.SubElement(zak_all, "BAZA_DOC_ID").text = "KSIG."

# ======================================================
# GENEROWANIE ZAKUPÓW
# ======================================================
for _, row in df.iterrows():
    z = etree.SubElement(zak_all, "REJESTR_ZAKUPU_VAT")

    # PODSTAWY
    etree.SubElement(z, "MODUL").text = "Rejestr Vat"
    etree.SubElement(z, "REJESTR").text = "ZAKUP_TEST"

    data_zak = parse_date(row.get("Data zakupu", ""))
    data_wpl = parse_date(row.get("Data wpływu", ""))
    data_wys = parse_date(row.get("Data wystawienia", ""))

    etree.SubElement(z, "DATA_ZAKUPU").text = data_zak
    etree.SubElement(z, "DATA_WPLYWU").text = data_wpl
    etree.SubElement(z, "DATA_WYSTAWIENIA").text = data_wys

    numer = txt(row.get("Numer dokumentu", ""))
    etree.SubElement(z, "NUMER").text = numer
    etree.SubElement(z, "ID_ZRODLA").text = numer

    # IDENTYFIKACJA
    etree.SubElement(z, "IDENTYFIKATOR_KSIEGOWY").text = txt(row.get("Id. księgowy", ""))
    etree.SubElement(z, "KOREKTA").text = "Nie"
    etree.SubElement(z, "KOREKTA_NUMER").text = ""
    etree.SubElement(z, "WEWNETRZNA").text = "Nie"
    etree.SubElement(z, "METODA_KASOWA").text = "Nie"
    etree.SubElement(z, "FORMA_ZAKUPU").text = txt(row.get("Rodzaj transakcji", ""))

    # PODMIOT
    kontr = txt(row.get("Kontrahent", ""))
    nip = clean_nip(row.get("NIP", ""))
    nip_kraj = txt(row.get("NIP_KRAJ", ""))

    etree.SubElement(z, "TYP_PODMIOTU").text = "KONTRAHENT"
    etree.SubElement(z, "PODMIOT").text = kontr
    etree.SubElement(z, "PODMIOT_ID").text = ""

    etree.SubElement(z, "NAZWA1").text = kontr
    etree.SubElement(z, "NAZWA2").text = ""
    etree.SubElement(z, "NAZWA3").text = ""

    etree.SubElement(z, "NIP_KRAJ").text = nip_kraj
    etree.SubElement(z, "NIP").text = nip

    etree.SubElement(z, "KRAJ").text = ""
    etree.SubElement(z, "WOJEWODZTWO").text = ""
    etree.SubElement(z, "POWIAT").text = ""
    etree.SubElement(z, "GMINA").text = ""
    etree.SubElement(z, "ULICA").text = ""
    etree.SubElement(z, "NR_DOMU").text = ""
    etree.SubElement(z, "NR_LOKALU").text = ""
    etree.SubElement(z, "MIASTO").text = ""
    etree.SubElement(z, "KOD_POCZTOWY").text = ""
    etree.SubElement(z, "POCZTA").text = ""

    etree.SubElement(z, "ROLNIK").text = "Nie"
    etree.SubElement(z, "TYP_PLATNIKA").text = "kontrahent"
    etree.SubElement(z, "PLATNIK").text = kontr
    etree.SubElement(z, "PLATNIK_ID").text = ""

    etree.SubElement(z, "OPIS").text = txt(row.get("Opis", ""))

    # KWOTY
    netto = to_decimal(row.get("Netto", "0"))
    vat = to_decimal(row.get("VAT", "0"))
    brutto = to_decimal(row.get("Brutto", "0"))
    waluta = txt(row.get("Waluta", "PLN"))

    etree.SubElement(z, "WALUTA").text = waluta
    etree.SubElement(z, "KURS_WALUTY").text = "NBP"
    etree.SubElement(z, "NOTOWANIE_WALUTY_ILE").text = "1"
    etree.SubElement(z, "NOTOWANIE_WALUTY_ZA_ILE").text = "1"

    # POZYCJE — zgodne z OPTIMĄ
    poz_block = etree.SubElement(z, "POZYCJE")
    poz = etree.SubElement(poz_block, "POZYCJA")

    # Stawka VAT
    st_vat = ""
    try:
        st_vat = str(round((float(vat.replace(",", ".")) / float(netto.replace(",", "."))) * 100))
    except:
        st_vat = ""

    etree.SubElement(poz, "STAWKA_VAT").text = st_vat
    etree.SubElement(poz, "STATUS_VAT").text = "opodatkowana"

    etree.SubElement(poz, "NETTO").text = netto
    etree.SubElement(poz, "NETTO_SYS").text = netto
    etree.SubElement(poz, "NETTO_SYS2").text = netto

    etree.SubElement(poz, "VAT").text = vat
    etree.SubElement(poz, "VAT_SYS").text = vat
    etree.SubElement(poz, "VAT_SYS2").text = vat

    etree.SubElement(poz, "ODLICZENIA_VAT").text = "Tak"
    etree.SubElement(poz, "RODZAJ_ZAKUPU").text = txt(row.get("Kategoria", ""))
    etree.SubElement(poz, "KOLUMNA_KPR").text = "Inne"
    etree.SubElement(poz, "OPIS_POZ").text = txt(row.get("Opis", ""))

    # PŁATNOŚCI – pełna wersja
    platnosci = etree.SubElement(z, "PLATNOSCI")
    plat = etree.SubElement(platnosci, "PLATNOSC")

    etree.SubElement(plat, "TERMIN_PLAT").text = data_zak
    etree.SubElement(plat, "FORMA_PLATNOSCI_PLAT").text = txt(row.get("Forma płatności", ""))
    etree.SubElement(plat, "WALUTA_DOK").text = waluta

    etree.SubElement(plat, "KWOTA_PLAT").text = brutto
    etree.SubElement(plat, "KWOTA_PLN_PLAT").text = brutto

    etree.SubElement(plat, "KIERUNEK").text = "rozchód"
    etree.SubElement(plat, "ID_ZRODLA_PLAT").text = ""
    etree.SubElement(plat, "PLATNOSC_TYP_PODMIOTU").text = "kontrahent"
    etree.SubElement(plat, "PLATNOSC_PODMIOT").text = kontr
    etree.SubElement(plat, "PLATNOSC_PODMIOT_RACHUNEK_NR").text = ""

    etree.SubElement(plat, "PLAT_SPLIT_PAYMENT").text = "Nie"
    etree.SubElement(plat, "PLAT_SPLIT_KWOTA_VAT").text = vat
    etree.SubElement(plat, "PLAT_SPLIT_NIP").text = nip
    etree.SubElement(plat, "PLAT_SPLIT_NR_DOKUMENTU").text = numer

    etree.SubElement(z, "DEKLARACJA_VAT7").text = txt(row.get("VAT-7", ""))
    etree.SubElement(z, "DEKLARACJA_VATUE").text = txt(row.get("VAT-UE", ""))
    etree.SubElement(z, "KWOTY_DODATKOWE").text = ""
    etree.SubElement(z, "KODY_JPK").text = txt(row.get("Kody JPK_V7", ""))
    etree.SubElement(z, "ATRYBUTY").text = ""

# ======================================================
# ZAPIS XML
# ======================================================
xml_bytes = etree.tostring(root, encoding="utf-8", xml_declaration=True, pretty_print=True)

with open(XML_OUT, "wb") as f:
    f.write(xml_bytes)

print(f"✔ XML został wygenerowany — {XML_OUT}")
