import pandas as pd
from lxml import etree
from datetime import datetime
import re

# ======================================================
# KONFIGURACJA
# ======================================================
XLSX_PATH = r"C:\Users\DELL\Downloads\maximus shumeeexcell.xlsx"
XML_OUT = "rejestr_zakupu.xml"
NS = "http://www.comarch.pl/cdn/optima/offline"

# ======================================================
# FUNKCJE POMOCNICZE
# ======================================================

def txt(v):
    return "" if v is None else str(v).strip()

def parse_date(s):
    if not isinstance(s, str):
        return ""
    s = s.strip()
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except Exception:
            pass
    return ""

def clean_nip(n):
    if n is None:
        return ""
    return re.sub(r"\D", "", str(n))

def clean_decimal(v):
    if v is None:
        return "0.00"
    return str(v).replace(",", ".").strip()

# ======================================================
# WCZYTANIE XLSX
# ======================================================
df = pd.read_excel(XLSX_PATH, dtype=str).fillna("")
print(f"📘 Wczytano {len(df)} wierszy.")
print("Kolumny:", df.columns.tolist())

# ======================================================
# ROOT XML
# ======================================================
root = etree.Element("ROOT", nsmap={None: NS})

# ======================================================
# KONTRAHENCI
# ======================================================
kon_all = etree.SubElement(root, "KONTRAHENCI")
etree.SubElement(kon_all, "WERSJA").text = "2.00"
etree.SubElement(kon_all, "BAZA_ZRD_ID").text = "KSIG."
etree.SubElement(kon_all, "BAZA_DOC_ID").text = "KSIG."

unikalni = df.drop_duplicates(subset=["NIP"])

for _, row in unikalni.iterrows():
    nazwa = txt(row.get("NAZWA KONTRAHENTA", ""))
    nip = clean_nip(row.get("NIP", ""))

    k = etree.SubElement(kon_all, "KONTRAHENT")
    etree.SubElement(k, "AKRONIM").text = nazwa
    etree.SubElement(k, "RODZAJ").text = "dostawca"

    adresy = etree.SubElement(k, "ADRESY")
    adr = etree.SubElement(adresy, "ADRES")

    etree.SubElement(adr, "STATUS").text = "aktualny"
    etree.SubElement(adr, "NAZWA1").text = nazwa
    etree.SubElement(adr, "ULICA").text = ""
    etree.SubElement(adr, "KOD_POCZTOWY").text = ""
    etree.SubElement(adr, "MIASTO").text = ""
    etree.SubElement(adr, "NIP").text = nip

    etree.SubElement(k, "WALUTA").text = "PLN"

# ======================================================
# PUSTE REJESTRY SPRZEDAŻY
# ======================================================
rs = etree.SubElement(root, "REJESTRY_SPRZEDAZY_VAT")
etree.SubElement(rs, "WERSJA").text = "2.00"
etree.SubElement(rs, "BAZA_ZRD_ID").text = "KSIG."
etree.SubElement(rs, "BAZA_DOC_ID").text = "KSIG."

# ======================================================
# REJESTRY ZAKUPU VAT
# ======================================================
rz = etree.SubElement(root, "REJESTRY_ZAKUPU_VAT")
etree.SubElement(rz, "WERSJA").text = "2.00"
etree.SubElement(rz, "BAZA_ZRD_ID").text = "KSIG."
etree.SubElement(rz, "BAZA_DOC_ID").text = "KSIG."

# GRUPOWANIE FAKTUR
grupy = df.groupby("Pełny numer")
print(f"🔍 Znaleziono {len(grupy)} faktur.")

# ======================================================
# GENEROWANIE XML FAKTUR
# ======================================================
for numer_faktury, group in grupy:

    if not txt(numer_faktury):
        continue

    row = group.iloc[0]

    fakt = etree.SubElement(rz, "REJESTR_ZAKUPU_VAT")

    etree.SubElement(fakt, "ID_ZRODLA").text = numer_faktury
    etree.SubElement(fakt, "MODUL").text = "Rejestr Vat"
    etree.SubElement(fakt, "TYP").text = "Rejestr zakupu"
    etree.SubElement(fakt, "REJESTR").text = "ZAKUP_TEST"

    data_wys = parse_date(row.get("Data wystawienia", ""))

    etree.SubElement(fakt, "DATA_WYSTAWIENIA").text = data_wys
    etree.SubElement(fakt, "DATA_ZAKUPU").text = data_wys
    etree.SubElement(fakt, "DATA_WPLYWU").text = data_wys
    etree.SubElement(fakt, "NUMER").text = numer_faktury

    # KONTRAHENT
    kontr = txt(row.get("NAZWA KONTRAHENTA", ""))
    nip = clean_nip(row.get("NIP", ""))

    etree.SubElement(fakt, "PODMIOT").text = kontr
    etree.SubElement(fakt, "NIP").text = nip
    etree.SubElement(fakt, "KRAJ").text = ""
    etree.SubElement(fakt, "ULICA").text = ""
    etree.SubElement(fakt, "MIASTO").text = ""
    etree.SubElement(fakt, "KOD_POCZTOWY").text = ""
    etree.SubElement(fakt, "FINALNY").text = "Nie"

    # POZYCJE
    PZ = etree.SubElement(fakt, "POZYCJE")

    for lp, (_, poz_row) in enumerate(group.iterrows(), start=1):
        netto = clean_decimal(poz_row.get("Netto", "0"))
        vat_rate = clean_decimal(poz_row.get("VAT", poz_row.get("...VAT", "0")))
        vat_value = clean_decimal(poz_row.get("Kwota VAT", "0"))
        brutto = clean_decimal(poz_row.get("Brutto", "0"))

        poz = etree.SubElement(PZ, "POZYCJA")
        etree.SubElement(poz, "LP").text = str(lp)
        etree.SubElement(poz, "STAWKA_VAT").text = vat_rate
        etree.SubElement(poz, "STATUS_VAT").text = "opodatkowana"
        etree.SubElement(poz, "NETTO").text = netto
        etree.SubElement(poz, "VAT").text = vat_value
        etree.SubElement(poz, "BRUTTO").text = brutto
        etree.SubElement(poz, "NETTO_SYS").text = netto
        etree.SubElement(poz, "VAT_SYS").text = vat_value
        etree.SubElement(poz, "NETTO_SYS2").text = netto
        etree.SubElement(poz, "VAT_SYS2").text = vat_value
        etree.SubElement(poz, "ODLICZENIA_VAT").text = "Tak"
        etree.SubElement(poz, "RODZAJ_ZAKUPU").text = "inne"

    # ==================================================
    # POPRAWIONE PŁATNOŚCI
    # ==================================================
    # Pobieramy netto z pierwszego wiersza (jak prosiłeś)
    kwota_plat = clean_decimal(row.get("Netto", "0"))

    PL = etree.SubElement(fakt, "PLATNOSCI")
    p = etree.SubElement(PL, "PLATNOSC")
    etree.SubElement(p, "TERMIN_PLAT").text = data_wys
    etree.SubElement(p, "FORMA_PLATNOSCI_PLAT").text = "PRZELEW"
    etree.SubElement(p, "WALUTA_DOK").text = "PLN"
    etree.SubElement(p, "KWOTA_PLAT").text = kwota_plat
    etree.SubElement(p, "KWOTA_PLN_PLAT").text = kwota_plat
    etree.SubElement(p, "KIERUNEK").text = "rozchód"

    etree.SubElement(fakt, "ATRYBUTY")

# ======================================================
# ZAPIS XML
# ======================================================
xml_bytes = etree.tostring(
    root,
    encoding="utf-8",
    xml_declaration=True,
    pretty_print=True
)

with open(XML_OUT, "wb") as f:
    f.write(xml_bytes)

print(f"✔ XML zapisany jako: {XML_OUT}")
