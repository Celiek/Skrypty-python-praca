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
    """Zamiana None -> '' + strip."""
    return "" if v is None else str(v).strip()

def parse_date(s):
    """dd.mm.yyyy / yyyy-mm-dd -> yyyy-mm-dd lub ''."""
    if not isinstance(s, str):
        return ""
    s = s.strip()
    if not s:
        return ""
    for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except Exception:
            pass
    return ""

def to_decimal(val):
    """Kwota -> 'xxx.xx'."""
    if val is None:
        return "0.00"
    val = str(val).replace(",", ".").replace(" ", "")
    try:
        return f"{float(val):.2f}"
    except Exception:
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

# Unikatowi kontrahenci po NIP
unikalni = df.drop_duplicates(subset=["NIP"])

for _, row in unikalni.iterrows():
    k = etree.SubElement(kon_all, "KONTRAHENT")

    nip = clean_nip(row.get("NIP", ""))
    nazwa = txt(row.get("Kontrahent", ""))
    nip_kraj = txt(row.get("NIP_KRAJ", "")) or "PL"

    # Główne pola kontrahenta
    etree.SubElement(k, "ID_ZRODLA").text = ""
    etree.SubElement(k, "AKRONIM").text = nazwa
    etree.SubElement(k, "RODZAJ").text = "dostawca"
    etree.SubElement(k, "FORMA_PLATNOSCI").text = "Przelew"

    # Brakujące według Optimy
    etree.SubElement(k, "EKSPORT").text = "Nie"
    etree.SubElement(k, "FINALNY").text = "Tak"
    etree.SubElement(k, "PLATNIK_VAT").text = "Tak"   # ew. "Nie" jeśli dostawca nieczynny
    etree.SubElement(k, "KRAJ_ISO").text = nip_kraj
    etree.SubElement(k, "WALUTA").text = "PLN"

    # ADRESY
    adresy = etree.SubElement(k, "ADRESY")
    adr = etree.SubElement(adresy, "ADRES")

    etree.SubElement(adr, "STATUS").text = "aktualny"
    etree.SubElement(adr, "NAZWA1").text = nazwa
    etree.SubElement(adr, "NAZWA2").text = ""
    etree.SubElement(adr, "NAZWA3").text = ""
    etree.SubElement(adr, "ULICA").text = ""
    etree.SubElement(adr, "KOD_POCZTOWY").text = ""
    etree.SubElement(adr, "MIASTO").text = ""
    etree.SubElement(adr, "KRAJ").text = ""
    etree.SubElement(adr, "NIP").text = nip
    etree.SubElement(adr, "NIP_KRAJ").text = nip_kraj
    etree.SubElement(adr, "TELEFON1").text = ""
    etree.SubElement(adr, "EMAIL").text = ""

    # KNT_RACHUNKI – struktura wymagana przez Optimę (puste rachunki)
    rach_all = etree.SubElement(k, "KNT_RACHUNKI")
    rach = etree.SubElement(rach_all, "KNT_RACHUNEK")
    etree.SubElement(rach, "LP").text = "1"
    etree.SubElement(rach, "DOMYSLNY").text = "Tak"
    etree.SubElement(rach, "RACHUNEK_NUMER").text = ""
    etree.SubElement(rach, "RACHUNEK_IBAN").text = ""
    etree.SubElement(rach, "SCHEM_BANK_NR").text = ""
    etree.SubElement(rach, "SCHEM_FORMA_PLATNOSCI").text = "Przelew"

# ======================================================
# SEKCJA KATEGORIE – pusta, ale wymagana
# ======================================================
kat = etree.SubElement(root, "KATEGORIE")
etree.SubElement(kat, "WERSJA").text = "2.00"
etree.SubElement(kat, "BAZA_ZRD_ID").text = "KSIG."
etree.SubElement(kat, "BAZA_DOC_ID").text = "KSIG."

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

    numer = txt(row.get("Numer dokumentu", ""))
    kontr = txt(row.get("Kontrahent", ""))
    nip = clean_nip(row.get("NIP", ""))
    nip_kraj = txt(row.get("NIP_KRAJ", "")) or "PL"

    data_zak = parse_date(row.get("Data zakupu", ""))
    data_wpl = parse_date(row.get("Data wpływu", ""))
    data_wys = parse_date(row.get("Data wystawienia", ""))

    netto = to_decimal(row.get("Netto", "0"))
    vat = to_decimal(row.get("VAT", "0"))
    brutto = to_decimal(row.get("Brutto", "0"))
    waluta = txt(row.get("Waluta", "PLN"))

    # === NAGŁÓWEK REJESTRU (zgodnie z Optimą) ===
    etree.SubElement(z, "ID_ZRODLA").text = numer
    etree.SubElement(z, "MODUL").text = "Rejestr Vat"
    etree.SubElement(z, "TYP").text = "zakup"
    etree.SubElement(z, "REJESTR").text = "ZAKUP_TEST"

    etree.SubElement(z, "DATA_ZAKUPU").text = data_zak
    etree.SubElement(z, "DATA_WPLYWU").text = data_wpl
    etree.SubElement(z, "DATA_WYSTAWIENIA").text = data_wys
    etree.SubElement(z, "TERMIN").text = data_zak
    etree.SubElement(z, "DATA_KURSU").text = data_zak
    etree.SubElement(z, "DATA_KURSU_2").text = data_zak
    etree.SubElement(z, "DATA_DATAOBOWIAZKUPODATKOWEGO").text = data_zak
    etree.SubElement(z, "DATA_DATAPRAWAODLICZENIA").text = data_zak

    etree.SubElement(z, "NUMER").text = numer

    etree.SubElement(z, "KOREKTA").text = "Nie"
    etree.SubElement(z, "KOREKTA_NUMER").text = ""
    etree.SubElement(z, "WEWNETRZNA").text = "Nie"
    etree.SubElement(z, "FISKALNA").text = "Nie"
    etree.SubElement(z, "DETALICZNA").text = "Nie"
    etree.SubElement(z, "EKSPORT").text = "Nie"
    etree.SubElement(z, "FINALNY").text = "Tak"
    etree.SubElement(z, "PODATNIK_CZYNNY").text = "Tak"
    etree.SubElement(z, "MPP").text = "Nie"

    # Podmiot
    etree.SubElement(z, "TYP_PODMIOTU").text = "kontrahent"
    etree.SubElement(z, "PODMIOT").text = kontr
    etree.SubElement(z, "PODMIOT_ID").text = ""
    etree.SubElement(z, "NIP").text = nip

    etree.SubElement(z, "TYP_PLATNIKA").text = "kontrahent"
    etree.SubElement(z, "PLATNIK").text = kontr
    etree.SubElement(z, "PLATNIK_ID").text = ""
    etree.SubElement(z, "PLATNIK_RACHUNEK_NR").text = ""

    # Forma płatności + deklaracje
    etree.SubElement(z, "FORMA_PLATNOSCI").text = txt(row.get("Forma płatności", ""))
    etree.SubElement(z, "DEKLARACJA_VAT7").text = txt(row.get("VAT-7", ""))
    etree.SubElement(z, "DEKLARACJA_VATUE").text = txt(row.get("VAT-UE", ""))
    etree.SubElement(z, "DEKLARACJA_VAT27").text = "Nie"

    # === POZYCJE ===
    poz_block = etree.SubElement(z, "POZYCJE")
    # Twoje wymagane metadane w POZYCJACH
    etree.SubElement(poz_block, "WERSJA").text = "2.00"
    etree.SubElement(poz_block, "BAZA_ZRD_ID").text = "KSIG."
    etree.SubElement(poz_block, "BAZA_DOC_ID").text = "KSIG."

    poz = etree.SubElement(poz_block, "POZYCJA")

    # Stawka VAT (procent)
    st_vat = ""
    try:
        netto_val = float(netto.replace(",", "."))
        vat_val = float(vat.replace(",", "."))
        if netto_val != 0:
            st_vat = str(round(vat_val / netto_val * 100))
    except Exception:
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

    # === PŁATNOŚCI ===
    platnosci = etree.SubElement(z, "PLATNOSCI")
    plat = etree.SubElement(platnosci, "PLATNOSC")

    etree.SubElement(plat, "TERMIN_PLAT").text = data_zak
    etree.SubElement(plat, "FORMA_PLATNOSCI_PLAT").text = txt(row.get("Forma płatności", ""))
    etree.SubElement(plat, "WALUTA_DOK").text = waluta
    etree.SubElement(plat, "DATA_KURSU_PLAT").text = data_zak

    etree.SubElement(plat, "KWOTA_PLAT").text = brutto
    etree.SubElement(plat, "KWOTA_PLN_PLAT").text = brutto

    etree.SubElement(plat, "KIERUNEK").text = "rozchód"
    etree.SubElement(plat, "ID_ZRODLA_PLAT").text = ""
    etree.SubElement(plat, "PLATNOSC_TYP_PODMIOTU").text = "kontrahent"
    etree.SubElement(plat, "PLATNOSC_PODMIOT").text = kontr
    etree.SubElement(plat, "PLATNOSC_PODMIOT_RACHUNEK_NR").text = ""

    # Elixir / split payment
    etree.SubElement(plat, "PLAT_ELIXIR_O1").text = ""  # np. f"Zapłata za {numer}"
    etree.SubElement(plat, "PLAT_SPLIT_PAYMENT").text = "Nie"
    etree.SubElement(plat, "PLAT_SPLIT_KWOTA_VAT").text = vat
    etree.SubElement(plat, "PLAT_SPLIT_NIP").text = nip
    etree.SubElement(plat, "PLAT_SPLIT_NR_DOKUMENTU").text = numer

    # === KODY_JPK (poprawiona struktura) ===
    kody_val = txt(row.get("Kody JPK_V7", ""))
    if kody_val:
        kody = etree.SubElement(z, "KODY_JPK")
        kod_jpk = etree.SubElement(kody, "KOD_JPK")
        etree.SubElement(kod_jpk, "KOD").text = kody_val

    # === ATRYBUTY (struktura jak w Optimie) ===
    atryb_root = etree.SubElement(z, "ATRYBUTY")
    atryb = etree.SubElement(atryb_root, "ATRYBUT")
    etree.SubElement(atryb, "KOD_ATR").text = ""
    etree.SubElement(atryb, "WARTOSC").text = ""

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

print(f"✔ XML został wygenerowany — {XML_OUT}")
