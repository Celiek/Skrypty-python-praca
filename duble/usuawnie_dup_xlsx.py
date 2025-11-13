import xml.etree.ElementTree as ET
import pandas as pd
import re

# ========================================
# KONFIGURACJA
# ========================================
XML_IN = r"C:\Users\DELL\Downloads\GREAT TEMU FV ZAGRANICA.xml"
XML_OUT = r"GREAT TEMU FV ZAGRANICA.xml"
XLSX_PATH = r"C:\Users\DELL\Downloads\greatstore temu fv duplikaty usuniete .xlsx"

KOLUMNA_ID = "Dokument"
KOL_NETTO = "Netto"
KOL_VAT = "Kwota VAT"

NS = {"c": "http://www.comarch.pl/cdn/optima/offline"}

# ========================================
# FUNKCJE POMOCNICZE
# ========================================
def norm(s: str | None) -> str:
    """Normalizuje string, usuwa whitespace, CDATA i różne kreski"""
    if not s:
        return ""
    s = re.sub(r"<!\[CDATA\[|\]\]>", "", str(s))
    s = s.strip()
    s = s.replace("\u00A0", "")
    s = s.replace("–", "-").replace("—", "-").replace("−", "-")
    s = re.sub(r"\s+", "", s)
    return s


def ensure_text(elem, value: str):
    """Ustawia tekst w elemencie, nawet jeśli był pusty lub None."""
    if elem is not None:
        elem.text = value
    return


def find_or_create(parent, tag_name: str, ns=NS):
    """Znajduje element lub tworzy nowy gdy go nie ma."""
    el = parent.find(f"c:{tag_name}", ns)
    if el is None:
        el = ET.SubElement(parent, f"{{{NS['c']}}}{tag_name}")
    return el


# ========================================
# 1) Wczytanie excela
# ========================================
df = pd.read_excel(XLSX_PATH)
df[KOL_NETTO] = pd.to_numeric(df[KOL_NETTO], errors="coerce").fillna(0)
df[KOL_VAT]   = pd.to_numeric(df[KOL_VAT],   errors="coerce").fillna(0)

mapa_kwot = {
    norm(row[KOLUMNA_ID]): (row[KOL_NETTO], row[KOL_VAT])
    for _, row in df.iterrows()
}

print(f"📘 Wczytano {len(mapa_kwot)} rekordów z Excela.")


# ========================================
# 2) Wczytanie XML
# ========================================
tree = ET.parse(XML_IN)
root = tree.getroot()

aktualizacje = 0
brak = 0


# ========================================
# 3) Aktualizacja kwot NETTO/VAT + poprawa walut
# ========================================
for rej in root.findall(".//c:REJESTR_SPRZEDAZY_VAT", NS):

    # numer dokumentu
    num_elem = rej.find("c:NUMER", NS)
    if num_elem is None or not num_elem.text:
        continue

    numer = norm(num_elem.text)

    # pobierz walutę dokumentu
    waluta_elem = rej.find("c:WALUTA", NS)
    waluta_dok = waluta_elem.text.strip() if waluta_elem is not None and waluta_elem.text else "PLN"
    waluta_dok = waluta_dok if waluta_dok != "" else "PLN"

    # ustaw walutę dokumentu (gdy pusta)
    ensure_text(waluta_elem, waluta_dok)

    # płatność
    platnosc = rej.find("c:PLATNOSCI/c:PLATNOSC", NS)
    if platnosc is not None:
        # WALUTA_PLAT
        w_plat = platnosc.find("c:WALUTA_PLAT", NS)
        w_plat = find_or_create(platnosc, "WALUTA_PLAT")
        ensure_text(w_plat, waluta_dok)

        # WALUTA_DOK
        w_dok = platnosc.find("c:WALUTA_DOK", NS)
        w_dok = find_or_create(platnosc, "WALUTA_DOK")
        ensure_text(w_dok, waluta_dok)

    # aktualizacja kwot z excela
    if numer in mapa_kwot:
        netto_val, vat_val = mapa_kwot[numer]

        for poz in rej.findall(".//c:POZYCJE/c:POZYCJA", NS):
            el_netto = poz.find("c:NETTO", NS)
            el_vat   = poz.find("c:VAT",   NS)

            if el_netto is not None:
                el_netto.text = f"{netto_val:.2f}".replace(".", ",")
            if el_vat is not None:
                el_vat.text = f"{vat_val:.2f}".replace(".", ",")

        aktualizacje += 1
    else:
        brak += 1


print(f"✅ Zaktualizowano: {aktualizacje}")
print(f"⚠️ Brak dopasowania: {brak}")


# ========================================
# 4) Usuwanie duplikatów po ID_ZRODLA
# ========================================
print("\n🧹 Usuwanie duplikatów po ID_ZRODLA...")

rejestry_parent = root.find(".//c:REJESTRY_SPRZEDAZY_VAT", NS)
if rejestry_parent is None:
    print("❌ Nie znaleziono REJESTRY_SPRZEDAZY_VAT")
else:
    seen_ids = set()
    to_remove = []

    for rej in rejestry_parent.findall("c:REJESTR_SPRZEDAZY_VAT", NS):
        id_elem = rej.find("c:ID_ZRODLA", NS)
        if id_elem is None or not id_elem.text:
            continue

        id_text = norm(id_elem.text)

        if id_text in seen_ids:
            to_remove.append(rej)
        else:
            seen_ids.add(id_text)

    for rej in to_remove:
        rejestry_parent.remove(rej)

    print(f"🗑️ Usunięto: {len(to_remove)}")
    print(f"📊 Pozostało: {len(seen_ids)} unikalnych")


# ========================================
# 5) Zapis bez ns0:
# ========================================
ET.register_namespace("", "http://www.comarch.pl/cdn/optima/offline")
tree.write(XML_OUT, encoding="utf-8", xml_declaration=True)

print(f"\n📁 Wynik zapisano do: {XML_OUT}")
