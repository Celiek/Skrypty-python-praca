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
    if not s:
        return ""
    s = re.sub(r"<!\[CDATA\[|\]\]>", "", str(s))
    s = s.strip().replace("\u00A0", "")
    return re.sub(r"\s+", "", s)


def ensure_text(elem, value: str):
    if elem is not None:
        elem.text = value


def find_or_create(parent, tag_name: str, ns=NS):
    el = parent.find(f"c:{tag_name}", ns)
    if el is None:
        el = ET.SubElement(parent, f"{{{NS['c']}}}{tag_name}")
    return el


# ========================================
# 1) Wczytanie excela
# ========================================
df = pd.read_excel(XLSX_PATH)

print("\n=== PODGLĄD PLIKU EXCEL ===")
print(df.head(10))              # podgląd pierwszych wierszy
print("Kolumny:", df.columns.tolist())  # nazwy kolumn

df[KOL_NETTO] = pd.to_numeric(df[KOL_NETTO], errors="coerce").fillna(0)
df[KOL_VAT]   = pd.to_numeric(df[KOL_VAT],   errors="coerce").fillna(0)

mapa_kwot = {
    norm(row[KOLUMNA_ID]): (float(row[KOL_NETTO]), float(row[KOL_VAT]))
    for _, row in df.iterrows()
}

print(f"📘 Wczytano {len(mapa_kwot)} rekordów z Excela.")


# ========================================
# 2) Wczytanie XML
# ========================================
tree = ET.parse(XML_IN)
root = tree.getroot()

print("\n=== PIERWSZE NUMERY W XML ===")
for i, rej in enumerate(root.findall(".//c:REJESTR_SPRZEDAZY_VAT", NS)[:20]):
    num_xml = rej.find("c:NUMER", NS)
    if num_xml is not None:
        print(i, repr(norm(num_xml.text)))

aktualizacje = 0
brak = 0


# ========================================
# 3) Aktualizacja kwot + płatności
# ========================================
for rej in root.findall(".//c:REJESTR_SPRZEDAZY_VAT", NS):

    # numer dokumentu
    num_elem = rej.find("c:NUMER", NS)
    if num_elem is None or not num_elem.text:
        continue
    numer = norm(num_elem.text)

    # waluta dokumentu
    waluta_elem = rej.find("c:WALUTA", NS)
    waluta_dok = waluta_elem.text.strip() if waluta_elem is not None and waluta_elem.text else "PLN"
    ensure_text(waluta_elem, waluta_dok)

    # przygotowanie płatności
    plat = rej.find("c:PLATNOSCI/c:PLATNOSC", NS)
    if plat is not None:
        ensure_text(find_or_create(plat, "WALUTA_PLAT"), waluta_dok)
        ensure_text(find_or_create(plat, "WALUTA_DOK"), waluta_dok)

    # kwoty z excela
    if numer not in mapa_kwot:
        brak += 1
        continue

    netto_excel, vat_excel = mapa_kwot[numer]
    brutto_excel = netto_excel + vat_excel

    # ===== ROZKŁAD POZYCJI =====
    pozycje = rej.findall(".//c:POZYCJE/c:POZYCJA", NS)

    if len(pozycje) > 0:
        # 1) Pierwsza pozycja => pełne wartości z Excela
        poz1 = pozycje[0]

        el_netto_1 = find_or_create(poz1, "NETTO")
        el_vat_1   = find_or_create(poz1, "VAT")

        el_netto_1.text = f"{netto_excel:.2f}".replace(".", ",")
        el_vat_1.text   = f"{vat_excel:.2f}".replace(".", ",")

        # NETTO_SYS = NETTO × kurs
        kurs_elem = rej.find("c:NOTOWANIE_WALUTY_ILE", NS)
        if kurs_elem is not None and kurs_elem.text:
            rate = float(kurs_elem.text.replace(",", "."))
        else:
            rate = 1.0  # fallback

        netto_sys = netto_excel * rate
        vat_sys = vat_excel * rate

        # ustaw sys-value
        ensure_text(find_or_create(poz1, "NETTO_SYS"),  f"{netto_sys:.2f}".replace(".", ","))
        ensure_text(find_or_create(poz1, "VAT_SYS"),    f"{vat_sys:.2f}".replace(".", ","))
        ensure_text(find_or_create(poz1, "NETTO_SYS2"), f"{netto_sys:.2f}".replace(".", ","))
        ensure_text(find_or_create(poz1, "VAT_SYS2"),   f"{vat_sys:.2f}".replace(".", ","))

        # 2) Pozostałe pozycje wyzerować
        for poz in pozycje[1:]:
            for tag in ["NETTO", "VAT", "NETTO_SYS", "VAT_SYS", "NETTO_SYS2", "VAT_SYS2"]:
                ensure_text(find_or_create(poz, tag), "0,00")

    # ===== PŁATNOŚCI =====
    if plat is not None:
        kw_plat = find_or_create(plat, "KWOTA_PLAT")
        kw_plat.text = f"{brutto_excel:.2f}".replace(".", ",")

        kw_pln = find_or_create(plat, "KWOTA_PLN_PLAT")
        kw_pln.text = f"{(brutto_excel * rate):.2f}".replace(".", ",")

    aktualizacje += 1


print(f"✅ Zaktualizowano: {aktualizacje}")
print(f"⚠️ Brak dopasowania: {brak}")


# ========================================
# 4) Usuwanie duplikatów po ID_ZRODLA
# ========================================
print("\n🧹 Usuwanie duplikatów po ID_ZRODLA...")

parent = root.find(".//c:REJESTRY_SPRZEDAZY_VAT", NS)
if parent is not None:
    seen = set()
    for rej in list(parent.findall("c:REJESTR_SPRZEDAZY_VAT", NS)):
        id_el = rej.find("c:ID_ZRODLA", NS)
        if not id_el or not id_el.text:
            continue
        key = norm(id_el.text)

        if key in seen:
            parent.remove(rej)
        else:
            seen.add(key)

# ========================================
# 5) Zapis bez ns0:
# ========================================
ET.register_namespace("", "http://www.comarch.pl/cdn/optima/offline")
tree.write(XML_OUT, encoding="utf-8", xml_declaration=True)

print(f"\n📁 Wynik zapisano do: {XML_OUT}")
