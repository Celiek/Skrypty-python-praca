import json
from lxml import etree

INPUT_XML = r"C:\Users\DELL\Documents\Skrypty\Skrypty-python-praca\generatory xml\xsara-tfoja_stara.xml"
OUTPUT_XML = "wyjebane_duplikaty.xml"
DUP_JSON = "duplikaty.json"

def suma_kwot(faktura):
    """Zlicza sumę NETTO i VAT z pozycji faktury."""
    netto_sum = 0.0
    vat_sum = 0.0

    for poz in faktura.xpath(".//POZYCJA"):
        netto = poz.findtext("NETTO", "0").replace(",", ".")
        vat = poz.findtext("VAT", "0").replace(",", ".")
        try:
            netto_sum += float(netto)
            vat_sum += float(vat)
        except ValueError:
            pass

    return round(netto_sum, 2), round(vat_sum, 2)


def usun_duplikaty():
    tree = etree.parse(INPUT_XML)
    root = tree.getroot()

    seen = {}
    duplicates = []

    faktury = root.findall(".//REJESTR_SPRZEDAZY_VAT")

    for fakt in faktury:
        id_zrodla = fakt.findtext("ID_ZRODLA", "").strip()

        netto_sum, vat_sum = suma_kwot(fakt)
        key = (id_zrodla, netto_sum, vat_sum)

        # Pierwsze wystąpienie — zapisujemy
        if key not in seen:
            seen[key] = fakt
        else:
            # TO JEST DUPLIKAT
            parent = fakt.getparent()
            parent.remove(fakt)

            duplicates.append({
                "ID_ZRODLA": id_zrodla,
                "NETTO_SUM": netto_sum,
                "VAT_SUM": vat_sum
            })

    # Zapis czystego XML
    tree.write(
        OUTPUT_XML,
        encoding="utf-8",
        pretty_print=True,
        xml_declaration=True
    )

    # Zapis JSON z duplikatami
    with open(DUP_JSON, "w", encoding="utf-8") as f:
        json.dump(duplicates, f, indent=4, ensure_ascii=False)

    print(f"✔ Usunięto {len(duplicates)} duplikatów")
    print(f"✔ Zapisano czysty XML → {OUTPUT_XML}")
    print(f"✔ Zapisano listę duplikatów → {DUP_JSON}")


if __name__ == "__main__":
    usun_duplikaty()
