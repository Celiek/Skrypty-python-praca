from lxml import etree

# Parser z zachowaniem CDATA
parser = etree.XMLParser(strip_cdata=False)
tree = etree.parse("dokumenty_SHUMEE_2025_9.xml", parser)
root = tree.getroot()

count = 0

# Iteruj po wszystkich elementach
for elem in root.iter():
    # Sprawdź czy tag kończy się na 'TYP'
    if elem.tag.endswith("TYP") and elem.text and elem.text.strip() == "Rejestr zakupu":
        print(f"Zmieniam: {elem.text!r} → 'ZAKUP_TEST'")
        elem.text = "ZAKUP_TEST"
        count += 1

print(f"Łącznie zmieniono {count} tagów <TYP>.")

# Zapisz wynik
tree.write("test_typ.xml", encoding="windows-1250", xml_declaration=True, pretty_print=True)
