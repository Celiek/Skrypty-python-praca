from lxml import etree

# zmiana daty na (9) pl DONE
# zmiana daty na (10) bez nipu
parser = etree.XMLParser(strip_cdata=False)
tree = etree.parse("report_20250801_20250831 (10)_v2.xml", parser)
root = tree.getroot()

# Namespace
ns = {"ns": "http://www.comarch.pl/cdn/optima/offline"}

cdata_tags = set()
for elem in root.iter():
    if isinstance(elem.text, etree.CDATA):
        cdata_tags.add(elem)

for sprzedaz in root.findall(".//ns:DATA_SPRZEDAZY", namespaces=ns):
    wartosc = sprzedaz.text.strip() if sprzedaz.text else ""
    parent = sprzedaz.getparent()
    wystawienia = parent.findall("ns:DATA_WYSTAWIENIA", namespaces=ns)

    for w in wystawienia:
        w.text = wartosc  
        cdata_tags.add(w) 

for elem in cdata_tags:
    if elem.text:
        elem.text = etree.CDATA(elem.text)

tree.write("report_20250801_20250831 (10)_v3.xml", encoding="utf-8", xml_declaration=True, pretty_print=True)