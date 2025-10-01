import os 
import re 
import fitz

# Działa to tak raczej średnio


# folder z którego pobierane są wyniki 
folder = r"C:\Users\DELL\Desktop\faktury 25.09.2025"

banki = {
    1010: "Narodowy Bank Polski",
    1020: "PKO BP",
    1030: "Bank Handlowy (Citi Handlowy)",
    1050: "ING Bank Śląski",
    1130: "BGK",
    1140: "mBank, Kompakt Finanse",
    1160: "Bank Millennium",
    1240: "Pekao SA",
    1280: "HSBC",
    1320: "Bank Pocztowy",
    1540: "BOŚ Bank",
    1580: "Mercedes-Benz Bank Polska",
    1610: "SGB - Bank",
    1670: "RBS Bank (Polska)",
    1680: "Plus Bank",
    1840: "Societe Generale",
    1870: "Nest Bank",
    1930: "Bank Polskiej Spółdzielczości",
    1940: "Credit Agricole Bank Polska",
    1950: "Idea Bank",
    2030: "BNP Paribas",
    2070: "FCE Bank Polska",
    2120: "Santander Consumer Bank",
    2130: "Volkswagen Bank",
    2140: "Fiat Bank Polska",
    2160: "Toyota Bank",
    2190: "DnB Nord",
    2480: "Getin Noble Bank",
    2490: "Alior Bank, T-Mobile Usługi Bankowe"
}


def fast_text_extractor(pdf_name):
    fitz.TOOLS.set_icc(False)
    try:
        with fitz.open(pdf_name) as doc:
            text = ''.join(page.get_text("text") for page in doc)
            text.replace(" ", "")
            text.replace("\n","")
    except Exception as e:
        return None, f"Błąd odczytu PDF: {e} | plik {pdf_name}"
    return text, None

wszystkie_pdfy = [
    os.path.join(root,f)
    for root, _, files in os.walk(folder)
    for f in files if f.lower().endswith(".pdf")
]

def przetworz_plik(pdf_path):
    tekst, blad = fast_text_extractor(pdf_path)
    if blad:
        return pdf_path, False, blad

    for identyfikator in banki.keys():
        if re.search(rf"\b{identyfikator}\b", tekst):
            return pdf_path, True, None  

    return pdf_path, False, None  

# Przetwarzanie wszystkich plików
pdfy_bez_identyfikatorow = []

for pdf in wszystkie_pdfy:
    sciezka, zawiera_id, blad = przetworz_plik(pdf)
    if blad:
        print(f"❌ {blad}")
    elif not zawiera_id:
        pdfy_bez_identyfikatorow.append(sciezka)

# Wynik
print("\n✅ PDFy bez identyfikatorów bankowych:")
for plik in pdfy_bez_identyfikatorow:
    print(plik)
