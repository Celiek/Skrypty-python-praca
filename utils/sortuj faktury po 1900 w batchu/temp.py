import fitz

def czytaj():
    doc = fitz.open(r"C:/Users/DELL/Documents/FAKTURY/great_posortowane_26_11_2025/GREATSTORE/_sklep@ubierzsie.com/2025-11-21_14-11-2025-SM.pdf.pdf")
    text = ""
    for page in doc:
        text+=page.get_text("text")
    doc.close()

    print(text)

czytaj()