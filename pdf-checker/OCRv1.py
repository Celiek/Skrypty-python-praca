import io
import fitz
import pytesseract
from PIL import Image

#projekt zarzucony nie działa
# nie ogarniany dalej

pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'
doc = fitz.open("Faktura(2).pdf")

page = doc[0]  # pierwsza strona


# for page_num in range(len(doc)):
#     page = doc.load_page(page_num)
#     pix = page.get_pixmap(dpi=300)
#
#     img = Image.open(io.BytesIO(pix.tobytes("png")))
#     text = pytesseract.image_to_string(img,lang="pol")
#     print(text)

blocks = page.get_text("dict")["blocks"]
for block in blocks:
    for line in block.get("lines", []):
        for span in line.get("spans", []):
            print(" ")
            # print(f"{span['text']} -> x={span['bbox'][0]:.2f}, y={span['bbox'][1]:.2f}")

dostawca = []
odbiorca = []

for block in blocks:
    for line in block.get("lines", []):
        for span in line.get("spans", []):
            x = span["bbox"][0]
            text = span["text"]

            if x < 200:  # lewa strona
                dostawca.append(text)
            elif x > 300:  # prawa strona
                odbiorca.append(text)
print(dostawca)
print(odbiorca)