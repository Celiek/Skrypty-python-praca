import pdfplumber
import re
import json
import os
import threading
import queue
from concurrent.futures import ThreadPoolExecutor

# Kolejka do przekazywania danych do zapisu
dane_queue = queue.Queue()
lock = threading.Lock()
stop_signal = object()

# Zbiory do logowania
brak_nipu_log = []
brak_nipu_lock = threading.Lock()

# Wątek zapisujący dane do zbiorczy.json
def zapisuj_dane_z_kolejki():
    with open("zbiorczy.json", "a", encoding="utf-8") as f:
        while True:
            dane = dane_queue.get()
            if dane is stop_signal:
                break
            with lock:
                f.write(json.dumps(dane, ensure_ascii=False) + ",\n")
            dane_queue.task_done()

# Zapis konta bankowego do osobnego pliku
def log_konto_bankowe(nazwa_pliku, konto):
    with lock:
        with open("log_konta_bankowe.txt", "a", encoding="utf-8") as f:
            f.write(f"{nazwa_pliku}: {konto}\n")

# Przetwarzanie pojedynczego pliku PDF
def przetworz_pdf(plik_pdf):
    nazwa_pliku = os.path.basename(plik_pdf)

    try:
        with pdfplumber.open(plik_pdf) as pdf:
            texts = [page.extract_text() for page in pdf.pages if page.extract_text()]
        text = "\n".join(texts)
    except Exception as e:
        print(f"❌ Błąd podczas otwierania pliku {nazwa_pliku}: {e}")
        return

    text = " ".join(text.split())

    nip_patterns = [
        r"\b\d{10}\b", r"NIP[:\s]*\d{10}\b", r"NIP[:\s]*\d{3}[-\s]?\d{3}[-\s]?\d{2}[-\s]?\d{2}",
        r"NIP\s+\d{10}\b", r"NIP\s*(\d{10})", r"NIP\s+(PL)?\s*\d{10}\b", r"NIP[:\s]*PL?\s*\d{10}\b",
        r"(PL)?\s*\d{10}\b", r"\bPL\d{10}\b", r"\bPL\s+\d{10}\b", r"\bPL[\s\-]?\d{10}\b",
        r"\b\d{3}-\d{2}-\d{2}-\d{3}\b", r"\b\d{3}[-\s]?\d{2}[-\s]?\d{2}[-\s]?\d{3}\b",
        r"NIP[:\s]*\d{3}[-\s]?\d{2}[-\s]?\d{2}[-\s]?\d{3}"
    ]

    bank_patterns = [
        r"\b\d{26}\b", r"\bPL\d{26}\b",
        r"\bPL[\s\-]?\d{2}[\s\-]?\d{4}[\s\-]?\d{4}[\s\-]?\d{4}[\s\-]?\d{4}[\s\-]?\d{4}\b"
    ]

    def is_bank_account(candidate):
        return any(re.fullmatch(p, candidate) for p in bank_patterns)

    wykluczone_nipy = {"7252302342", "7252291331", "7252140827"}
    znalezione_nipy = set()
    nip = None

    for pattern in nip_patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        for match in matches:
            czysty_nip = re.sub(r"\D", "", match)
            if czysty_nip and czysty_nip not in wykluczone_nipy and not is_bank_account(czysty_nip):
                nip = czysty_nip
                break
            else:
                znalezione_nipy.add(czysty_nip)
        if nip:
            break

    if not nip:
        with brak_nipu_lock:
            brak_nipu_log.append(nazwa_pliku)
        if znalezione_nipy:
            print(f"⛔ Wykluczone lub błędne NIP-y w pliku: {nazwa_pliku} → {', '.join(znalezione_nipy)}")
        else:
            print(f"⚠ Brak NIP-u w pliku: {nazwa_pliku}")
        return

    konto_match = re.search(r"\b(?:\d[\s-]?){26}\b", text)
    konto = re.sub(r"\D", "", konto_match.group()) if konto_match else None
    if konto:
        log_konto_bankowe(nazwa_pliku, konto)

    dane = {}
    if nip:
        dane["nip"] = nip
    if konto:
        dane["nr_konta"] = konto

    dane_queue.put(dane)
    print(f"✔ Dodano dane z: {nazwa_pliku}")

# Główna logika
sciezka = r"C:\Users\DELL\Desktop"
pdf_files = []

for root, dirs, files in os.walk(sciezka):
    for file in files:
        if file.lower().endswith(".pdf"):
            pdf_files.append(os.path.join(root, file))

# Start wątku zapisującego
zapis_thread = threading.Thread(target=zapisuj_dane_z_kolejki)
zapis_thread.start()

# Przetwarzanie równoległe
with ThreadPoolExecutor(max_workers=4) as executor:
    executor.map(przetworz_pdf, pdf_files)

# Zakończenie zapisu
dane_queue.put(stop_signal)
zapis_thread.join()

# Zapis logu brakujących NIP-ów
if brak_nipu_log:
    with open("log_brak_nipu.txt", "w", encoding="utf-8") as f:
        for plik in brak_nipu_log:
            f.write(f"{plik}\n")
    print(f"📝 Zapisano log {len(brak_nipu_log)} plików bez NIP-u do log_brak_nipu.txt")
else:
    print("✅ Wszystkie pliki zawierały poprawne NIP-y.")