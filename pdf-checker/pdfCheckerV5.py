from datetime import date
import hashlib
import os
from pathlib import Path
import re
import shutil
import tkinter as tk
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from functools import partial
from tkinter import filedialog, messagebox, scrolledtext, ttk
import threading
import fitz
import json

# ================== KONFIGURACJA ==================

DOZWOLONE_NAZWY = ["EXTRASTORE", "GREATSTORE", "SHUMEE", "SUPER MERCHANT"]
DOZWOLONE_TEKSTY = [
    "faktura", "invoice", "korekta",
    "Faktura VAT KOREKTA", "Invoice FS",
    "Faktura eksportowa", "Faktura VAT korekta", "Faktura VAT"
]

# ================== STAN APLIKACJI ==================

brak_wzorca_files = []
duplikaty = []
liczba_duplikatow = 0
unikalne_teksty = {}
poprawne_bez_duplikatow = []

_local_cache = {}

# ================== PDF ==================

def fast_extract_text(pdf_path):
    if pdf_path in _local_cache:
        return _local_cache[pdf_path], None
    try:
        with fitz.open(pdf_path) as doc:
            text = "".join(page.get_text("text") for page in doc)
            _local_cache[pdf_path] = text
            return text, None
    except Exception as e:
        return None, str(e)

def hash_pdf_text_md5(pdf_path):
    text, err = fast_extract_text(pdf_path)
    if err or not text:
        return None
    return hashlib.md5(text.encode("utf-8")).hexdigest()

def hash_pdf_text_sha256(pdf_path):
    text, err = fast_extract_text(pdf_path)
    if err or not text:
        return None
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

# ================== JSON ==================

def load_hashes(json_path: str) -> set[str]:
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return {
        entry["hash"].lower()
        for entry in data
        if "hash" in entry
    }

# ================== PRZESZUKIWANIE ==================

def przetworz_plik(pdf_path, wzorce_re):
    text, err = fast_extract_text(pdf_path)
    if err:
        return pdf_path, None, err, None

    h = hashlib.md5(text.encode("utf-8")).hexdigest()

    for wz in wzorce_re:
        if wz.search(text):
            return pdf_path, wz.pattern.strip("\\b"), None, h

    return pdf_path, None, None, h

def przeszukaj_pdfy(folder, wzorce, ignore_case, gui_cb):
    global liczba_duplikatow
    liczba_duplikatow = 0

    brak_wzorca_files.clear()
    duplikaty.clear()
    unikalne_teksty.clear()
    poprawne_bez_duplikatow.clear()

    flags = re.IGNORECASE if ignore_case else 0
    wzorce_re = [re.compile(rf"\b{re.escape(w)}\b", flags) for w in wzorce]

    wszystkie_pdf = [
        os.path.join(r, f)
        for r, _, fs in os.walk(folder)
        for f in fs if f.lower().endswith(".pdf")
    ]

    buffer = []

    with ProcessPoolExecutor() as pool:
        for path, wz, err, h in pool.map(
            partial(przetworz_plik, wzorce_re=wzorce_re),
            wszystkie_pdf
        ):
            log = f"📄 {path}\n"

            if err:
                buffer.append((log + f"⚠️ {err}\n", None))
                continue

            if h in unikalne_teksty:
                duplikaty.append(path)
                liczba_duplikatow += 1
                buffer.append((log + "♻️ Duplikat treści\n", "duplikat"))
                continue

            unikalne_teksty[h] = path

            if wz:
                poprawne_bez_duplikatow.append(path)
                buffer.append((log + f"✅ {wz}\n", None))
            else:
                brak_wzorca_files.append(path)
                buffer.append((log + "❌ Brak wzorca\n", None))

    gui_cb(buffer, len(wszystkie_pdf))

# ================== GUI CALLBACK ==================

def start_przeszukiwanie():
    folder = folder_var.get()
    if not os.path.isdir(folder):
        messagebox.showerror("Błąd", "Niepoprawny folder")
        return

    output_text.delete(1.0, tk.END)
    duplikat_output_text.delete(1.0, tk.END)
    listbox.delete(0, tk.END)
    duplikat_listbox.delete(0, tk.END)

    progressbar.start()
    btn_przeszukaj.config(state="disabled")

    def gui_cb(buffer, count):
        for log, tag in buffer:
            if tag:
                duplikat_output_text.insert(tk.END, log, tag)
            else:
                output_text.insert(tk.END, log)

        for f in brak_wzorca_files:
            listbox.insert(tk.END, f)
        for f in duplikaty:
            duplikat_listbox.insert(tk.END, f)

        progressbar.stop()
        btn_przeszukaj.config(state="normal")
        output_text.insert(
            tk.END,
            f"\n📦 Pliki: {count}\n♻️ Duplikaty: {liczba_duplikatow}\n",
            "info"
        )

    threading.Thread(
        target=przeszukaj_pdfy,
        args=(folder, DOZWOLONE_TEKSTY, czy_ignore_case.get(), gui_cb),
        daemon=True
    ).start()

# ================== KOPIOWANIE ==================

def kopiuj_poprawne_pliki():
    if not poprawne_bez_duplikatow and not brak_wzorca_files:
        messagebox.showwarning("Brak danych", "Brak plików do kopiowania")
        return

    folder_docelowy = filedialog.askdirectory(title="Folder wynikowy")
    if not folder_docelowy:
        return

    root_folder = folder_var.get()
    dozwolone_re = [re.compile(rf"\b{re.escape(w)}\b", re.IGNORECASE) for w in DOZWOLONE_NAZWY]

    tasks = []

    for plik in poprawne_bez_duplikatow + brak_wzorca_files:
        rel = os.path.relpath(plik, root_folder)
        sub = os.path.dirname(rel)

        text, _ = fast_extract_text(plik)
        found = [w for w in DOZWOLONE_NAZWY if re.search(rf"\b{w}\b", text, re.I)]
        folder = found[0] if found else "INNE"

        dst = os.path.join(folder_docelowy, folder, sub, os.path.basename(plik))
        tasks.append((plik, dst))

    def copy_one(src_dst):
        src, dst = src_dst
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.copy(src, dst)

    with ThreadPoolExecutor(max_workers=8) as pool:
        pool.map(copy_one, tasks)

    # ===== JSON → NOWE PLIKI NA PULPICIE =====
    json_path = filedialog.askopenfilename(
        title="Plik JSON (hashy archiwum)",
        filetypes=[("JSON", "*.json")]
    )

    if json_path:
        known = load_hashes(json_path)
        today = date.today().isoformat()
        desktop = Path.home() / "Desktop"
        out = desktop / f"SHUMEE_bez_duplikatow_{today}"
        out.mkdir(exist_ok=True)

        copied = 0
        for root, _, files in os.walk(folder_docelowy):
            for f in files:
                if not f.lower().endswith(".pdf"):
                    continue
                src = os.path.join(root, f)
                h = hash_pdf_text_sha256(src)
                if h and h not in known:
                    shutil.copy(src, out / f)
                    copied += 1

        messagebox.showinfo(
            "Gotowe",
            f"Nowe pliki: {copied}\n📂 {out}"
        )

# ================== GUI ==================

def main():
    global root, folder_var, czy_ignore_case
    global output_text, duplikat_output_text
    global listbox, duplikat_listbox
    global progressbar, btn_przeszukaj

    root = tk.Tk()
    root.title("PDF Checker V5")
    root.geometry("1200x720")

    folder_var = tk.StringVar()
    czy_ignore_case = tk.BooleanVar(value=True)

    top = tk.Frame(root)
    top.pack(pady=10)

    tk.Label(top, text="Folder z pdfami").grid(row=0,column=0,padx=5)
    tk.Entry(top, textvariable=folder_var, width=60).grid(row=0, column=1, padx=5)
    tk.Button(top, text="Wybierz Folder", command=lambda: folder_var.set(filedialog.askdirectory())).grid(row=0, column=2,padx=5)
    tk.Checkbutton(top, text="Ignoruj wielkość liter", variable=czy_ignore_case).grid(row=1, column=0,sticky="w")

    btn_przeszukaj = tk.Button(root, text="🔍 Przeszukaj pdfy", command=start_przeszukiwanie, bg="#4CAF50", fg="white")
    btn_przeszukaj.pack(pady=5)

    tk.Button(root, text="💾 Skopiuj do folderu", command=kopiuj_poprawne_pliki,bg="#2196F3", fg="white").pack(pady=5)

    progressbar = ttk.Progressbar(root, mode="indeterminate",length=300)
    progressbar.pack(pady=5)

    frame = tk.Frame(root)
    frame.pack()

    listbox = tk.Listbox(frame, width=80, height=8)
    listbox.pack(side="left")

    duplikat_listbox = tk.Listbox(frame, width=80, height=8)
    duplikat_listbox.pack(side="left")

    logs = tk.Frame(root)
    logs.pack()

    output_text = scrolledtext.ScrolledText(logs, width=70, height=20)
    output_text.pack(side="left")

    duplikat_output_text = scrolledtext.ScrolledText(logs, width=70, height=20)
    duplikat_output_text.pack(side="left")

    root.mainloop()

if __name__ == "__main__":
    import multiprocessing
    multiprocessing.freeze_support()
    main()
