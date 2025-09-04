import hashlib
import os
import re
import threading
import tkinter as tk
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from tkinter import filedialog, messagebox, scrolledtext, ttk

import fitz

wzorce = ["EXTRASTORE", "GREATSTORE", "SHUMEE", "SUPER MERCHANT"]
brak_wzorca_files = []
duplikaty = []
unikalne_teksty = {}
poprawne_bez_duplikatow = []
znalezione_wzorce_w_folderach = {}


def fast_extract_text(pdf_name):
    fitz.TOOLS.set_icc(False)
    try:
        with fitz.open(pdf_name) as doc:
            return ''.join(page.get_text("text") for page in doc), None
    except Exception as e:
        return None, f"Błąd odczytu PDF: {e}"


def przetworz_plik(pdf_path, wzorzec, ignore_case):
    tekst, blad = fast_extract_text(pdf_path)
    if blad:
        return pdf_path, None, blad, None
    text_hash = hashlib.md5(tekst.encode("utf-8")).hexdigest()
    flags = re.DOTALL | re.IGNORECASE if ignore_case else re.DOTALL
    dopasowanie = re.search(wzorzec, tekst, flags=flags)
    return pdf_path, dopasowanie.group() if dopasowanie else None, None, text_hash


def przeszukaj_pdfy(folder, wzorzec, ignore_case, gui_callback):
    brak_wzorca_files.clear()
    duplikaty.clear()
    unikalne_teksty.clear()
    poprawne_bez_duplikatow.clear()
    buffer = []
    duplikaty_log = []

    wszystkie_pdf = [
        os.path.join(root, f)
        for root, _, files in os.walk(folder)
        for f in files if f.lower().endswith(".pdf")
    ]

    with ThreadPoolExecutor() as executor:
        func = partial(przetworz_plik, wzorzec=wzorzec, ignore_case=ignore_case)
        results = executor.map(func, wszystkie_pdf)

        for pdf_path, wynik, blad, tekst_hash in results:
            log = f"📄 Przetwarzanie pliku: {pdf_path}\n"
            if blad:
                log += f"⚠️ {blad}\n"
                buffer.append((log, None))
                continue
            if tekst_hash in unikalne_teksty:
                duplikaty.append(pdf_path)
                log += f"⚠️ Duplikat treści z: {unikalne_teksty[tekst_hash]}\n"
                duplikaty_log.append(log)
                continue
            unikalne_teksty[tekst_hash] = pdf_path
            tekst, _ = fast_extract_text(pdf_path)
            if wynik:
                log += f"✅ Znaleziony fragment:\n{wynik}\n"
                poprawne_bez_duplikatow.append(pdf_path)
                inne = [w for w in wzorce if w.lower() != wynik.lower() and re.search(w, tekst, re.IGNORECASE)]
                if inne:
                    log += f"🔍 Inne znalezione wzorce: {', '.join(inne)}\n"
                    buffer.append((log, "inne_wzorce"))
                else:
                    buffer.append((log, None))
            else:
                znalezione = [w for w in wzorce if re.search(w, tekst, re.IGNORECASE)]
                if znalezione:
                    log += f"❌ Brak wzorca {wybrany_wzorzec.get()} – znaleziony wzorzec: {znalezione[0]}\n"
                else:
                    log += f"❌ Brak wzorca {wybrany_wzorzec.get()}\n"
                brak_wzorca_files.append(pdf_path)
                buffer.append((log, "brak_wzorca"))

    gui_callback(buffer, duplikaty_log, len(wszystkie_pdf))


def start_przeszukiwanie():
    folder = folder_var.get()
    if not os.path.isdir(folder):
        messagebox.showerror("Błąd", "Wybierz poprawny folder.")
        return
    output_text.delete(1.0, tk.END)
    duplikat_output_text.delete(1.0, tk.END)
    progressbar.start()
    btn_przeszukaj.config(state="disabled")

    tekst = wybrany_wzorzec.get().strip()
    wzorzec = rf"\b{tekst}\b"
    ignore_case = czy_ignore_case.get()

    def gui_callback(buffer, duplikaty_log, liczba_plikow):
        for log, tag in buffer:
            if tag == "brak_wzorca" and "znaleziony wzorzec:" in log:
                cz1, cz2 = log.split("– znaleziony wzorzec:")
                output_text.insert(tk.END, cz1 + "– ", "brak_wzorca")
                output_text.insert(tk.END, "znaleziony wzorzec:" + cz2.strip() + "\n", "inne_wzorce")
            elif tag:
                output_text.insert(tk.END, log + "\n", tag)
            else:
                output_text.insert(tk.END, log + "\n")
        for log in duplikaty_log:
            duplikat_output_text.insert(tk.END, log + "\n", "duplikat")
        progressbar.stop()
        btn_przeszukaj.config(state="normal")
        output_text.insert(tk.END, f"📦 Lącznie przeszukano plików: {liczba_plikow}\n", "info")

    threading.Thread(target=przeszukaj_pdfy, args=(folder, wzorzec, ignore_case, gui_callback)).start()


# GUI setup
root = tk.Tk()
root.title("PDF Checker")
root.geometry("1200x720")

folder_var = tk.StringVar()
wybrany_wzorzec = tk.StringVar(value=wzorce[0])
czy_ignore_case = tk.BooleanVar(value=True)

frame = tk.Frame(root)
frame.pack(pady=10)

# Inputs
tk.Label(frame, text="Folder z PDF-ami:").grid(row=0, column=0, padx=5)
tk.Entry(frame, textvariable=folder_var, width=60).grid(row=0, column=1, padx=5)
tk.Button(frame, text="Wybierz folder", command=lambda: folder_var.set(filedialog.askdirectory())).grid(row=0, column=2, padx=5)

tk.Label(frame, text="Wzorzec do wyszukania:").grid(row=1, column=0, padx=5)
tk.OptionMenu(frame, wybrany_wzorzec, *wzorce).grid(row=1, column=1, sticky="w")
tk.Checkbutton(frame, text="Ignoruj wielkość liter", variable=czy_ignore_case).grid(row=2, column=1, sticky="w")

btn_przeszukaj = tk.Button(root, text="🔍 Przeszukaj PDF-y", command=start_przeszukiwanie, bg="#4CAF50", fg="white")
btn_przeszukaj.pack(pady=5)

progressbar = ttk.Progressbar(root, mode="indeterminate", length=300)
progressbar.pack(pady=5)

# Labels for outputs
label_frame = tk.Frame(root)
label_frame.pack()
tk.Label(label_frame, text="📋 Główne logi:").pack(side="left", padx=60)
tk.Label(label_frame, text="♻️ Duplikaty:").pack(side="left", padx=60)

# Output logs
output_frame = tk.Frame(root)
output_frame.pack(padx=10, pady=10)

output_text = scrolledtext.ScrolledText(output_frame, wrap=tk.WORD, width=70, height=20)
output_text.pack(side="left", padx=5)

duplikat_output_text = scrolledtext.ScrolledText(output_frame, wrap=tk.WORD, width=70, height=20)
duplikat_output_text.pack(side="left", padx=5)

# Text formatting
output_text.tag_config("brak_wzorca", foreground="red")
output_text.tag_config("duplikat", foreground="orange")
output_text.tag_config("info", foreground="blue", font=("Arial", 10, "bold"))
output_text.tag_config("inne_wzorce", foreground="purple")
duplikat_output_text.tag_config("duplikat", foreground="orange")

root.mainloop()
