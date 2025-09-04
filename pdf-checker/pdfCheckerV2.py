import hashlib
import os
import platform
import re
import shutil
import subprocess
import tkinter as tk
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from tkinter import filedialog, messagebox, scrolledtext, ttk

import fitz

# zmienne do wyłapywania duplikatów
wzorce = ["EXTRASTORE", "GREATSTORE", "SHUMEE","SUPER MERCHANT"]
brak_wzorca_files = []
duplikaty = []
unikalne_teksty = {}
poprawne_bez_duplikatow = []

def fast_extract_text(pdf_name):
    fitz.TOOLS.set_icc(False)
    try:
        with fitz.open(pdf_name) as doc:
            text = ''.join(page.get_text("text") for page in doc)
    except Exception as e:
        return None, f"Błąd odczytu PDF: {e} | plik: {pdf_name}"
    return text, None

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
            elif tekst_hash in unikalne_teksty:
                duplikaty.append(pdf_path)
                log += f"⚠️ Duplikat treści z: {unikalne_teksty[tekst_hash]}\n"
            else:
                unikalne_teksty[tekst_hash] = pdf_path
                if wynik:
                    log += f"✅ Znaleziony fragment:\n{wynik}\n"
                    poprawne_bez_duplikatow.append(pdf_path)
                else:
                    log += f"❌ Brak wzorca\n"
                    brak_wzorca_files.append(pdf_path)
            buffer.append(log)
    gui_callback(buffer, len(wszystkie_pdf))

def start_przeszukiwanie():
    folder = folder_var.get()
    if not os.path.isdir(folder):
        messagebox.showerror("Błąd", "Wybierz poprawny folder.")
        return

    output_text.delete(1.0, tk.END)
    listbox.delete(0, tk.END)
    duplikat_listbox.delete(0, tk.END)
    progressbar.start()
    btn_przeszukaj.config(state="disabled")

    tekst = wybrany_wzorzec.get().strip()
    wzorzec = rf"\b{tekst}\b"
    ignore_case = czy_ignore_case.get()

    def gui_callback(buffer, liczba_plikow):
        for log in buffer:
            if "Brak wzorca" in log:
                output_text.insert(tk.END, log + "\n", "brak_wzorca")
            elif "Duplikat treści" in log:
                output_text.insert(tk.END, log + "\n", "duplikat")
            else:
                output_text.insert(tk.END, log + "\n")

        for f in brak_wzorca_files:
            listbox.insert(tk.END, f)
        for f in duplikaty:
            duplikat_listbox.insert(tk.END, f)

        progressbar.stop()
        btn_przeszukaj.config(state="normal")
        output_text.insert(tk.END, f"📦 Łącznie przeszukano plików: {liczba_plikow}\n", "info")

    threading.Thread(target=przeszukaj_pdfy, args=(folder, wzorzec, ignore_case, gui_callback)).start()

def otworz_pdf(path):
    try:
        if platform.system() == "Windows":
            os.startfile(path)
        elif platform.system() == "Darwin":
            subprocess.run(["open", path])
        else:
            subprocess.run(["xdg-open", path])
    except Exception as e:
        messagebox.showerror("Błąd", f"Nie udało się otworzyć pliku:\n{path}\n\n{e}")

def otworz_wybrany_pdf(event):
    index = listbox.curselection()
    if index:
        otworz_pdf(listbox.get(index[0]))

def otworz_duplikat_i_oryginal(event):
    index = duplikat_listbox.curselection()
    if not index:
        return
    duplikat_path = duplikat_listbox.get(index[0])
    tekst, _ = fast_extract_text(duplikat_path)
    tekst_hash = hashlib.md5(tekst.encode("utf-8")).hexdigest()
    oryginal_path = next((p for p in poprawne_bez_duplikatow if hashlib.md5(fast_extract_text(p)[0].encode("utf-8")).hexdigest() == tekst_hash), None)
    otworz_pdf(duplikat_path)
    if oryginal_path:
        otworz_pdf(oryginal_path)

def kopiuj_poprawne_pliki():
    if not poprawne_bez_duplikatow:
        messagebox.showwarning("Brak danych", "Brak poprawnych plików do skopiowania.")
        return

    folder_docelowy = filedialog.askdirectory(title="Wybierz folder docelowy")
    if not folder_docelowy:
        return

    root_folder = folder_var.get()  # folder źródłowy

    for plik in poprawne_bez_duplikatow:
        try:
            # znajdź ścieżkę względną względem folderu źródłowego
            rel_path = os.path.relpath(plik, root_folder)
            dest_path = os.path.join(folder_docelowy, rel_path)

            # utwórz folder docelowy jeśli nie istnieje
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)

            # kopiuj plik
            shutil.copy(plik, dest_path)

        except Exception as e:
            messagebox.showerror("Błąd", f"Nie udało się skopiować pliku:\n{plik}\n\n{e}")
            continue

    messagebox.showinfo("Sukces", f"Skopiowano {len(poprawne_bez_duplikatow)} plików do:\n{folder_docelowy}")
# GUI
import threading
root = tk.Tk()
root.title("PDF Checker – szybka wersja")
root.geometry("900x720")

folder_var = tk.StringVar()
wybrany_wzorzec = tk.StringVar(value=wzorce[0])
czy_ignore_case = tk.BooleanVar(value=True)

frame = tk.Frame(root)
frame.pack(pady=10)

tk.Label(frame, text="Folder z PDF-ami:").grid(row=0, column=0, padx=5, pady=5)
tk.Entry(frame, textvariable=folder_var, width=60).grid(row=0, column=1, padx=5)
tk.Button(frame, text="Wybierz folder", command=lambda: folder_var.set(filedialog.askdirectory())).grid(row=0, column=2, padx=5)

tk.Label(frame, text="Wzorzec do wyszukania:").grid(row=1, column=0, padx=5, pady=5)
tk.OptionMenu(frame, wybrany_wzorzec, *wzorce).grid(row=1, column=1, sticky="w", padx=5)

tk.Checkbutton(frame, text="Ignoruj wielkość liter", variable=czy_ignore_case).grid(row=2, column=1, sticky="w", padx=5)

btn_przeszukaj = tk.Button(root, text="🔍 Przeszukaj PDF-y", command=start_przeszukiwanie, bg="#4CAF50", fg="white", padx=10, pady=5)
btn_przeszukaj.pack(pady=5)

btn_kopiuj = tk.Button(root, text="📁 Skopiuj poprawne PDF-y", command=kopiuj_poprawne_pliki, bg="#2196F3", fg="white", padx=10, pady=5)
btn_kopiuj.pack(pady=(0, 10))

progressbar = ttk.Progressbar(root, mode="indeterminate", length=300)
progressbar.pack(pady=5)

output_text = scrolledtext.ScrolledText(root, wrap=tk.WORD, width=110, height=20)
output_text.pack(padx=10, pady=10)
output_text.tag_config("brak_wzorca", foreground="red")
output_text.tag_config("duplikat", foreground="orange")
output_text.tag_config("info", foreground="blue", font=("Arial", 10, "bold"))

tk.Label(root, text="📄 Pliki bez wzorca (kliknij 2x, aby otworzyć):").pack(pady=(10, 0))
listbox = tk.Listbox(root, width=110, height=6)
listbox.pack(padx=10, pady=(0, 10))
listbox.bind("<Double-Button-1>", otworz_wybrany_pdf)

tk.Label(root, text="📄 Pliki duplikaty (kliknij 2x, aby otworzyć oba):").pack(pady=(10, 0))
duplikat_listbox = tk.Listbox(root, width=110, height=6)
duplikat_listbox.pack(padx=10, pady=(0, 10))
duplikat_listbox.bind("<Double-Button-1>", otworz_duplikat_i_oryginal)

root.mainloop()
