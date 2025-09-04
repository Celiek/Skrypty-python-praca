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
import threading
import fitz

# zmienne do wyłapywania duplikatów
wzorce = ["EXTRASTORE", "GREATSTORE", "SHUMEE","SUPER MERCHANT"]
# zmienne bez wzorca ( do wyświetlania wyników)
brak_wzorca_files = []
# tablica duplikatów
duplikaty = []
liczba_duplikatow = 0
# zawiera dictionary tekstów - do porównania z innymi tekstami
unikalne_teksty = {}
# zawiera liste plików ze wzorcem bez duplikatów
# potem do kopiowania do gotowego pliku
poprawne_bez_duplikatow = []
# pliki ktorych nie udalo sie skopiowac
bledne_pliki = []
# słownik/dictioanry przechowuje inforamcje o tym jaki plik w jakim folderze
# zawiera wzorzec nie pasujący do wyszkiwanego
znalezione_wzorce_w_folderach ={}

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
    # liczenie hash - do sprawdzania duplikatów
    text_hash = hashlib.md5(tekst.encode("utf-8")).hexdigest()
    flags = re.DOTALL | re.IGNORECASE if ignore_case else re.DOTALL
    dopasowanie = re.search(wzorzec, tekst, flags=flags)
    return pdf_path, dopasowanie.group() if dopasowanie else None, None, text_hash


def przeszukaj_pdfy(folder, wzorzec, ignore_case, gui_callback):
    global liczba_duplikatow
    liczba_duplikatow = 0
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
                buffer.append((log, None))
                continue

            if tekst_hash in unikalne_teksty:
                duplikaty.append(pdf_path)
                liczba_duplikatow += 1
                log += f"⚠️ Duplikat treści z: {unikalne_teksty[tekst_hash]}\n"
                buffer.append((log, "duplikat"))
                continue

            unikalne_teksty[tekst_hash] = pdf_path
            tekst, _ = fast_extract_text(pdf_path)

            if wynik:
                log += f"✅ Znaleziony fragment:\n{wynik}\n"
                poprawne_bez_duplikatow.append(pdf_path)
                inne_wzorce = [w for w in wzorce if w.lower() != wynik.lower()]
                znalezione = [wz for wz in inne_wzorce if re.search(wz, tekst, re.IGNORECASE)]

                if znalezione:
                    log += f"🔍 Inne znalezione wzorce: {', '.join(znalezione)}\n"
                    folder_nadrzedny = os.path.basename(os.path.dirname(pdf_path))
                    plik_nazwa = os.path.basename(pdf_path)

                    if folder_nadrzedny not in znalezione_wzorce_w_folderach:
                        znalezione_wzorce_w_folderach[folder_nadrzedny] = {}

                    znalezione_wzorce_w_folderach[folder_nadrzedny][plik_nazwa] = znalezione
                    buffer.append((log, "inne_wzorce"))
                    continue
                else:
                    buffer.append((log, None))
            else:
                znalezione = [wz for wz in wzorce if re.search(wz, tekst, re.IGNORECASE)]
                if znalezione:
                    log += f"❌ Brak wzorca {wybrany_wzorzec.get()} – znaleziony wzorzec: {znalezione[0]}\n"
                else:
                    log += f"❌ Brak wzorca {wybrany_wzorzec.get()}\n"
                brak_wzorca_files.append(pdf_path)
                buffer.append((log, "brak_wzorca"))

    gui_callback(buffer, len(wszystkie_pdf))


def start_przeszukiwanie():
    folder = folder_var.get()
    if not os.path.isdir(folder):
        messagebox.showerror("Błąd", "Wybierz poprawny folder.")
        return
    duplikat_output_text.delete(1.0, tk.END)
    output_text.delete(1.0, tk.END)
    listbox.delete(0, tk.END)
    duplikat_listbox.delete(0, tk.END)
    progressbar.start()
    btn_przeszukaj.config(state="disabled")

    tekst = wybrany_wzorzec.get().strip()
    wzorzec = rf"\b{tekst}\b"
    ignore_case = czy_ignore_case.get()

    def gui_callback(buffer, liczba_plikow):
        for entry in buffer:
            if isinstance(entry, tuple):
                log, tag = entry
            else:
                log, tag = entry, None

            if tag == "brak_wzorca" and "znaleziony wzorzec:" in log:
                czesc_czerwona, czesc_fioletowa = log.split("– znaleziony wzorzec:")
                output_text.insert(tk.END, czesc_czerwona + "– ", "brak_wzorca")
                output_text.insert(tk.END, "znaleziony wzorzec:" + czesc_fioletowa.strip() + "\n", "inne_wzorce")

            elif "Duplikat treści" in log:

                duplikat_output_text.insert(tk.END, log + "\n", "duplikat")
            elif tag:
                output_text.insert(tk.END, log + "\n", tag)
            else:
                output_text.insert(tk.END, log + "\n")

        for f in brak_wzorca_files:
            listbox.insert(tk.END, f)
        for f in duplikaty:
            duplikat_listbox.insert(tk.END, f)

        progressbar.stop()
        btn_przeszukaj.config(state="normal")
        output_text.insert(tk.END, f"📦 Łącznie przeszukano plików: {liczba_plikow}\n", "info")
        output_text.insert(tk.END, f"♻️ Liczba duplikatów treści: {liczba_duplikatow}\n", "info")
        print("=== PODSUMOWANIE DIAGNOSTYCZNE ===")
        print(f"Plików przeszukanych      : {liczba_plikow}")
        print(f"Znalezione duplikaty      : {len(duplikaty)}")
        print(f"Poprawne bez duplikatów   : {len(poprawne_bez_duplikatow)}")
        print(f"Brak wzorca               : {len(brak_wzorca_files)}")
        print(f"SUMA                      : {len(duplikaty) + len(poprawne_bez_duplikatow) + len(brak_wzorca_files)}")
        print("===================================")
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
    if not poprawne_bez_duplikatow and not brak_wzorca_files:
        messagebox.showwarning("Brak danych", "Brak plików do skopiowania.")
        return

    folder_docelowy = filedialog.askdirectory(title="Wybierz folder docelowy")
    if not folder_docelowy:
        return

    root_folder = folder_var.get()
    kopiowane_razem = 0
    bledy = 0

    # KOPIOWANIE: poprawne
    for plik in poprawne_bez_duplikatow:
        try:
            rel_path = os.path.relpath(plik, root_folder)
            folder_nadrzedny = os.path.dirname(rel_path)

            tekst, _ = fast_extract_text(plik)
            wzorzec_znaleziony = next((wz for wz in wzorce if wz.lower() in tekst.lower()), "NIEZNANY")
            folder_z_wzorcem = wzorzec_znaleziony.upper()

            folder_koncowy = os.path.join(folder_docelowy, folder_z_wzorcem, folder_nadrzedny)
            os.makedirs(folder_koncowy, exist_ok=True)
            shutil.copy(plik, os.path.join(folder_koncowy, os.path.basename(plik)))
            kopiowane_razem += 1
        except Exception as e:
            print(f"[BŁĄD] Nie skopiowano: {plik}\nPowód: {e}")
            bledy += 1

    # KOPIOWANIE: brak wzorca
    for plik in brak_wzorca_files:
        try:
            rel_path = os.path.relpath(plik, root_folder)
            folder_nadrzedny = os.path.dirname(rel_path)

            tekst, _ = fast_extract_text(plik)
            wzorzec_dopasowany = next((wz for wz in wzorce if wz.lower() in tekst.lower()), "NIEZNANY")
            folder_z_wzorcem = wzorzec_dopasowany.upper()

            folder_koncowy = os.path.join(folder_docelowy, folder_z_wzorcem, folder_nadrzedny)
            os.makedirs(folder_koncowy, exist_ok=True)
            shutil.copy(plik, os.path.join(folder_koncowy, os.path.basename(plik)))
            kopiowane_razem += 1
        except Exception as e:
            print(f"[BŁĄD] Nie skopiowano: {plik}\nPowód: {e}")
            bledy += 1

    messagebox.showinfo(
        "Podsumowanie kopiowania",
        f"✅ Skopiowano: {kopiowane_razem}\n❌ Błędy kopiowania: {bledy}\n📂 Folder docelowy: {folder_docelowy}"
    )




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
# self explanatory
btn_przeszukaj = tk.Button(root, text="🔍 Przeszukaj PDF-y", command=start_przeszukiwanie, bg="#4CAF50", fg="white")
btn_przeszukaj.pack(pady=5)
# guzik zapisujący do folderów
btn_kopiuj = tk.Button(root, text="💾 Zapisz bez powtórzeń", command=kopiuj_poprawne_pliki, bg="#2196F3", fg="white")
btn_kopiuj.pack(pady=10)

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
# listboxy z wynikami działania skryptu
listbox_frame = tk.Frame(root)
listbox_frame.pack(padx=10, pady=5)

listbox = tk.Listbox(listbox_frame, width=80, height=8)
listbox.pack(side="left", padx=5)
listbox.bind("<Double-Button-1>", otworz_wybrany_pdf)

duplikat_listbox = tk.Listbox(listbox_frame, width=80, height=8)
duplikat_listbox.pack(side="left", padx=5)
duplikat_listbox.bind("<Double-Button-1>", otworz_duplikat_i_oryginal)

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
