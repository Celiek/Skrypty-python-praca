import hashlib
import os
import platform
import re
import shutil
import subprocess
import tkinter as tk
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
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

dozwolone_teskty_w_pliku = ["faktura","invoice","korekta","Faktura VAT KOREKTA","Invoice FS","Faktura eksportowa","Faktura VAT korekta"," Faktura VAT"]

cache_tekstow = {}
dozwolone_nazwy = ["EXTRASTORE", "GREATSTORE", "SHUMEE", "SUPER MERCHANT"]
wzorce = dozwolone_nazwy[:]  # zachowujemy spójność z GUI

_local_cache = {}

# ===== FUNKCJE =====
def fast_extract_text(pdf_name):
    """Ekstrakcja tekstu z PDF z cache’owaniem (szybsza praca)."""
    if pdf_name in _local_cache:
        return _local_cache[pdf_name], None
    try:
        with fitz.open(pdf_name) as doc:
            text = ''.join(page.get_text("text") for page in doc)
            _local_cache[pdf_name] = text
            return text, None
    except Exception as e:
        return None, f"Błąd odczytu PDF: {e} | plik: {pdf_name}"


def przetworz_plik(pdf_path, wzorce_re, ignore_case):
    """Analiza pojedynczego PDF — zwraca dane o dopasowaniu i hash treści."""
    tekst, blad = fast_extract_text(pdf_path)
    if blad:
        return pdf_path, None, blad, None

    text_hash = hashlib.md5(tekst.encode("utf-8")).hexdigest()

    dopasowany = None
    for wzorzec in wzorce_re:
        if wzorzec.search(tekst):
            dopasowany = wzorzec.pattern.strip("\\b").strip("\\b")  # surowy tekst wzorca
            break

    return pdf_path, dopasowany, None, text_hash


def przeszukaj_pdfy(folder, wzorce, ignore_case, gui_callback):
    """Przeszukuje PDF-y równolegle (z multiprocessingiem)."""
    global liczba_duplikatow
    liczba_duplikatow = 0
    brak_wzorca_files.clear()
    duplikaty.clear()
    unikalne_teksty.clear()
    poprawne_bez_duplikatow.clear()
    buffer = []

    # przygotuj wzorce jako regexy
    flags = re.IGNORECASE if ignore_case else 0
    wzorce_re = [re.compile(rf"\b{re.escape(w)}\b", flags) for w in wzorce]

    wszystkie_pdf = [
        os.path.join(root, f)
        for root, _, files in os.walk(folder)
        for f in files if f.lower().endswith(".pdf")
    ]

    with ProcessPoolExecutor() as executor:
        func = partial(przetworz_plik, wzorce_re=wzorce_re, ignore_case=ignore_case)
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

            if wynik:
                log += f"✅ Dopasowano wzorzec: {wynik}\n"
                poprawne_bez_duplikatow.append(pdf_path)
            else:
                log += f"❌ Brak wzorca z listy dozwolonych\n"
                brak_wzorca_files.append(pdf_path)

            buffer.append((log, None))

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

    ignore_case = czy_ignore_case.get()

    def gui_callback(buffer, liczba_plikow):
        for log, tag in buffer:
            if "Duplikat treści" in log:
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
        output_text.insert(tk.END, f"♻️ Duplikaty treści: {liczba_duplikatow}\n", "info")

    threading.Thread(target=przeszukaj_pdfy, args=(folder, dozwolone_teskty_w_pliku, ignore_case, gui_callback)).start()


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
    oryginal_path = next(
        (p for p in poprawne_bez_duplikatow if hashlib.md5(fast_extract_text(p)[0].encode("utf-8")).hexdigest() == tekst_hash),
        None
    )
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
    files_to_copy = []

    # przygotuj regexy dozwolonych nazw
    dozwolone_re = [re.compile(rf"\b{re.escape(w)}\b", re.IGNORECASE) for w in dozwolone_nazwy]

    for plik in poprawne_bez_duplikatow + brak_wzorca_files:
        try:
            rel_path = os.path.relpath(plik, root_folder)
            folder_nadrzedny = os.path.dirname(rel_path)
            tekst, _ = fast_extract_text(plik)

            # znajdź czy zawiera dozwolone nazwy
            znalezione = [pat.pattern.strip("\\b").strip("\\b") for pat in dozwolone_re if pat.search(tekst)]

            if znalezione:
                folder_z_wzorcem = znalezione[0].upper()
            else:
                folder_z_wzorcem = "INNE"

            folder_koncowy = os.path.join(folder_docelowy, folder_z_wzorcem, folder_nadrzedny)
            dst = os.path.join(folder_koncowy, os.path.basename(plik))
            files_to_copy.append((plik, dst))
        except Exception as e:
            print(f"[BŁĄD] Nie skopiowano: {plik}\nPowód: {e}")
            bledy += 1

    # Równoległe kopiowanie
    def copy_file(src_dst):
        src, dst = src_dst
        try:
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            shutil.copy(src, dst)
            return True
        except Exception as e:
            print(f"[BŁĄD kopiowania] {src}: {e}")
            return False

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(copy_file, files_to_copy))
        kopiowane_razem = sum(1 for r in results if r)

    messagebox.showinfo(
        "Podsumowanie kopiowania",
        f"✅ Skopiowano: {kopiowane_razem}\n❌ Błędy: {bledy}\n📂 Folder docelowy: {folder_docelowy}"
    )

def main():
    global root, folder_var, czy_ignore_case, duplikat_output_text, output_text
    global listbox, duplikat_listbox, progressbar, btn_przeszukaj, btn_kopiuj

    root = tk.Tk()
    root.title("PDF Checker — szybka wersja")
    root.geometry("1200x720")

    folder_var = tk.StringVar()
    czy_ignore_case = tk.BooleanVar(value=True)

    frame = tk.Frame(root)
    frame.pack(pady=10)

    tk.Label(frame, text="Folder z PDF-ami:").grid(row=0, column=0, padx=5)
    tk.Entry(frame, textvariable=folder_var, width=60).grid(row=0, column=1, padx=5)
    tk.Button(frame, text="Wybierz folder", command=lambda: folder_var.set(filedialog.askdirectory())).grid(row=0, column=2, padx=5)

    tk.Checkbutton(frame, text="Ignoruj wielkość liter", variable=czy_ignore_case).grid(row=1, column=1, sticky="w")

    btn_przeszukaj = tk.Button(root, text="🔍 Przeszukaj PDF-y", command=start_przeszukiwanie, bg="#4CAF50", fg="white")
    btn_przeszukaj.pack(pady=5)

    btn_kopiuj = tk.Button(root, text="💾 Zapisz do folderów", command=kopiuj_poprawne_pliki, bg="#2196F3", fg="white")
    btn_kopiuj.pack(pady=10)

    progressbar = ttk.Progressbar(root, mode="indeterminate", length=300)
    progressbar.pack(pady=5)

    label_frame = tk.Frame(root)
    label_frame.pack()
    tk.Label(label_frame, text="📋 Logi główne:").pack(side="left", padx=60)
    tk.Label(label_frame, text="♻️ Duplikaty:").pack(side="left", padx=60)

    output_frame = tk.Frame(root)
    output_frame.pack(padx=10, pady=10)

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

    output_text.tag_config("duplikat", foreground="orange")
    output_text.tag_config("info", foreground="blue", font=("Arial", 10, "bold"))
    duplikat_output_text.tag_config("duplikat", foreground="orange")

    root.mainloop()


if __name__ == "__main__":
    import multiprocessing
    multiprocessing.freeze_support()  # 🧊 dla Windowsa
    main()