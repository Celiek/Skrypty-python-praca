import os
import platform
import re
import subprocess
import threading
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext, ttk
import hashlib

from pdfminer.high_level import extract_text

# Lista wzorców
wzorce = ["EXTRASTORE", "GREATSTORE", "SHUMEE"]

# Globalna lista ścieżek plików bez wzorca
brak_wzorca_files = []

# Główna funkcja przeszukiwania PDF-ów
def przeszukaj_pdfy():
    folder = folder_var.get()
    unikalne_teksty = {}
    if not os.path.isdir(folder):
        messagebox.showerror("Błąd", "Wybierz poprawny folder.")
        return

    wzorzec = rf"\b{re.escape(wybrany_wzorzec.get())}\b"

    # Reset GUI i stanu
    btn_przeszukaj.config(state="disabled")
    progressbar.start()
    output_text.delete(1.0, tk.END)
    listbox.delete(0, tk.END)
    brak_wzorca_files.clear()

    try:
        for root_dir, dirs, files in os.walk(folder):
            for filename in files:
                if filename.lower().endswith(".pdf"):
                    pdf_path = os.path.join(root_dir, filename)
                    output_text.insert(tk.END, f"\n📄 Przetwarzanie pliku: {pdf_path}\n")

                    try:
                        tekst = extract_text(pdf_path)
                        dopasowanie = re.search(wzorzec, tekst, flags=re.DOTALL | re.IGNORECASE)
                        # Oblicz hash tekstu
                        tekst_hash = hashlib.md5(tekst.encode('utf-8')).hexdigest()

                        # Sprawdź, czy taki tekst już był
                        if tekst_hash in unikalne_teksty:
                            inny_plik = unikalne_teksty[tekst_hash]
                            output_text.insert(tk.END, f"⚠️ Duplikat treści z plikiem: {inny_plik}\n", "duplikat")
                        else:
                            unikalne_teksty[tekst_hash] = pdf_path

                        if dopasowanie:
                            output_text.insert(tk.END, "✅ Znaleziony fragment:\n")
                            output_text.insert(tk.END, dopasowanie.group() + "\n")
                        else:
                            output_text.insert(tk.END, f"❌ Brak wzorca w pliku: {filename}\n", "brak_wzorca")
                            brak_wzorca_files.append(pdf_path)
                            listbox.insert(tk.END, pdf_path)

                    except Exception as e:
                        output_text.insert(tk.END, f"⚠️ Błąd w pliku {filename}: {e}\n")
    finally:
        btn_przeszukaj.config(state="normal")
        progressbar.stop()
        messagebox.showinfo("Zakończono", "Przeszukiwanie PDF-ów zostało zakończone.")

# Obsługa dwukliku w Listbox – otwarcie pliku
def otworz_wybrany_pdf(event):
    selected_index = listbox.curselection()
    if not selected_index:
        return
    filepath = listbox.get(selected_index[0])
    try:
        if platform.system() == "Windows":
            os.startfile(filepath)
        elif platform.system() == "Darwin":
            subprocess.run(["open", filepath])
        else:
            subprocess.run(["xdg-open", filepath])
    except Exception as e:
        messagebox.showerror("Błąd", f"Nie udało się otworzyć pliku:\n{filepath}\n\n{e}")

# Wybór folderu
def wybierz_folder():
    folder = filedialog.askdirectory()
    if folder:
        folder_var.set(folder)

# Uruchomienie przetwarzania w osobnym wątku
def start_przeszukiwanie():
    threading.Thread(target=przeszukaj_pdfy).start()

# ================== TWORZENIE GUI ==================
root = tk.Tk()
root.title("PDF Checker z Listą i Dwuklikiem")
root.geometry("900x700")

folder_var = tk.StringVar()
wybrany_wzorzec = tk.StringVar(value=wzorce[0])

# Ramka górna
frame = tk.Frame(root)
frame.pack(pady=10)

tk.Label(frame, text="Folder z PDF-ami:").grid(row=0, column=0, padx=5, pady=5)
tk.Entry(frame, textvariable=folder_var, width=60).grid(row=0, column=1, padx=5)
tk.Button(frame, text="Wybierz folder", command=wybierz_folder).grid(row=0, column=2, padx=5)

tk.Label(frame, text="Wzorzec do wyszukania:").grid(row=1, column=0, padx=5, pady=5)
tk.OptionMenu(frame, wybrany_wzorzec, *wzorce).grid(row=1, column=1, sticky="w", padx=5)

# Przycisk uruchamiający przeszukiwanie
btn_przeszukaj = tk.Button(
    root,
    text="🔍 Przeszukaj PDF-y",
    command=start_przeszukiwanie,
    bg="#4CAF50",
    fg="white",
    padx=10,
    pady=5
)
btn_przeszukaj.pack(pady=5)

# Pasek postępu
progressbar = ttk.Progressbar(root, mode="indeterminate", length=300)
progressbar.pack(pady=5)

# Pole tekstowe z wynikami
output_text = scrolledtext.ScrolledText(root, wrap=tk.WORD, width=110, height=20)
output_text.pack(padx=10, pady=10)
output_text.tag_config("brak_wzorca", foreground="red")
output_text.tag_config("duplikat", foreground="orange")

# Listbox z plikami bez wzorca
tk.Label(root, text="📄 Pliki bez wzorca (kliknij 2x, aby otworzyć):").pack(pady=(10, 0))
listbox = tk.Listbox(root, width=110, height=8)
listbox.pack(padx=10, pady=(0, 10))
listbox.bind("<Double-Button-1>", otworz_wybrany_pdf)

# Start GUI
root.mainloop()
