import os
import shutil
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext

def get_all_files(folder):
    files_set = set()
    for dirpath, _, filenames in os.walk(folder):
        for file in filenames:
            rel_path = os.path.relpath(os.path.join(dirpath, file), folder)
            files_set.add(rel_path)
    return files_set

def find_missing_files(folder1, folder2):
    files1 = get_all_files(folder1)
    files2 = get_all_files(folder2)
    return sorted(files1 - files2)

def choose_folder(title):
    return filedialog.askdirectory(title=title)

def run_comparison():
    global missing_files, src_folder
    folder1 = choose_folder("Wybierz folder 1 (źródłowy)")
    folder2 = choose_folder("Wybierz folder 2 (porównawczy)")

    if not folder1 or not folder2:
        messagebox.showwarning("Błąd", "Wybierz oba foldery.")
        return

    missing_files = find_missing_files(folder1, folder2)
    src_folder = folder1

    text_output.delete('1.0', tk.END)
    if missing_files:
        text_output.insert(tk.END, "Brakujące pliki:\n")
        for file in missing_files:
            text_output.insert(tk.END, f"{file}\n")
        save_button.config(state=tk.NORMAL)
    else:
        text_output.insert(tk.END, "Brak brakujących plików.\n")
        save_button.config(state=tk.DISABLED)

def save_missing_files():
    if not missing_files:
        messagebox.showinfo("Informacja", "Brak plików do zapisania.")
        return

    output_folder = choose_folder("Wybierz folder docelowy")
    if not output_folder:
        return

    for rel_path in missing_files:
        src_path = os.path.join(src_folder, rel_path)
        dest_path = os.path.join(output_folder, rel_path)

        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        shutil.copy2(src_path, dest_path)

    messagebox.showinfo("Sukces", "Brakujące pliki zostały skopiowane.")

# === GUI ===
root = tk.Tk()
root.title("Porównywarka folderów")

frame = tk.Frame(root)
frame.pack(padx=10, pady=10)

btn_compare = tk.Button(frame, text="Porównaj foldery", command=run_comparison)
btn_compare.pack(fill='x')

save_button = tk.Button(frame, text="Zapisz brakujące pliki", command=save_missing_files, state=tk.DISABLED)
save_button.pack(fill='x', pady=5)

text_output = scrolledtext.ScrolledText(frame, width=80, height=20)
text_output.pack()

# Zmienne globalne
missing_files = []
src_folder = ""

root.mainloop()
