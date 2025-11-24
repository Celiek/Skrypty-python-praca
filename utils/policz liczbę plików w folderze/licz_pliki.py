import os
import pandas as pd
from pathlib import Path

# =========================================
# KONFIGURACJA
# =========================================
ROOT_DIR = r"C:\Users\DELL\Documents\FAKTURY\extra_posortowane_17.11.2025\EXTRASTORE"   # folder główny
OUT_XLSX = r"raport_liczba_plikow EXTRASTORE.xlsx"                  # plik wynikowy


def count_files_in_folders(root_dir: str):
    root = Path(root_dir)
    results = []

    for folder, subfolders, files in os.walk(root):
        folder_path = Path(folder)
        file_count = len(files)

        # dodaj rekord tylko gdy folder zawiera pliki
        results.append({
            "Folder": str(folder_path),
            "Liczba plików": file_count
        })

    return results


def save_to_excel(data, out_path: str):
    df = pd.DataFrame(data)
    df.to_excel(out_path, index=False)
    print(f"✅ Zapisano raport do: {out_path}")


def main():
    print("⏳ Liczę pliki...")
    data = count_files_in_folders(ROOT_DIR)
    save_to_excel(data, OUT_XLSX)
    print("✔️ Gotowe!")


if __name__ == "__main__":
    main()
