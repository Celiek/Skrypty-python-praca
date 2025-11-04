# copy_unique_pdfs.py
import hashlib
import shutil
from pathlib import Path
import os
import pandas as pd

# === KONFIGURACJA ===
FOLDER_B = r"C:\Users\DELL\Sm Dropbox\Faktury kontrahentów\greatstore\27.10.2025"     # folder 1 (np. baza)
FOLDER_A = r"C:\Users\DELL\Desktop\greatstore 04.11.2025"   # folder 2 (do porównania)
DEST_DIR = r"C:\Users\DELL\Desktop\great dedup 4.11"  # tu skopiujemy unikalne PDF-y
HASH_METHOD = "sha256"   # lub 'md5' dla szybszego działania
DRY_RUN = False           # True = test, False = faktyczne kopiowanie
SAVE_REPORT = True       # zapis raportu XLSX
REPORT_PATH = "unikalne_raport.xlsx"


# === FUNKCJE ===
def file_hash(path: Path, method="sha256", chunk=1024 * 1024):
    """Zwraca hash pliku (SHA-256 lub MD5)."""
    h = hashlib.new(method)
    with open(path, "rb") as f:
        while chunk_data := f.read(chunk):
            h.update(chunk_data)
    return h.hexdigest()


def collect_hashes(folder: Path, method="sha256") -> dict[str, Path]:
    """Tworzy mapę {hash: Path} dla wszystkich PDF-ów w folderze."""
    hashes = {}
    for f in folder.rglob("*.pdf"):
        try:
            h = file_hash(f, method)
            hashes[h] = f
        except Exception as e:
            print(f"⚠️ Błąd przy {f}: {e}")
    return hashes


def copy_unique_pdfs(folder_a, folder_b, dest_dir, method="sha256", dry_run=True, save_report=False, report_path="report.xlsx"):
    folder_a = Path(folder_a)
    folder_b = Path(folder_b)
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)

    if not folder_a.exists() or not folder_b.exists():
        print("❌ Jeden z folderów nie istnieje.")
        return

    print(f"🔍 Liczę hashe w folderze B (porównawczym): {folder_b}")
    hashes_b = collect_hashes(folder_b, method)
    print(f"✅ Znaleziono {len(hashes_b)} plików PDF w folderze B")

    print(f"\n🔎 Szukam unikalnych plików w folderze A: {folder_a}")
    unique_files = []
    total = 0

    for f in folder_a.rglob("*.pdf"):
        total += 1
        try:
            h = file_hash(f, method)
            if h not in hashes_b:
                unique_files.append({"plik": str(f), "hash": h})
                if dry_run:
                    print(f"[UNIKALNY] {f}")
                else:
                    dest_path = dest_dir / f.name
                    # uniknij kolizji nazw
                    if dest_path.exists():
                        base, ext = os.path.splitext(f.name)
                        counter = 1
                        while True:
                            new_name = f"{base}_{counter}{ext}"
                            dest_path = dest_dir / new_name
                            if not dest_path.exists():
                                break
                            counter += 1
                    shutil.copy2(f, dest_path)
                    print(f"📄 Skopiowano: {f} → {dest_path}")
        except Exception as e:
            print(f"⚠️ Błąd przy {f}: {e}")

    print(f"\n📦 Sprawdzono {total} plików PDF w folderze A.")
    print(f"✅ Znaleziono {len(unique_files)} unikalnych plików (nieobecnych w folderze B).")

    # Zapisz raport
    if save_report:
        df = pd.DataFrame(unique_files)
        df.to_excel(report_path, index=False)
        print(f"📊 Raport zapisano do: {Path(report_path).resolve()}")

    if dry_run:
        print("🔎 Tryb testowy — nic nie zostało skopiowane.")
    else:
        print(f"📂 Unikalne pliki PDF skopiowano do: {dest_dir.resolve()}")

    return unique_files


# === URUCHOMIENIE ===
if __name__ == "__main__":
    copy_unique_pdfs(FOLDER_A, FOLDER_B,
                     dest_dir=DEST_DIR,
                     method=HASH_METHOD,
                     dry_run=DRY_RUN,
                     save_report=SAVE_REPORT,
                     report_path=REPORT_PATH)
