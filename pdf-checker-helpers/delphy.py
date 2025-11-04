# delete_pdfs_by_mdate.py
from pathlib import Path
from datetime import datetime, date
import os

# === KONFIGURACJA ===
BASE_DIR = r"C:\Users\DELL\Desktop\skrypty\great_sm"   # <-- główny katalog
TARGET_DATE = date(2025, 10, 27)                  # <-- dzień, którego szukasz
DRY_RUN = False                                   # True = tylko pokaże co by usunął, False = faktycznie usuwa

def delete_pdfs_by_date(base_dir: str, target_date: date, dry_run: bool = True):
    base = Path(base_dir)
    if not base.exists():
        print(f"❌ Folder nie istnieje: {base}")
        return

    deleted = 0
    checked = 0

    for pdf_path in base.rglob("*.pdf"):  # przeszukuje wszystkie podfoldery
        try:
            mtime = datetime.fromtimestamp(pdf_path.stat().st_mtime).date()
            checked += 1
            if mtime == target_date:
                if dry_run:
                    print(f"[DRY-RUN] Usunąłbym: {pdf_path}")
                else:
                    os.remove(pdf_path)
                    print(f"🗑️ Usunięto: {pdf_path}")
                deleted += 1
        except Exception as e:
            print(f"⚠️ Błąd przy pliku {pdf_path}: {e}")

    print(f"\n✅ Sprawdzono {checked} plików PDF.")
    if dry_run:
        print(f"🔎 Znalazłoby się do usunięcia: {deleted} plików (tryb testowy).")
    else:
        print(f"🗑️ Usunięto {deleted} plików PDF zmodyfikowanych {target_date}.")

if __name__ == "__main__":
    delete_pdfs_by_date(BASE_DIR, TARGET_DATE, dry_run=DRY_RUN)
