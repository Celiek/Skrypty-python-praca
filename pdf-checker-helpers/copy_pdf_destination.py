# copy_all_pdfs.py
import shutil
from pathlib import Path
import os

##kopiuje pliki pdf z jednego folderu

# === KONFIGURACJA ===
SOURCE_DIR = r"C:\Users\DELL\Desktop\shumee 04.11.2025\SHUMEE"    # folder główny (z podfolderami)
DEST_DIR   = r"C:\Users\DELL\Desktop\shumee 04.11.2025\drop2"  # gdzie mają trafić PDF-y
PRESERVE_STRUCTURE = False  # True = zachowaj strukturę podfolderów, False = wszystko do jednego folderu
DRY_RUN = False              # True = tylko pokaże co by zrobił (bez kopiowania)

def copy_all_pdfs(source_dir: str, dest_dir: str, preserve_structure: bool = False, dry_run: bool = True):
    src = Path(source_dir)
    dst = Path(dest_dir)
    dst.mkdir(parents=True, exist_ok=True)

    if not src.exists():
        print(f"❌ Folder źródłowy nie istnieje: {src}")
        return

    copied = 0
    total = 0

    for pdf_path in src.rglob("*.pdf"):
        total += 1
        try:
            rel_path = pdf_path.relative_to(src)
            if preserve_structure:
                # zachowaj strukturę podfolderów
                target_path = dst / rel_path
                target_path.parent.mkdir(parents=True, exist_ok=True)
            else:
                # wszystko do jednego folderu
                target_path = dst / pdf_path.name

                # jeśli już istnieje plik o tej nazwie — dopisz licznik
                if target_path.exists():
                    base, ext = os.path.splitext(pdf_path.name)
                    counter = 1
                    while True:
                        new_name = f"{base}_{counter}{ext}"
                        target_path = dst / new_name
                        if not target_path.exists():
                            break
                        counter += 1

            if dry_run:
                print(f"[DRY-RUN] Skopiowałbym: {pdf_path} → {target_path}")
            else:
                shutil.copy2(pdf_path, target_path)
                print(f"📄 Skopiowano: {pdf_path} → {target_path}")
                copied += 1

        except Exception as e:
            print(f"⚠️ Błąd przy kopiowaniu {pdf_path}: {e}")

    print(f"\n🔍 Znaleziono {total} plików PDF.")
    if dry_run:
        print(f"✅ Tryb testowy — nic nie zostało skopiowane.")
    else:
        print(f"📦 Skopiowano {copied} plików PDF do {dst}")

if __name__ == "__main__":
    copy_all_pdfs(SOURCE_DIR, DEST_DIR, PRESERVE_STRUCTURE, DRY_RUN)
