# copy_all_pdfs.py
import shutil
from pathlib import Path
import os

# === KONFIGURACJA ===
SOURCE_DIR = r"C:\Users\DELL\Sm Dropbox\Faktury kontrahentów\greatstore\4.11.2025\drop 1"    # folder główny (z podfolderami)
DEST_DIR   = r"C:\Users\DELL\Sm Dropbox\Faktury kontrahentów\greatstore\4.11.2025\drop1_2"  # gdzie mają trafić PDF-y
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
            parent_folder = pdf_path.parent.name  # nazwa folderu nadrzędnego
            base_name = pdf_path.stem
            ext = pdf_path.suffix
            new_name = f"{base_name}_{parent_folder}{ext}"

            if preserve_structure:
                rel_path = pdf_path.relative_to(src)
                target_path = dst / rel_path.parent / new_name
                target_path.parent.mkdir(parents=True, exist_ok=True)
            else:
                target_path = dst / new_name

            # jeśli istnieje plik o tej nazwie — dopisz licznik
            counter = 1
            while target_path.exists():
                new_name = f"{base_name}_{parent_folder}_{counter}{ext}"
                target_path = dst / new_name
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
