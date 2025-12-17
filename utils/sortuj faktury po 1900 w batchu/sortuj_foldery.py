import os
import re
import shutil
import fitz  # PyMuPDF
import pandas as pd
from pathlib import Path
from multiprocessing import Pool, cpu_count
from tqdm import tqdm

# ===================== KONFIG ==========================
SOURCE_DIR = r"C:\Users\DELL\Desktop\great_posortowane"
TARGET_DIR = r"C:\Users\DELL\Desktop\great 15.12"
NIP_FILE = r"ListaKontrahentówdo ksiegowania Excell.xlsx"
BATCH_SIZE = 1900

# Regex wyłapujący wszystkie typy NIP
NIP_REGEX = re.compile(
    r"\b(\d{10})\b|"                # 1234567890
    r"\b(\d{3}[-\s]?\d{3}[-\s]?\d{2}[-\s]?\d{2})\b"  # 123-456-78-90
)

# =======================================================


def extract_nip_from_pdf(pdf_path: Path) -> str | None:
    """Odczytuje NIP z treści PDF. Zwraca 10 cyfr lub None."""
    try:
        doc = fitz.open(pdf_path)
        text = ""
        for page in doc:
            text += page.get_text("text")
        doc.close()

        matches = NIP_REGEX.findall(text)

        for m in matches:
            nip_raw = "".join(m).replace("-", "").replace(" ", "")
            nip_digits = re.sub(r"\D", "", nip_raw)
            if len(nip_digits) == 10:
                return nip_digits

    except Exception:
        return None

    return None


def process_one(pdf_path: Path):
    """Funkcja wołana w wielu procesach: odczytuje NIP i generuje nową nazwę."""
    nip = extract_nip_from_pdf(pdf_path)
    parent_name = pdf_path.parent.name
    new_name = f"{parent_name}_{pdf_path.name}"
    return (pdf_path, nip, new_name)


def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def main():
    # ========= Wczytaj listę NIP-ów księgowanych ==========================
    if NIP_FILE.endswith(".xlsx"):
        nip_df = pd.read_excel(NIP_FILE, dtype=str)
    else:
        nip_df = pd.read_csv(NIP_FILE, dtype=str)

    nip_df["NIP"] = nip_df["NIP"].astype(str).str.replace(r"\D", "", regex=True)
    nip_set = set(nip_df["NIP"].dropna().tolist())

    print(f"[INFO] Wczytano {len(nip_set)} NIPów z pliku.")

    # ========= Pobierz wszystkie PDF-y ===================================
    pdf_files = list(Path(SOURCE_DIR).rglob("*.pdf"))
    print(f"[INFO] Znaleziono {len(pdf_files)} plików PDF.")

    # ========= Multiprocessing ===========================================
    print(f"[INFO] Używam {cpu_count()} rdzeni CPU...")

    with Pool(cpu_count()) as pool:
        results = list(tqdm(pool.imap(process_one, pdf_files), total=len(pdf_files)))

    # ========= Przygotowanie katalogów ==================================
    base_target = Path(TARGET_DIR)
    ensure_dir(base_target)

    ksiegowane_dir = base_target / "ksiegowane_z_pliku"
    brak_nipu_dir = base_target / "brak_nipu"
    ensure_dir(ksiegowane_dir)
    ensure_dir(brak_nipu_dir)

    batch_idx = 1
    batch_counter = 0
    batch_dir = base_target / f"batch_{batch_idx:03d}"
    ensure_dir(batch_dir)

    # ========= Kopiowanie plików ========================================
    for pdf_path, nip, new_name in tqdm(results, desc="Kopiowanie"):

        if nip is None:
            # PDF bez NIP-u
            dest = brak_nipu_dir / new_name

        elif nip in nip_set:
            # NIP należy do księgowanych
            dest = ksiegowane_dir / new_name

        else:
            # NIP jest, ale nie księgowany → batchowanie
            if batch_counter >= BATCH_SIZE:
                batch_idx += 1
                batch_counter = 0
                batch_dir = base_target / f"batch_{batch_idx:03d}"
                ensure_dir(batch_dir)

            dest = batch_dir / new_name
            batch_counter += 1

        shutil.copy2(pdf_path, dest)

    # ========= Podsumowanie =============================================
    print("\n============================================")
    print("        ZAKOŃCZONO PRZETWARZANIE PDF-ów")
    print("============================================")
    print(f"[INFO] Utworzono folderów batch: {batch_idx}")
    print(f"[INFO] Folder księgowane: {ksiegowane_dir}")
    print(f"[INFO] Folder brak_nipu: {brak_nipu_dir}")
    print("============================================")


if __name__ == "__main__":
    main()
