import os
import pandas as pd
import hashlib
import shutil
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed


# ===========================================================
#               KONFIGURACJA PRZYSPIESZENIA
# ===========================================================

CHUNK_FULL = 8 * 1024 * 1024
CHUNK_FAST = 1024
HASH_METHOD = "md5"
WORKERS = os.cpu_count()


# ===========================================================
#               FUNKCJE HASHUJĄCE
# ===========================================================

def fast_hash(path: Path, method="md5"):
    h = hashlib.new(method)
    with open(path, "rb") as f:
        h.update(f.read(CHUNK_FAST))
    return h.hexdigest()


def full_hash(path: Path, method="md5"):
    h = hashlib.new(method)
    with open(path, "rb") as f:
        while chunk := f.read(CHUNK_FULL):
            h.update(chunk)
    return h.hexdigest()


def combined_hash(path: Path):
    fh = fast_hash(path, HASH_METHOD)
    return (path, fh, None)


def compute_full_hash(path_fast_full):
    path, fast_h = path_fast_full
    full_h = full_hash(path, HASH_METHOD)
    return (path, fast_h, full_h)


# ===========================================================
#               HASHOWANIE FOLDERÓW
# ===========================================================

def collect_fast_hashes(folder: Path):
    files = list(folder.rglob("*.pdf"))
    out = []

    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        futures = [ex.submit(combined_hash, f) for f in files]
        for fut in as_completed(futures):
            out.append(fut.result())

    return out


def compute_full_hashes_for_matches(files_a, fast_map_b):
    tasks = [(path, fast_h) for (path, fast_h, _) in files_a if fast_h in fast_map_b]
    results = []

    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        futures = [ex.submit(compute_full_hash, t) for t in tasks]
        for fut in as_completed(futures):
            results.append(fut.result())

    return results


# ===========================================================
#         FUNKCJA PRZENOSZENIA DUPLIKATÓW Z FOLDERU A
# ===========================================================

def move_from_A_to_trash(duplicates, trash_dir="kosz_duplikatow"):
    """Przenosi pliki z folderu A do kosza. Folder B zostaje nietknięty."""

    trash = Path(trash_dir)
    trash.mkdir(parents=True, exist_ok=True)

    moved = []

    for d in duplicates:
        src = Path(d["plik_A"])
        if src.exists():
            dest = trash / src.name
            shutil.move(str(src), str(dest))
            moved.append(str(dest))

    return moved


# ===========================================================
#               GŁÓWNA FUNKCJA PORÓWNANIA
# ===========================================================

def find_duplicates_pdfs(folder_a, folder_b, save_report=False, report_path="duplikaty.xlsx"):
    folder_a, folder_b = Path(folder_a), Path(folder_b)

    # 1) FAST hash B
    files_b = collect_fast_hashes(folder_b)
    fast_map_b = {fast_h: path for (path, fast_h, _) in files_b}

    # 2) FAST hash A
    files_a = collect_fast_hashes(folder_a)

    # 3) FULL hash B
    b_full = compute_full_hashes_for_matches(files_b, fast_map_b)
    full_map_b = {full_h: path for (path, fast_h, full_h) in b_full}

    # 4) FULL hash A
    a_full = compute_full_hashes_for_matches(files_a, fast_map_b)

    # 5) dopasowanie po pełnym hash
    duplicates = []
    for (path_a, fast_h, full_h) in a_full:
        if full_h in full_map_b:
            duplicates.append({
                "plik_A": str(path_a),
                "plik_B": str(full_map_b[full_h]),
                "hash": full_h
            })

    # 6) raport XLSX
    if save_report:
        pd.DataFrame(duplicates).to_excel(report_path, index=False)

    print("====================================")
    print("      RAPORT PORÓWNANIA PDF")
    print("====================================")
    print(f"Folder A: {folder_a}")
    print(f"Folder B: {folder_b}")
    print(f"PDF w A: {len(files_a)}")
    print(f"PDF w B: {len(files_b)}")
    print(f"Duplikaty: {len(duplicates)}")
    print(f"Raport: {report_path}" if save_report else "Raport nie zapisany")

    return duplicates


# ===========================================================
#               START
# ===========================================================

if __name__ == "__main__":
    duplicates = find_duplicates_pdfs(
        r"C:\Users\DELL\Desktop\sm_posortowane_03_10_25\SHUMEE",
        r"C:\Users\DELL\Documents\FAKTURY\shumee_posortowane_26_11_2025\SHUMEE",
        save_report=True,
        report_path="raport_duplikatow_SHUMEE_03_12.xlsx"
    )

    # 🔥 PRZENOSZENIE WYŁĄCZNIE z folderu A
    moved = move_from_A_to_trash(duplicates, trash_dir="kosz_duplikatow")

    print("\nPrzeniesiono do kosza:")
    for m in moved:
        print(" -", m)
