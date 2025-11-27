import os
import pandas as pd
import hashlib
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed


# ===========================================================
#               KONFIGURACJA PRZYSPIESZENIA
# ===========================================================

CHUNK_FULL = 8 * 1024 * 1024       # 8 MB – szybkie hashowanie dużych plików
CHUNK_FAST = 1024                  # 1 KB – wstępny szybki hash
HASH_METHOD = "md5"                # najszybszy sensowny wybór
WORKERS = os.cpu_count()           # liczba CPU do multiprocessing


# ===========================================================
#               FUNKCJE HASHUJĄCE
# ===========================================================

def fast_hash(path: Path, method="md5"):
    """Bardzo szybki hash pierwszych 1024 bajtów."""
    h = hashlib.new(method)
    with open(path, "rb") as f:
        h.update(f.read(CHUNK_FAST))
    return h.hexdigest()


def full_hash(path: Path, method="md5"):
    """Wolniejszy pełny hash (8 MB chunk), wywoływany tylko gdy fast_hash pasuje."""
    h = hashlib.new(method)
    with open(path, "rb") as f:
        while chunk := f.read(CHUNK_FULL):
            h.update(chunk)
    return h.hexdigest()


def combined_hash(path: Path):
    """
    2-etapowe hashowanie:
      1) hash 1 KB – super szybki
      2) jeśli potrzebne → hash pełny 8 MB
    Zwraca:
      (path, fast_hash, full_hash)
    """
    fh = fast_hash(path, HASH_METHOD)
    return (path, fh, None)  # pełny hash liczymy tylko gdy trzeba


def compute_full_hash(path_fast_full):
    """Liczy pełny hash dla pary (path, fast_hash)."""
    path, fast_h = path_fast_full
    full_h = full_hash(path, HASH_METHOD)
    return (path, fast_h, full_h)


# ===========================================================
#               SKANOWANIE FOLDERU I HASHOWANIE
# ===========================================================

def collect_fast_hashes(folder: Path):
    """Równoległe liczenie tylko fast_hash dla wszystkich PDF w folderze."""
    files = list(folder.rglob("*.pdf"))
    out = []

    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        futures = [ex.submit(combined_hash, f) for f in files]
        for fut in as_completed(futures):
            out.append(fut.result())

    return out


def compute_full_hashes_for_matches(files_a, fast_map_b):
    """
    Dla plików A, których fast_hash jest w folderze B → policz pełny hash.
    """
    tasks = [(path, fast_h) for (path, fast_h, _) in files_a if fast_h in fast_map_b]

    results = []
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        futures = [ex.submit(compute_full_hash, p) for p in tasks]
        for fut in as_completed(futures):
            results.append(fut.result())

    return results


# ===========================================================
#               GŁÓWNA FUNKCJA PORÓWNYWANIA
# ===========================================================

def find_duplicates_pdfs(folder_a, folder_b, save_report=False, report_path="duplikaty.xlsx"):
    folder_a, folder_b = Path(folder_a), Path(folder_b)

    # ===========================
    # 1) FAST HASH folderu B
    # ===========================
    files_b = collect_fast_hashes(folder_b)
    fast_map_b = {fast_h: path for (path, fast_h, _) in files_b}

    # ===========================
    # 2) FAST HASH folderu A
    # ===========================
    files_a = collect_fast_hashes(folder_a)

    # ===========================
    # 3) FULL HASH w folderze B (tylko gdy fast_hash pasuje)
    # ===========================
    b_candidates = [(path, fast_h) for (path, fast_h, _) in files_b]
    b_full = compute_full_hashes_for_matches(files_b, fast_map_b)
    full_map_b = {full_h: path for (path, fast_h, full_h) in b_full}

    # ===========================
    # 4) FULL HASH plików A z dopasowanym fast_hash
    # ===========================
    a_full = compute_full_hashes_for_matches(files_a, fast_map_b)

    # ===========================
    # 5) Dopasowanie full_hash
    # ===========================
    duplicates = []
    for (path_a, fast_h, full_h) in a_full:
        if full_h in full_map_b:
            duplicates.append({
                "plik_A": str(path_a),
                "plik_B": str(full_map_b[full_h]),
                "hash": full_h
            })

    # ===========================
    # 6) RAPORT
    # ===========================
    if save_report:
        df = pd.DataFrame(duplicates)
        df.to_excel(report_path, index=False)

    # ===========================
    # 7) FINALNY LOG (bez logowania w trakcie)
    # ===========================
    print("====================================")
    print("      RAPORT PORÓWNANIA PDF")
    print("====================================")
    print(f"Folder A: {folder_a}")
    print(f"Folder B: {folder_b}")
    print(f"Liczba PDF w A: {len(files_a)}")
    print(f"Liczba PDF w B: {len(files_b)}")
    print(f"Duplikaty: {len(duplicates)}")
    print(f"Raport zapisano: {report_path}" if save_report else "Raport nie zapisany")

    return duplicates


# ===========================================================
#               START
# ===========================================================

if __name__ == "__main__":
    find_duplicates_pdfs(
        r"C:\Users\DELL\Sm Dropbox\Faktury kontrahentów\greatstore\26.11.2025",
        r"C:\Users\DELL\Documents\FAKTURY\great_sm",
        save_report=True,
        report_path="raport_duplikatow_shumee_04_11.xlsx"
    )
