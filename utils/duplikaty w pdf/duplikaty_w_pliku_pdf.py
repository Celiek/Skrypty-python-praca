import os
import pandas as pd
import hashlib
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import json


# ===========================================================
#               KONFIGURACJA
# ===========================================================

CHUNK_FULL = 8 * 1024 * 1024   # 8 MB
CHUNK_FAST = 256  * 1024            # 1 KB
HASH_METHOD = "md5"           # możesz zmienić na sha256
WORKERS = os.cpu_count() or 8


# ===========================================================
#               FUNKCJE HASHUJĄCE
# ===========================================================

def fast_hash(path: Path, method=HASH_METHOD) -> str:
    h = hashlib.new(method)
    size = path.stat().st_size

    with open(path, "rb") as f:
        h.update(f.read(4096))        # początek
        if size > 8192:
            f.seek(size // 2)
            h.update(f.read(4096))    # środek
            f.seek(-4096, os.SEEK_END)
            h.update(f.read(4096))    
    return h.hexdigest()


def full_hash(path: Path, method=HASH_METHOD) -> str:
    h = hashlib.new(method)
    with open(path, "rb") as f:
        while chunk := f.read(CHUNK_FULL):
            h.update(chunk)
    return h.hexdigest()


def compute_fast(path: Path):
    return path, fast_hash(path)


def compute_full(task):
    path, fast_h = task
    return path, fast_h, full_hash(path)


# ===========================================================
#           ZBIERANIE FAST HASHY (PDF + PODFOLDERY)
# ===========================================================

def collect_fast_hashes(folder: Path):
    files = list(folder.rglob("*.pdf"))
    results = []

    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        futures = [ex.submit(compute_fast, f) for f in files]
        for fut in as_completed(futures):
            results.append(fut.result())

    return results


# ===========================================================
#        DUPLIKATY W JEDNYM FOLDERZE
# ===========================================================

def find_duplicates_in_single_folder(
    folder,
    save_report=False,
    report_path="duplikaty_jeden_folder.xlsx"
):
    folder = Path(folder)

    print(f"[INFO] Skanuję folder: {folder}")

    # 1) FAST HASH
    fast_results = collect_fast_hashes(folder)
    total_files = len(fast_results)

    fast_groups = {}
    for path, fast_h in fast_results:
        fast_groups.setdefault(fast_h, []).append(path)

    # tylko potencjalne kolizje
    fast_collisions = {
        h: paths for h, paths in fast_groups.items()
        if len(paths) > 1
    }

    print(f"[INFO] Potencjalne kolizje (fast-hash): {len(fast_collisions)}")

    # 2) FULL HASH tylko dla kolizji
    full_tasks = [
        (path, fast_h)
        for fast_h, paths in fast_collisions.items()
        for path in paths
    ]

    full_results = []
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        futures = [ex.submit(compute_full, t) for t in full_tasks]
        for fut in as_completed(futures):
            full_results.append(fut.result())

    full_groups = {}
    for path, _, full_h in full_results:
        full_groups.setdefault(full_h, []).append(path)

    # 3) PRAWDZIWE DUPLIKATY
    duplicates = []
    for full_h, paths in full_groups.items():
        if len(paths) > 1:
            for p in paths:
                duplicates.append({
                    "plik": str(p),
                    "hash": full_h,
                    "ilosc_w_grupie": len(paths)
                })

    duplicated_files = len(duplicates)
    duplicated_groups = len([v for v in full_groups.values() if len(v) > 1])

    # 4) RAPORT XLSX
    if save_report and duplicates:
        df = pd.DataFrame(duplicates)
        df.sort_values(["hash", "plik"], inplace=True)
        df.to_excel(report_path, index=False)

    # 5) PODSUMOWANIE
    print("====================================")
    print("        RAPORT DUPLIKATÓW PDF")
    print("====================================")
    print(f"Folder: {folder}")
    print(f"Wszystkie PDF: {total_files}")
    print(f"Pliki zduplikowane: {duplicated_files}")
    print(f"Grupy duplikatów: {duplicated_groups}")
    print(f"Raport: {report_path}" if save_report else "Raport nie zapisany")
    print("====================================")

    return duplicates


# ===========================================================
#               START
# ===========================================================

if __name__ == "__main__":
    duplicates = find_duplicates_in_single_folder(
        r"C:\Users\DELL\Sm Dropbox\Faktury kontrahentów\SHUMEE\15.12.2025",
        save_report=True,
        report_path="raport_duplikatow_shumee_29_12_2025.xlsx"
    )
