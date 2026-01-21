import json
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import hashlib 
import os 
from multiprocessing import freeze_support


CHUNK_FULL = 8*1024*1024
HASH_METHOD = "sha256"
WORKERS = os.cpu_count() or 4

def full_hash(path: Path, method=HASH_METHOD) -> str:
    h = hashlib.new(method)
    with open(path,"rb") as f:
        while chunk:= f.read(CHUNK_FULL):
            h.update(chunk)
    return h.hexdigest()

def compute_full_with_meta(path: Path):
    # return str(path), full_hash(path)
    return {
        "hash": full_hash(path),
        "name":path.name
    }

def generate_hashes_json(folder, out_json="hashes.json"):
    folder = Path(folder)
    files = list(folder.rglob("*.pdf"))

    results = []

    with ProcessPoolExecutor(max_workers=os.cpu_count()) as ex:
        futures = [ex.submit(compute_full_with_meta, f) for f in files]
        for fut in as_completed(futures):
            results.append(fut.result())

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    print(f"[OK] Zapisano {len(results)} hashy")



if __name__ == "__main__":
    freeze_support()
    generate_hashes_json(folder=r"C:\Users\DELL\Sm Dropbox\Faktury kontrahentów\extrastore")