import hashlib
from pathlib import Path
from collections import defaultdict

# =========================
# KONFIGURACJA
# =========================
FOLDER_B = Path(r"C:\Users\DELL\Sm Dropbox\Faktury kontrahentów\SHUMEE\15.12.2025\batch_001 NIE")   # ← zmień
FOLDER_A = Path(r"C:\Users\DELL\Sm Dropbox\Faktury kontrahentów\extrastore\15.12.2025")   # ← zmień

OUT_DUP = "duplikaty.txt"
OUT_ONLY_A = "nieduplikaty_A.txt"
OUT_ONLY_B = "nieduplikaty_B.txt"

CHUNK_SIZE = 1024 * 1024  # 1 MB

# =========================
# SHA256
# =========================
def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(CHUNK_SIZE):
            h.update(chunk)
    return h.hexdigest()

# =========================
# ZBIERANIE HASHY (rekurencyjnie)
# =========================
def collect_hashes(folder: Path) -> dict[str, list[Path]]:
    hashes = defaultdict(list)
    for pdf in folder.rglob("*.pdf"):
        try:
            h = sha256_file(pdf)
            hashes[h].append(pdf)
        except Exception as e:
            print(f"⚠️ Błąd przy {pdf}: {e}")
    return hashes

# =========================
# MAIN
# =========================
def main():
    print("🔍 Liczenie SHA256 – Folder A...")
    hashes_a = collect_hashes(FOLDER_A)

    print("🔍 Liczenie SHA256 – Folder B...")
    hashes_b = collect_hashes(FOLDER_B)

    set_a = set(hashes_a.keys())
    set_b = set(hashes_b.keys())

    common = set_a & set_b
    only_a = set_a - set_b
    only_b = set_b - set_a

    # =========================
    # DUPLIKATY
    # =========================
    with open(OUT_DUP, "w", encoding="utf-8") as f:
        f.write("===== DUPLIKATY (WSPÓLNE) =====\n")
        for h in sorted(common):
            f.write(f"\nSHA256: {h}\n")
            for p in hashes_a[h]:
                f.write(f"A: {p}\n")
            for p in hashes_b[h]:
                f.write(f"B: {p}\n")

    # =========================
    # NIEDUPLIKATY – A
    # =========================
    with open(OUT_ONLY_A, "w", encoding="utf-8") as f:
        f.write("===== NIEDUPLIKATY – TYLKO FOLDER A =====\n")
        for h in sorted(only_a):
            for p in hashes_a[h]:
                f.write(str(p) + "\n")

    # =========================
    # NIEDUPLIKATY – B
    # =========================
    with open(OUT_ONLY_B, "w", encoding="utf-8") as f:
        f.write("===== NIEDUPLIKATY – TYLKO FOLDER B =====\n")
        for h in sorted(only_b):
            for p in hashes_b[h]:
                f.write(str(p) + "\n")

    print("✅ Gotowe!")
    print(f"📄 {OUT_DUP}")
    print(f"📄 {OUT_ONLY_A}")
    print(f"📄 {OUT_ONLY_B}")

if __name__ == "__main__":
    main()
