import re
import glob
import pandas as pd
from pathlib import Path
import argparse

ENCODING = "iso8859_2"  # pliki ELIXIR zwykle w ISO-8859-2

def parse_elixir_file(path: Path):
    rows = []
    with open(path, "r", encoding=ENCODING, errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line.startswith("110,"):
                continue
            cols = line.split(",")
            if len(cols) < 9:
                continue
            # kolumny wg Twojego generatora:
            # 0: typ(110), 1: data, 2: kwota, 5: rach_zlec, 6: rach_kontr,
            # 7: nazwa/adres zleceniodawcy, 8: nazwa/adres kontrahenta, 11/12: szczegóły
            nazwa_i_adres_kontr = cols[8].strip()
            # nazwa bywa w formacie "NAZWA|ADRES..." — bierzemy samą nazwę (do 1. '|')
            nazwa = nazwa_i_adres_kontr.split("|", 1)[0].strip()

            # szczegóły zwykle w kol. 12 (indeks 11); czasem w 13 jeśli są puste pola
            szczegoly = ""
            if len(cols) >= 12:
                szczegoly = cols[11].strip().strip('"')
            if "/IDC/" not in szczegoly and len(cols) >= 13:
                szczegoly = cols[12].strip().strip('"')

            m_nip = re.search(r"/IDC/(\d{10})", szczegoly)
            nip = m_nip.group(1) if m_nip else ""

            if nip:  # zapisuj tylko gdy NIP jest obecny
                rows.append({"plik_elixir": path.name, "NIP": nip, "Nazwa": nazwa})
    return rows

def main():
    ap = argparse.ArgumentParser(description="Zbiorczy Excel z NIP i nazw z plików ELIXIR.")
    ap.add_argument("--folder", default=".", help="Folder z plikami *_przelewy_w_*.txt (domyślnie bieżący).")
    ap.add_argument("--pattern", default="*_przelewy_w_*.txt", help="Wzorzec nazw plików (glob).")
    ap.add_argument("--out", default="kontrahenci_elixir.xlsx", help="Nazwa pliku wyjściowego .xlsx")
    args = ap.parse_args()

    base = Path(args.folder)
    files = sorted(base.glob(args.pattern))
    if not files:
        print(f"[ERR] Brak plików dla wzorca: {base / args.pattern}")
        return

    all_rows = []
    for p in files:
        all_rows.extend(parse_elixir_file(p))

    if not all_rows:
        print("[WARN] Nie znaleziono żadnych rekordów z NIP w plikach ELIXIR.")
        return

    df_all = pd.DataFrame(all_rows, columns=["plik_elixir", "NIP", "Nazwa"])

    # wariant unikalny (po NIP, a nazwę bierzemy z pierwszego wystąpienia)
    df_unique = (
        df_all.sort_values(["NIP", "plik_elixir"])
              .drop_duplicates(subset=["NIP"], keep="first")[["NIP", "Nazwa"]]
              .reset_index(drop=True)
    )

    with pd.ExcelWriter(args.out, engine="xlsxwriter") as xw:
        df_all.to_excel(xw, index=False, sheet_name="wszystko")
        df_unique.to_excel(xw, index=False, sheet_name="unikalne")

    print(f"[OK] Zapisano: {args.out}")
    print(f"    - arkusz 'wszystko': {len(df_all)} wierszy")
    print(f"    - arkusz 'unikalne': {len(df_unique)} NIP-ów")

if __name__ == "__main__":
    main()
