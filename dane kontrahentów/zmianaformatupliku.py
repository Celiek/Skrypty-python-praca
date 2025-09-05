# Create a reusable Python script to detect encoding/delimiter and convert CSV/TXT to a target encoding/delimiter.
from pathlib import Path



"""
convert_csv_encoding.py

Prosty skrypt do:
- wykrycia kodowania (BOM + heurystyki dla UTF-16/UTF-8/CP852/CP1250/ISO-8859-2 itd.),
- wykrycia separatora (średnik, przecinek, tab, pionowa kreska),
- przepisania pliku do wybranego kodowania i separatora.

Użycie:
  python convert_csv_encoding.py INPUT.csv [-o OUTPUT.csv]
      [--from-enc auto|utf-8|utf-16le|utf-16be|cp1250|iso-8859-2|cp852|cp1252|latin1]
      [--to-enc utf-8] [--in-delim auto|;|,|tab|pipe] [--out-delim same|;|,|tab|pipe]
      [--crlf] [--preview]

Przykłady:
  # Najczęstsze: auto-detekcja i zapis do UTF-8 z CRLF (Windows)
  python convert_csv_encoding.py "historia.csv" -o "historia_utf8.csv" --crlf

  # Wymuszenie CP852 -> UTF-8, zamiana separatora na przecinek
  python convert_csv_encoding.py "historia.csv" -o "historia_utf8.csv" --from-enc cp852 --out-delim ,

  # Wymuszenie UTF-16LE -> UTF-8, separator bez zmian
  python convert_csv_encoding.py "bank.txt" -o "bank_utf8.csv" --from-enc utf-16le

Autor: ChatGPT
"""
import sys, csv, io, argparse, re
from pathlib import Path
from typing import Optional

# ---------- Wykrywanie kodowania ----------

def looks_utf16_heuristic(b: bytes) -> Optional[str]:
    if len(b) < 6:  # za mało danych
        return None
    even_zeros = sum(1 for i in range(0, len(b), 2) if b[i] == 0)
    odd_zeros  = sum(1 for i in range(1, len(b), 2) if b[i] == 0)
    ratio_even = even_zeros / max(1, len(b)//2)
    ratio_odd  = odd_zeros / max(1, len(b)//2)
    if ratio_even > 0.30 and ratio_odd < 0.05:
        return "utf-16le"
    if ratio_odd > 0.30 and ratio_even < 0.05:
        return "utf-16be"
    return None

def detect_encoding(data: bytes) -> str:
    # BOM
    if data.startswith(b"\xFF\xFE"):
        return "utf-16le"
    if data.startswith(b"\xFE\xFF"):
        return "utf-16be"
    if data.startswith(b"\xEF\xBB\xBF"):
        return "utf-8-sig"

    # Heurystyka UTF-16 bez BOM
    guess = looks_utf16_heuristic(data[:4096])
    if guess:
        return guess

    # Spróbuj listę popularnych
    candidates = ["utf-8","cp852","cp1250","iso-8859-2","cp1252","latin1"]
    best = None
    best_score = -10**9
    for enc in candidates:
        try:
            txt = data.decode(enc)  # strict
            repl = 0
        except UnicodeDecodeError:
            txt = data.decode(enc, errors="replace")
            repl = txt.count("\uFFFD")
        # prosty scoring: polskie litery + separator + mało zastąpień
        pl = sum(txt.count(ch) for ch in "ąćęłńóśżźĄĆĘŁŃÓŚŻŹ")
        score = (pl * 5) - (repl * 50) + txt[:8000].count(";") + txt[:8000].count(",")
        if score > best_score:
            best_score = score
            best = enc
    return best or "utf-8"  # fallback

# ---------- Wykrywanie separatora ----------

def detect_delimiter(text: str) -> str:
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if not lines:
        return ","
    sample = lines[0]
    counts = {d: sample.count(d) for d in [",",";","\t","|"]}
    # wybierz separator występujący najczęściej
    return max(counts, key=counts.get)

def map_delim(name: str, same_as: Optional[str]=None) -> str:
    if name == "same":
        if same_as is None:
            raise ValueError("--out-delim same bez znanego wejściowego separatora")
        return same_as
    if name in {",",";","|","\t"}:
        return name
    if name == "tab":
        return "\t"
    if name == "pipe":
        return "|"
    if name == "auto":
        raise ValueError("map_delim('auto') wymaga wcześniejszej detekcji")
    raise ValueError(f"Nieznany separator: {name!r}")

# ---------- Konwersja ----------

def recode_file(input_path: Path, output_path: Path,
                from_enc: str="auto", to_enc: str="utf-8",
                in_delim: str="auto", out_delim: str="same",
                crlf: bool=False, preview: bool=False) -> None:
    data = input_path.read_bytes()
    enc = detect_encoding(data) if from_enc == "auto" else from_enc
    try:
        text = data.decode(enc)  # strict
    except UnicodeDecodeError:
        # czasem dane mają drobne błędy – w ostateczności zastąp problematyczne znaki
        text = data.decode(enc, errors="replace")

    if in_delim == "auto":
        delim_in = detect_delimiter(text)
    else:
        delim_in = map_delim(in_delim)

    delim_out = map_delim(out_delim, same_as=delim_in)

    # Wczytaj jako CSV (bez zmiany typów — wszystko jako tekst)
    reader = csv.reader(io.StringIO(text), delimiter=delim_in)
    rows = list(reader)

    if preview:
        print(f"[INFO] Wykryte kodowanie: {enc}")
        print(f"[INFO] Wejściowy separator: {repr(delim_in)}")
        print(f"[INFO] Wyjściowy separator: {repr(delim_out)}")
        print("[INFO] Podgląd pierwszych 3 wierszy:")
        for r in rows[:3]:
            print("  ", r)

    # Zapis
    newline = "\r\n" if crlf else "\n"
    with output_path.open("w", encoding=to_enc, newline="") as f:
        w = csv.writer(f, delimiter=delim_out, lineterminator=newline, quoting=csv.QUOTE_MINIMAL)
        w.writerows(rows)

def main(argv=None):
    ap = argparse.ArgumentParser(description="Konwersja CSV/TXT: kodowanie i separator.")
    ap.add_argument("input", help="Ścieżka wejściowa (CSV/TXT)")
    ap.add_argument("-o","--output", help="Ścieżka wyjściowa (domyślnie: dodaje _utf8.csv)")
    ap.add_argument("--from-enc", default="auto", help="Źródłowe kodowanie (auto|utf-8|utf-16le|utf-16be|cp1250|iso-8859-2|cp852|cp1252|latin1)")
    ap.add_argument("--to-enc", default="utf-8", help="Docelowe kodowanie (np. utf-8, cp1250)")
    ap.add_argument("--in-delim", default="auto", help="Separator wejściowy (auto|;|,|tab|pipe)")
    ap.add_argument("--out-delim", default="same", help="Separator wyjściowy (same|;|,|tab|pipe)")
    ap.add_argument("--crlf", action="store_true", help="Wymuś zakończenia linii CRLF (Windows)")
    ap.add_argument("--preview", action="store_true", help="Pokaż wykryte ustawienia i podgląd")
    args = ap.parse_args(argv)

    inp = Path(args.input)
    if not inp.exists():
        sys.exit(f"Nie znaleziono pliku: {inp}")

    out = Path(args.output) if args.output else inp.with_suffix(".utf8.csv")

    recode_file(
        input_path=inp,
        output_path=out,
        from_enc=args.from_enc.lower(),
        to_enc=args.to_enc.lower(),
        in_delim=args.in_delim.lower(),
        out_delim=args.out_delim.lower(),
        crlf=args.crlf,
        preview=args.preview
    )
    print(f"Zapisano: {out}")

if __name__ == "__main__":
    main()
