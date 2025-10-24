import os
import re
import hashlib
import fitz
from concurrent.futures import ThreadPoolExecutor, as_completed
from argparse import ArgumentParser

def fast_extract_text(pdf_path):
    """Szybki odczyt tekstu z pliku PDF."""
    try:
        with fitz.open(pdf_path) as doc:
            return ''.join(page.get_text("text") for page in doc)
    except Exception as e:
        print(f"[BŁĄD] Nie można odczytać pliku {pdf_path}: {e}")
        return None


def przetworz_pdf(pdf_path, wzorzec, ignore_case):
    """Zwraca True jeśli wzorzec znaleziony w pliku PDF."""
    tekst = fast_extract_text(pdf_path)
    if not tekst:
        return None, None

    flags = re.IGNORECASE if ignore_case else 0
    dopasowanie = re.search(wzorzec, tekst, flags=flags)
    text_hash = hashlib.md5(tekst.encode("utf-8")).hexdigest()
    return dopasowanie is not None, text_hash


def przeszukaj_folder(folder, wzorzec, ignore_case=False):
    """Przeszukuje wszystkie PDF-y w folderze."""
    pdfy = [os.path.join(root, f)
            for root, _, files in os.walk(folder)
            for f in files if f.lower().endswith(".pdf")]

    znalezione = []
    duplikaty = set()
    unikalne = set()

    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(przetworz_pdf, p, wzorzec, ignore_case): p for p in pdfy}
        for future in as_completed(futures):
            pdf_path = futures[future]
            try:
                znaleziony, text_hash = future.result()
                if text_hash:
                    if text_hash in unikalne:
                        duplikaty.add(pdf_path)
                    else:
                        unikalne.add(text_hash)

                if znaleziony:
                    znalezione.append(pdf_path)
                    print(f"✅ {pdf_path}")
                else:
                    print(f"❌ {pdf_path}")
            except Exception as e:
                print(f"[BŁĄD] {pdf_path}: {e}")

    print("\n=== PODSUMOWANIE ===")
    print(f"Plików przeszukanych : {len(pdfy)}")
    print(f"Znalezionych dopasowań : {len(znalezione)}")
    print(f"Duplikatów treści : {len(duplikaty)}")
    return znalezione


if __name__ == "__main__":
    parser = ArgumentParser(description="Przeszukuje pliki PDF pod kątem tekstu.")
    parser.add_argument("folder", help="Ścieżka do folderu z PDF-ami")
    parser.add_argument("tekst", help="Tekst lub wyrażenie regularne do wyszukania")
    parser.add_argument("--ignore-case", action="store_true", help="Ignoruj wielkość liter")
    parser.add_argument("--regex", action="store_true", help="Traktuj wzorzec jako regex (domyślnie zwykły tekst)")

    args = parser.parse_args()

    wzorzec = args.tekst if args.regex else re.escape(args.tekst)

    print(f"🔍 Szukanie wzorca: '{args.tekst}' w folderze: {args.folder}")
    znalezione = przeszukaj_folder(args.folder, wzorzec, args.ignore_case)

    if znalezione:
        print("\n📄 Pliki zawierające wzorzec:")
        for path in znalezione:
            print(" -", path)
    else:
        print("\n❌ Nie znaleziono dopasowań.")
