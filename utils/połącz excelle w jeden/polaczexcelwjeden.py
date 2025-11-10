import pandas as pd
from pathlib import Path

# === KONFIGURACJA ===
FOLDER = r"C:\Users\DELL\Documents\Skrypty\Skrypty-python-praca\kontrahenci 3 procent\raporty_xlsx\greatstore\2025-11-10"       # folder z plikami XLSX
OUTPUT = r"C:\Users\DELL\Documents\Skrypty\Skrypty-python-praca\kontrahenci 3 procent\raporty_xlsx\greatstore\2025-11-10\polaczone_raporty.xlsx"       # wynikowy plik

NAGLOWKI = {
    "NIP", "nip",
    "Data", "data",
    "Kwota", "kwota",
    "Numer", "numer",
    "Numer dokumentu", "dokument",
    "Kontrahent", "kontrahenci"
}

def lacz_bez_naglowkow(folder: str, output: str):
    folder_path = Path(folder)
    pliki = sorted(folder_path.glob("*.xlsx"))

    if not pliki:
        print("❌ Brak plików XLSX.")
        return

    frames = []

    for p in pliki:
        df = pd.read_excel(p, header=None, dtype=str)  # wczytujemy bez nagłówków

        # normalizacja – usunięcie spacji, wielkości liter itp.
        df = df.applymap(lambda x: str(x).strip() if pd.notna(x) else x)

        # usunięcie wierszy które są "nagłówkami"
        df = df[~df[0].isin(NAGLOWKI)]

        # usunięcie pustych wierszy
        df = df[df[0].notna() & (df[0] != "")]

        frames.append(df)

    # łączenie
    wynik = pd.concat(frames, ignore_index=True)

    # zapis
    wynik.to_excel(output, index=False, header=False)

    print(f"✅ Połączono {len(pliki)} plików i zapisano do: {output}")

# Uruchomienie
lacz_bez_naglowkow(FOLDER, OUTPUT)