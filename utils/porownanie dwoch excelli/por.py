import pandas as pd

# ============================
# KONFIGURACJA
# ============================

plik1 = r"C:\Users\DELL\Downloads\raport_9730408592.xlsx"
plik2 = r"C:\Users\DELL\Downloads\shumee_10_2025.xlsx"
plik_wynikowy = "wynik_porownania.xlsx"
kolumna_klucz = 'Numer dokumentu'

# NAZWA KOLUMNY DO PORÓWNANIA:
kolumna_klucz = "Numer dokumentu"   # <-- wpisz na twardo dokładną nazwę


# ============================
# PROGRAM
# ============================

def main():

    # 1. Wczytanie dwóch plików z nagłówkami w pierwszym wierszu
    df1 = pd.read_excel(plik1)
    df2 = pd.read_excel(plik2)

    # 2. Walidacja – czy kolumna istnieje?
    if 'Numer dokumentu' not in df1.columns:
        raise ValueError(f"Kolumna '{kolumna_klucz}' nie istnieje w pliku 1! "
                         f"Kolumny: {df1.columns.tolist()}")

    if 'Numer dokumentu' not in df2.columns:
        raise ValueError(f"Kolumna '{kolumna_klucz}' nie istnieje w pliku 2! "
                         f"Kolumny: {df2.columns.tolist()}")

    # 3. Rekordy z pliku 1, których nie ma w pliku 2
    brak_w_pliku2 = df1[~df1[kolumna_klucz].isin(df2[kolumna_klucz])]

    # 4. Rekordy z pliku 2, których nie ma w pliku 1
    brak_w_pliku1 = df2[~df2[kolumna_klucz].isin(df1[kolumna_klucz])]

    # 5. Zapis wyników do Excela
    with pd.ExcelWriter(plik_wynikowy, engine='openpyxl') as writer:
        brak_w_pliku2.to_excel(writer, sheet_name="brak_w_pliku2", index=False)
        brak_w_pliku1.to_excel(writer, sheet_name="brak_w_pliku1", index=False)

    print("\n✔ GOTOWE!")
    print("Wynik zapisany w:", plik_wynikowy)
    print(f"- Brakujących w pliku 2: {len(brak_w_pliku2)}")
    print(f"- Brakujących w pliku 1: {len(brak_w_pliku1)}")


if __name__ == "__main__":
    main()
