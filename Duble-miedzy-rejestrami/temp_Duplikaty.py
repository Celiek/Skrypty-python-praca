import pandas as pd

# === konfiguracja ===
plik_we = r"C:\Users\DELL\Downloads\temu duplikaty pazdziernik.xlsx"   # Twój plik
plik_wy = "suma_duplikatow.xlsx"

# === wczytaj plik ===
df = pd.read_excel(plik_we)

# sprawdź dostępne kolumny
print("Kolumny w pliku:", df.columns.tolist())

# upewnij się, że wartości numeryczne są liczbami
df["Netto"] = pd.to_numeric(df["Netto"], errors="coerce").fillna(0)
df["Kwota VAT"] = pd.to_numeric(df["Kwota VAT"], errors="coerce").fillna(0)

# === grupuj po 'Dokument' i sumuj Netto + Kwota VAT ===
wynik = (
    df.groupby("Dokument", as_index=False)
      .agg({"Netto": "sum", "Kwota VAT": "sum", "Data wystawienia": "first"})
)

# dodaj kolumnę z liczbą wystąpień (ile razy dany dokument wystąpił)
licznik = df["Dokument"].value_counts().reset_index()
licznik.columns = ["Dokument", "Liczba wystąpień"]
wynik = wynik.merge(licznik, on="Dokument", how="left")

# zachowaj tylko te, które były duplikatami (więcej niż 1 wystąpienie)
wynik_duplikaty = wynik[wynik["Liczba wystąpień"] > 1]

# === zapisz wynik ===
wynik_duplikaty.to_excel(plik_wy, index=False)
print(f"✅ Wynik zapisano do: {plik_wy}")
print(f"📊 Znaleziono {len(wynik_duplikaty)} duplikatów zsumowanych.")
