import pandas as pd

# Wczytanie pliku
plik = r"C:\Users\DELL\Documents\Skrypty\Skrypty-python-praca\kontrahenci 3 procent\test_3_procent_listopad.xlsx"
df = pd.read_excel(plik)

# ---- Oczyszczenie NIP-u (opcjonalne, ale polecam!) ----
df["NIP"] = (
    df["NIP"]
    .astype(str)                  # na string
    .str.upper()                  # duże litery
    .str.replace(r"\D", "", regex=True)   # usuń wszystko co nie jest cyfrą
)

# ---- Pobranie unikalnych NIP-ów ----
unique_nipy = df["NIP"].dropna().unique()

# Wypisanie
print("Wszystkie unikalne NIP-y:")
for nip in unique_nipy:
    print(nip)

print("Liczba unikalnych NIP-ów:", len(unique_nipy))
