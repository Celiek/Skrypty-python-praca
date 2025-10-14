import pandas as pd

# porównuje dwa pliki
# na podstawie jednej kolumny

# === Wczytaj pliki ===
brakujace = pd.read_excel("brakujace_w_b.xlsx")
plik_c = pd.read_excel("great przelewy 07.10.25 po deduplikacji2 (1).xlsx")

# === Normalizacja kolumn ===
brakujace.columns = brakujace.columns.str.strip().str.lower()
plik_c.columns = plik_c.columns.str.strip().str.lower()

# === Nazwa kolumny, po której porównujemy (np. 'a') ===
col = 'nip'

# === Sprawdzenie: które z wartości w pliku brakującym występują w pliku C ===
w_c = brakujace[brakujace[col].isin(plik_c[col])]

# === Oraz które NIE występują w pliku C (dla porównania) ===
nie_w_c = brakujace[~brakujace[col].isin(plik_c[col])]

# === Zapisz wyniki ===
w_c.to_excel("wystepuja_w_c.xlsx", index=False)
nie_w_c.to_excel("nie_wystepuja_w_c.xlsx", index=False)

wspolne = plik_c.merge(brakujace[[col]], on=col, how='inner')

# === Zapis do Excela ===
wspolne.to_excel("pelne_wiersze_z_c.xlsx", index=False)

print(f"✅ Zapisano {len(wspolne)} pasujących wierszy do pliku 'pelne_wiersze_z_c.xlsx'")

print(f"✅ Zapisano {len(w_c)} wierszy, które występują w pliku C")
print(f"⚠️ Zapisano {len(nie_w_c)} wierszy, których nie ma w pliku C")
