# import pandas as pd

# # === Konfiguracja ===
# plik = "shumee optima vs base.xlsx"
# arkusz1 = "optima"
# arkusz2 = "excel-wiecej"
# kolumna_klucz = "Nazwa"   # kolumna po której porównujesz

# # === Wczytaj oba arkusze ===
# df2 = pd.read_excel(plik, sheet_name=arkusz1)
# df1 = pd.read_excel(plik, sheet_name=arkusz2)

# # === Znajdź różnice po nazwie kolumny ===
# roznice = df1[~df1["Dokument"].isin(df2["Numer dokumentu"])]
# tylko_w_1 = df1[~df1["Dokument"].isin(df2["Numer dokumentu"])]
# tylko_w_2 = df2[~df2["Numer dokumentu"].isin(df1["Dokument"])]
# roznica = pd.concat([tylko_w_1, tylko_w_2], ignore_index=True)

import pandas as pd

plik = "shumee optima vs base.xlsx"
arkusz1 = "optima"
arkusz2 = "excel-wiecej"
# kolumna_klucz = "Nazwa"

# Wczytaj dane
df2 = pd.read_excel(plik, sheet_name=arkusz1)
df1 = pd.read_excel(plik, sheet_name=arkusz2)
kolumna_klucz = "Numer dokumentu"

# Oczyść dane (usuń spacje, zamień NaN na pusty tekst, na stringi)
df1[kolumna_klucz] = df1[kolumna_klucz].astype(str).str.strip()
df2[kolumna_klucz] = df2[kolumna_klucz].astype(str).str.strip()

# Znajdź tylko te, które NIE występują w drugim arkuszu

df2[kolumna_klucz] = df2[kolumna_klucz].astype(str).str.strip()

# Znajdź tylko te, które NIE występują w drugim arkuszu
roznice = df1[~df1[kolumna_klucz].isin(df2[kolumna_klucz])]

# Pokaż tylko nieistniejące wartości
if roznice.empty:
    print("✅ Wszystkie wartości z Arkusza1 występują w Arkuszu2.")
else:
    print("❌ Wiersze z Arkusza1, których brak w Arkuszu2:")
    print(roznice[[kolumna_klucz]])
