import pandas as pd

df = pd.read_excel("tescikkk.xlsx")

# upewnij się, że kolumna Brutto to liczby
df["Brutto"] = pd.to_numeric(df["Brutto"], errors="coerce")

# normalizujemy NIP do samych cyfr
df["NIP_clean"] = df["NIP"].astype(str).str.replace(r"\D", "", regex=True)

# policz dokładnie dla interesującego NIP-u
mask = df["NIP_clean"] == "6560002322"
print("Wiersze dla 6560002322:")
print(df.loc[mask, ["Numer dokumentu", "Data wpływu", "Brutto"]])

print("Suma Brutto dla 6560002322 =", df.loc[mask, "Brutto"].sum())

print("Ilość wierszy dla NIP 6560002322 =", mask.sum())
print("Suma Brutto (wszystkie wiersze) =", df.loc[mask, "Brutto"].sum())
print("Suma Brutto (tylko dodatnie)     =", df.loc[mask & (df["Brutto"]>0), "Brutto"].sum())
print("Suma Brutto (tylko ujemne)       =", df.loc[mask & (df["Brutto"]<0), "Brutto"].sum())
