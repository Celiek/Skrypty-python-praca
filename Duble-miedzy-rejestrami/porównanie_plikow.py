import pandas as pd

# sprwadza jakie dane występują w jednym pliku (merchanci staging i merchanci merchanci z bazy danych)

a = pd.read_excel("dane z merchanci.xlsx")
b = pd.read_excel("dane z merchanci_staging.xlsx")


a.columns = a.columns.str.strip().str.lower()
b.columns = b.columns.str.strip().str.lower()

col = 'nip'

nie_w_b = a[~a[col].isin(b[col])]

print("Wartości z pliku A, których nie ma w pliku B:")
print(nie_w_b)

nie_w_b.to_excel("nadmiarowe dane.xlsx", index=False)

