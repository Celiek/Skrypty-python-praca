import pandas as pd
from sqlalchemy import create_engine

# Połączenie z bazą danych (np. MySQL, PostgreSQL, SQLite)
engine = create_engine(f"postgresql+psycopg2://gabriel:lhj7r7nk7e@localhost:5432/merchanci")

# Pobranie danych z tabeli
df = pd.read_sql("SELECT nip,nazwa FROM merchanci ", engine)
df['nip'] = df['nip'].astype('Int64')

df2 = pd.read_sql("SELECT nip,nazwa FROM merchanci_staging",engine)
df2['nip'] = df['nip'].astype('Int64')
# pattern = r'[^\w\s]'

# df['adres'] = df['adres'].replace(pattern,' ',regex=True)
print("Dane z merchanci")
print(df.head())

print("Dane z merchanci")
print(df2.head())

# Zapis do pliku Excel
df.to_excel("dane z merchanci.xlsx", index=False)
df2.to_excel("dane z merchanci_staging.xlsx",index = False)