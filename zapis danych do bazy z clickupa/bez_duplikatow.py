from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv

# Tworzenie silnika SQLAlchemy dla PostgreSQL
load_dotenv()
engine = create_engine("DB_URL")

# Wczytaj dane z obu tabel — używamy silnika SQLAlchemy, nie osobnego `conn`
df_merchanci = pd.read_sql("SELECT id, nip, nazwa, status FROM merchanci", engine)
df_staging = pd.read_sql("SELECT id, nip, nazwa, status FROM merchanci_staging", engine)

# Połącz dane
df_combined = pd.concat([df_merchanci, df_staging], ignore_index=True)

# Usuń duplikaty — np. na podstawie 'nip'
df_cleaned = df_combined.drop_duplicates(subset='nip', keep='first')

# Zapisz dane do tabeli — nadpisując istniejącą
df_cleaned.to_sql('merchanci', engine, if_exists='replace', index=False)

engine.dispose()
