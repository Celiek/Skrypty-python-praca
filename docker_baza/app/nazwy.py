import pandas as pd
import psycopg2
import os

# Wczytaj dane z pliku Excel
df = pd.read_excel("clickup_tasks_clean.xlsx")  # np. "nazwa_merchanci.xlsx"

# Połączenie z bazą danych
conn = psycopg2.connect(
    host=os.getenv("DB_HOST", "localhost"),
    port=os.getenv("DB_PORT", "5432"),
    dbname=os.getenv("DB_NAME", "merchanci"),
    user=os.getenv("DB_USER", "gabriel"),
    password=os.getenv("DB_PASSWORD", "lhj7r7nk7e")
)
cursor = conn.cursor()
print("✅ Połączono z bazą danych")

# Aktualizacja kolumny 'nazwa'
for _, row in df.iterrows():
    try:
        cursor.execute("""
            UPDATE MERCHANCI
            SET nazwa = %s
            WHERE id = %s
        """, (row['Nazwa'], row['ID']))
    except Exception as e:
        print(f"❌ Błąd przy aktualizacji: {e}")
        print(f"🧪 Wiersz: {row}")

conn.commit()
print("✅ Kolumna 'nazwa' została uzupełniona")