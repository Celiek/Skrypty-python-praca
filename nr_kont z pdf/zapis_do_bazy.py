import json
import psycopg2


db_config = {
    "host": "localhost",
    "port": 5432,
    "dbname": "merchanci",
    "user": "gabriel",
    "password": "lhj7r7nk7e"
}

conn = psycopg2.connect(**db_config)
cursor = conn.cursor()


with open("zbiorczy_bez_duplikatow.json","r", encoding="utf-8") as f:
    dane = json.load(f)

for rekord in dane:
    nip = rekord.get("nip")
    nr_konta = rekord.get("nr_konta")

    if nip and nr_konta:
        cursor.execute("""
            UPDATE merchanci SET nr_konta = %s WHERE nip = %s
        """, (nr_konta, nip))
    else:
        print(f"⚠️ Pominięto rekord bez wymaganych danych: {rekord}")

conn.commit()
cursor.close()
conn.close()

print("dane są juz w bazie danych")
