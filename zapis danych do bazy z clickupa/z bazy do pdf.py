from openpyxl import Workbook
import psycopg2

# db_config = psycopg2.connect(
#     host="localhost",
#     database="merchanci",
#     user="gabriel",
#     password="lhj7r7nk7e"
# )


db_config = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'merchanci',
    'user': 'gabriel',
    'password': 'lhj7r7nk7e'
}


query = "SELECT * FROM merchanci"

try:
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    wb = Workbook()
    ws = wb.active
    ws.title = "Dane z PostgreSQL"


    ws.append(columns)


    for row in rows:
        ws.append(row)


    wb.save("dane_postgresql.xlsx")
    print("Dane zostały zapisane do pliku dane_postgresql.xlsx")

except Exception as e:
    print("Wystąpił błąd:", e)

finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
