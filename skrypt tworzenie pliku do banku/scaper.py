import requests
import pandas as pd
import time

# pobiera podstawowe dane kontrahentów z clickupa (api) i zapisuje
# do pliku xlsx

API_TOKEN =  'pk_100546826_B534EGYUV8ZGS9L5YX5PEG7W2S4SPF31'

# 🆔 ID listy z ClickUp
LIST_ID = '901507790851'

# 🔖 Nagłówki autoryzacji
headers = {
    'Authorization': API_TOKEN
}

# 📦 Lista do przechowywania wszystkich zadań
all_tasks = []
page = 0

# 🔁 Pętla paginacji
while True:
    print(f"📄 Pobieram stronę {page}...")
    url = f'https://api.clickup.com/api/v2/list/{LIST_ID}/task?page={page}'

    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()
        tasks = data.get('tasks', [])
    except requests.exceptions.RequestException as e:
        print(f"❌ Błąd przy pobieraniu strony {page}: {e}")
        break

    if not tasks:
        print("✅ Wszystkie strony zostały pobrane.")
        break

    all_tasks.extend(tasks)
    page += 1
    time.sleep(1)  # ⏳ opcjonalne opóźnienie

# 📊 Przetwarzanie zadań
task_data = []

for i, task in enumerate(all_tasks, start=1):
    print(f"🔄 Przetwarzam zadanie {i}/{len(all_tasks)}")

    try:
        task_info = {
            'ID': task.get('id'),
            'Nazwa': task.get('name'),
            'Status': task.get('status', {}).get('status'),
            'Opis': task.get('description'),
            'Data utworzenia': pd.to_datetime(int(task.get('date_created', 0)), unit='ms'),
            'Data aktualizacji': pd.to_datetime(int(task.get('date_updated', 0)), unit='ms'),
            'Przypisani': ', '.join([user.get('username', '') for user in task.get('assignees', [])])
        }

        # 🔍 Dodaj custom fields
        for field in task.get('custom_fields', []):
            name = field.get('name')
            value = field.get('value')

            if isinstance(value, dict) and 'name' in value:
                value = value['name']
            elif isinstance(value, list):
                value = ', '.join(str(v) for v in value)

            task_info[name] = value

        task_data.append(task_info)

    except Exception as e:
        print(f"⚠️ Błąd przy przetwarzaniu zadania {task.get('id')}: {e}")
        continue

# 📤 Gotowe dane w DataFrame
df = pd.DataFrame(task_data)


# 📊 Zapis do pliku Excel
if task_data:
    df = pd.DataFrame(task_data)
    df.to_excel('clickup_tasks_clean(2).xlsx', index=False)
    print("✅ Dane zostały zapisane do pliku clickup_tasks_clean.xlsx")
else:
    print("⚠️ Brak danych do zapisania.")
