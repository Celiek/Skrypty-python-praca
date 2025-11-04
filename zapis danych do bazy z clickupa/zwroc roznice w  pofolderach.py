import os
import re

# używane do spradzania jakie podfoldery z fakturami znajdują się w pliku shumme a jakie w folderze great

def clean_name(name):
    """Usuwa liczby (0–999) i podkreślenie z początku nazwy folderu."""
    return re.sub(r'^\d{1,3}_', '', name)

def list_unique_folders(folder1, folder2):
    # Pobierz listę nazw folderów (oczyszczonych)
    folders1 = {clean_name(f) for f in os.listdir(folder1) if os.path.isdir(os.path.join(folder1, f))}
    folders2 = {clean_name(f) for f in os.listdir(folder2) if os.path.isdir(os.path.join(folder2, f))}
    
    # Znajdź foldery, które są w folderze1, ale nie w folderze2
    unique_folders = folders1 - folders2
    
    return sorted(unique_folders)

# --- Przykład użycia ---
folder1_path = r'C:\Users\DELL\Documents\FAKTURY\great_sm'
folder2_path = r'C:\Users\DELL\Documents\FAKTURY\shumee_sm'

result = list_unique_folders(folder1_path, folder2_path)

print("📁 Foldery obecne w folderze1, ale nie w folderze2:")
for folder in result:
    print(" -", folder)
