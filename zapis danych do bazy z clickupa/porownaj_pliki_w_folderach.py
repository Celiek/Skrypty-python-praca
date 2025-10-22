import os

def list_unique_folders(folder1, folder2):
    # Pobierz listę folderów w folderze1
    folders1 = {f for f in os.listdir(folder1) if os.path.isdir(os.path.join(folder1, f))}
    # Pobierz listę folderów w folderze2
    folders2 = {f for f in os.listdir(folder2) if os.path.isdir(os.path.join(folder2, f))}
    
    # Znajdź foldery, które są w folderze1, ale nie w folderze2
    unique_folders = folders1 - folders2
    
    return sorted(unique_folders)

# Przykład użycia
folder1_path = r'C:\Users\DELL\Documents\FAKTURY\great posortowane 14_10_25\GREATSTORE'
folder2_path = r'C:\Users\DELL\Documents\FAKTURY\shumee_sm'

result = list_unique_folders(folder1_path, folder2_path)
print("Foldery obecne w folderze1, ale nie w folderze2:")
for folder in result:
    print(folder)