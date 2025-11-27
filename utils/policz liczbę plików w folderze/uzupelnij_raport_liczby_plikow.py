import pandas as pd
import os

# --- Pliki ---
PLIK_GLOWNY = "raport_liczba_plikow EXTRASTORE 26_11.2025.xlsx"
PLIK_DOPASOWANE = "dopasowane.xlsx"
WYJSCIE = "raport_uzupelniony_extrastore_26_11_2025.xlsx"

# --- Funkcja: wyciąganie ostatniego segmentu folderu ---
def extract_last_folder(path):
    """
    Z pełnej ścieżki zwraca ostatni segment (po ostatnim backslashu).
    Np.  C:\A\B\C\SHUMEE\ABC@example.com -> ABC@example.com
    """
    if not isinstance(path, str):
        return ""
    return os.path.basename(path)  # działa idealnie

# --- Wczytanie plików ---
df_main = pd.read_excel(PLIK_GLOWNY)
df_map = pd.read_excel(PLIK_DOPASOWANE)

# --- Dodaj kolumnę 'folder_clean' w obu plikach ---
df_main["folder_clean"] = df_main["Folder"].apply(extract_last_folder)
df_map["folder_clean"] = df_map["Folder"].apply(extract_last_folder)

# --- Merge / JOIN po folder_clean ---
df_joined = df_main.merge(
    df_map[["folder_clean", "nazwa_kontrahenta", "nip"]],
    on="folder_clean",
    how="left"
)

# --- BRAK zamiast NaN ---
df_joined["nazwa_kontrahenta"] = df_joined["nazwa_kontrahenta"].fillna("BRAK")
df_joined["nip"] = df_joined["nip"].fillna("BRAK")

# --- Usuwamy pomocniczą kolumnę ---
df_joined.drop(columns=["folder_clean"], inplace=True)

# --- Zapis ---
df_joined.to_excel(WYJSCIE, index=False)

print("✔ Raport uzupełniony zapisany jako:", WYJSCIE)
