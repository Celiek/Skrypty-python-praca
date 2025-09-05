import json

# Wczytaj dane z pliku
with open("zbiorczy.json", "r", encoding="utf-8") as f:
    dane = json.load(f)

unikalne = []
widziane = set()

for rekord in dane:
    nip = rekord.get("nip")
    konto = rekord.get("nr_konta")
    klucz = (nip, konto)

    if klucz not in widziane:
        widziane.add(klucz)
        unikalne.append(rekord)

# Zapisz dane bez duplikatów do nowego pliku
with open("zbiorczy_bez_duplikatow.json", "w", encoding="utf-8") as f:
    json.dump(unikalne, f, ensure_ascii=False, indent=2)
print(f"✔ Usunięto duplikaty. Zapisano {len(unikalne)} unikalnych rekordów.")