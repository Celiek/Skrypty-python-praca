import sys
from lxml import etree

def extract_paths(elem, prefix=""):
    """Rekurencyjne pobieranie pełnych ścieżek tagów."""
    paths = set()
    tag = etree.QName(elem).localname
    current_path = f"{prefix}/{tag}"
    paths.add(current_path)

    for child in elem:
        paths |= extract_paths(child, current_path)

    return paths

# ==== Podmień nazwy plików ====
xml_user = r"pkik_final.xml"
xml_optima = r"C:\Users\DELL\Downloads\great temu korekty zagranica.xml"

# ==== Parsowanie =====
tree_user = etree.parse(xml_user)
tree_optima = etree.parse(xml_optima)

root_user = tree_user.getroot()
root_optima = tree_optima.getroot()

# ==== Wyodrębnienie ścieżek ====
paths_user = extract_paths(root_user)
paths_optima = extract_paths(root_optima)

# ==== Porównanie ====
missing = paths_optima - paths_user       # czego brakuje?
extra = paths_user - paths_optima         # co jest nadmiarowe?

print("\n=== TAGI, KTÓRYCH BRAKUJE W TWOIM XML (a są w Optimie) ===")
for p in sorted(missing):
    print(p)

print("\n=== TAGI NADMIAROWE (są u Ciebie, a nie ma ich w Optimie) ===")
for p in sorted(extra):
    print(p)

print("\n=== PODSUMOWANIE ===")
print(f"Liczba tagów w Optimie: {len(paths_optima)}")
print(f"Liczba tagów w Twoim pliku: {len(paths_user)}")
print(f"Brakuje: {len(missing)}")
print(f"Nadmiarowe: {len(extra)}")
