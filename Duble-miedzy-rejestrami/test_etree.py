import xml.etree.ElementTree as ET
from collections import Counter
import re

# KSIG.
# k1

XML_IN = r"C:\Users\DELL\Downloads\GREAT TEMU FV POLSKA (2) (1).xml"
NS = {"c": "http://www.comarch.pl/cdn/optima/offline"}

def print_tree(elem, level=0):
    indent = "    " * level
    tag = elem.tag.split('}')[-1]   # usunięcie namespace
    print(f"{indent}<{tag}>")

    for child in elem:
        print_tree(child, level + 1)


def main():
    print(" Odczytuję strukturę XML...\n")
    
    tree = ET.parse(XML_IN)
    root = tree.getroot()

    print_tree(root)


if __name__ == "__main__":
    main()
