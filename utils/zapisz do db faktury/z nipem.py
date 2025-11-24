import os
import re
import tkinter as tk
from tkinter import filedialog, messagebox


# ---------------------------------------------------
# TWOJA FUNKCJA update_xml (bez zmian w logice)
# ---------------------------------------------------
def update_xml(input_file, output_file):
    with open(input_file, "r", encoding="utf-8") as file:
        xml_content = file.read()

    rejestr_pattern = re.compile(r"(<REJESTR_SPRZEDAZY_VAT>.*?</REJESTR_SPRZEDAZY_VAT>)", re.DOTALL)
    updated_kwota_count = 0

    country_name_to_code = {
        "Polska": "PL", "Niemcy": "DE", "Francja": "FR", "Włochy": "IT", "Hiszpania": "ES",
        "Czechy": "CZ", "Słowacja": "SK", "Węgry": "HU", "Austria": "AT", "Belgia": "BE",
        "Niderlandy": "NL", "Holandia": "NL", "Litwa": "LT", "Łotwa": "LV", "Estonia": "EE",
        "Rumunia": "RO", "Bułgaria": "BG", "Szwecja": "SE", "Dania": "DK", "Finlandia": "FI",
        "Norwegia": "NO", "Irlandia": "IE", "Portugalia": "PT", "Grecja": "GR",
        "Szwajcaria": "CH", "Wielka Brytania": "GB", "Zjednoczone Królestwo": "GB",
        "USA": "US", "Stany Zjednoczone": "US",
    }

    def process_section(match):
        nonlocal updated_kwota_count
        section = match.group(0)

        kod_match = re.search(r"<KOD_KRAJU_ODBIORCY><!\[CDATA\[(.*?)\]\]></KOD_KRAJU_ODBIORCY>", section)
        kraj_match = re.search(r"<KRAJ><!\[CDATA\[(.*?)\]\]></KRAJ>", section)

        kod_kraju_value = kod_match.group(1).strip() if kod_match else ""
        kraj_value = kraj_match.group(1).strip() if kraj_match else ""

        if kod_kraju_value == "PL" and kraj_value in country_name_to_code:
            actual_country_code = country_name_to_code[kraj_value]
            section = re.sub(
                r"<KOD_KRAJU_ODBIORCY><!\[CDATA\[.*?\]\]></KOD_KRAJU_ODBIORCY>",
                f"<KOD_KRAJU_ODBIORCY><![CDATA[{actual_country_code}]]></KOD_KRAJU_ODBIORCY>",
                section
            )
        else:
            actual_country_code = kod_kraju_value

        nip_kraj_pattern = re.compile(
            r"(<NIP_KRAJ><!\[CDATA\[.*?\]\]></NIP_KRAJ>)(\s*<NIP>\s*<!\[CDATA\[\]\]>\s*</NIP>)"
        )

        def replace_nip(m):
            nip_kraj_part = m.group(1)
            nip_part = m.group(2)
            if actual_country_code:
                return f"<NIP_KRAJ><![CDATA[{actual_country_code}]]></NIP_KRAJ>" + nip_part
            return m.group(0)

        section = nip_kraj_pattern.sub(replace_nip, section)

        section = re.sub(
            r"<FORMA_PLATNOSCI><!\[CDATA\[.*?\]\]></FORMA_PLATNOSCI>",
            "<FORMA_PLATNOSCI><![CDATA[przelew]]></FORMA_PLATNOSCI>", section
        )
        section = re.sub(
            r"<FORMA_PLATNOSCI_ID><!\[CDATA\[.*?\]\]></FORMA_PLATNOSCI_ID>",
            "<FORMA_PLATNOSCI_ID><![CDATA[98843769]]></FORMA_PLATNOSCI_ID>", section
        )
        section = re.sub(
            r"<FORMA_PLATNOSCI_PLAT><!\[CDATA\[.*?\]\]></FORMA_PLATNOSCI_PLAT>",
            "<FORMA_PLATNOSCI_PLAT><![CDATA[przelew]]></FORMA_PLATNOSCI_PLAT>", section
        )
        section = re.sub(
            r"<FORMA_PLATNOSCI_ID_PLAT><!\[CDATA\[.*?\]\]></FORMA_PLATNOSCI_ID_PLAT>",
            "<FORMA_PLATNOSCI_ID_PLAT><![CDATA[98843769]]></FORMA_PLATNOSCI_ID_PLAT>", section
        )

        pozycje = re.findall(
            r"<POZYCJA>.*?<NETTO>(.*?)</NETTO>.*?<VAT>(.*?)</VAT>.*?</POZYCJA>",
            section, re.DOTALL
        )
        netto_vat_sum = sum(
            float(n.replace(",", ".")) + float(v.replace(",", "."))
            for n, v in pozycje
        )

        id_match = re.search(r"<ID_ZRODLA><!\[CDATA\[(.*?)\]\]></ID_ZRODLA>", section)
        id_zrodla = id_match.group(1) if id_match else "[UNKNOWN]"

        kw_match = re.search(r"<KWOTA_PLAT>(.*?)</KWOTA_PLAT>", section)
        current_kw = float(kw_match.group(1).replace(",", ".")) if kw_match else None

        expected_kw = round(abs(netto_vat_sum), 2)

        if current_kw is None or abs(current_kw - expected_kw) >= 0.001:
            updated_kwota_count += 1
            section = re.sub(
                r"<KWOTA_PLAT>.*?</KWOTA_PLAT>",
                f"<KWOTA_PLAT>{expected_kw:.2f}</KWOTA_PLAT>",
                section
            )

        return section

    updated = rejestr_pattern.sub(process_section, xml_content)

    with open(output_file, "w", encoding="utf-8") as file:
        file.write(updated)

    return updated_kwota_count


# ---------------------------------------------------
# GUI — OKNO WYBORU PLIKU + ZAPIS NA PULPIT
# ---------------------------------------------------
class App(tk.Tk):
    def __init__(self):
        super().__init__()

        self.title(" POPRAWKI VAT Z NIPEM ")
        self.geometry("540x300")

        self.file_path = None

        tk.Button(self, text="Wybierz plik XML", command=self.choose_file,
                  font=("Arial", 12)).pack(pady=10)

        tk.Button(self, text="Przetwórz i zapisz na pulpicie", command=self.process_file,
                  bg="#4CAF50", fg="white", font=("Arial", 12)).pack(pady=10)

    def choose_file(self):
        file = filedialog.askopenfilename(filetypes=[("XML files", "*.xml")])
        if file:
            self.file_path = file
            messagebox.showinfo("Wybrano plik", file)

    def process_file(self):
        if not self.file_path:
            messagebox.showerror("Błąd", "Najpierw wybierz plik XML.")
            return

        filename = os.path.basename(self.file_path)
        name, ext = os.path.splitext(filename)

        desktop = os.path.join(os.path.expanduser("~"), "Desktop")
        output_file = os.path.join(desktop, f"{name}_v2{ext}")

        try:
            updated = update_xml(self.file_path, output_file)
            messagebox.showinfo(
                "Sukces",
                f"Zapisano: {output_file}\n\nZaktualizowano {updated} pozycji."
            )
        except Exception as e:
            messagebox.showerror("Błąd przetwarzania", str(e))


if __name__ == "__main__":
    App().mainloop()
