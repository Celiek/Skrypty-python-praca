import os
import re
import tkinter as tk
from tkinter import messagebox, filedialog
from tkinterdnd2 import DND_FILES, TkinterDnD

# =====================================
# MAPA KRAJÓW – BEZ ZMIAN
# =====================================
country_code_map = {
    "Rumunia": "RO", "Niemcy": "DE", "Francja": "FR", "Włochy": "IT",
    "Hiszpania": "ES", "Czechy": "CZ", "Słowacja": "SK", "Węgry": "HU",
    "Holandia": "NL", "Belgia": "BE", "Austria": "AT", "Dania": "DK",
    "Szwecja": "SE", "Norwegia": "NO", "Finlandia": "FI", "Litwa": "LT",
    "Łotwa": "LV", "Estonia": "EE", "Grecja": "GR", "Irlandia": "IE",
    "Portugalia": "PT", "Chorwacja": "HR", "Słowenia": "SI", "Bułgaria": "BG",
}

# =====================================
# TWOJA FUNKCJA update_xml NIEZMIENIONA
# =====================================
def update_xml(input_file, output_file):
    with open(input_file, "r", encoding="utf-8") as file:
        xml_content = file.read()

    rejestr_pattern = re.compile(r"(<REJESTR_SPRZEDAZY_VAT>.*?</REJESTR_SPRZEDAZY_VAT>)", re.DOTALL)
    updated_kwota_count = 0

    def process_rejestr_section(match):
        nonlocal updated_kwota_count
        section = match.group(0)
        rejestr_section = section


        kod_match = re.search(r"<KOD_KRAJU_ODBIORCY><!\[CDATA\[(.*?)\]\]></KOD_KRAJU_ODBIORCY>", rejestr_section)
        kraj_match = re.search(r"<KRAJ><!\[CDATA\[(.*?)\]\]></KRAJ>", rejestr_section)

        kod_kraju_value = kod_match.group(1).strip() if kod_match else ""
        kraj_value = kraj_match.group(1).strip() if kraj_match else ""

        if kod_kraju_value == "PL" and kraj_value in country_code_map:
            actual_country_code = country_code_map[kraj_value]

            # Replace <KOD_KRAJU_ODBIORCY>
            rejestr_section = re.sub(
                r"<KOD_KRAJU_ODBIORCY><!\[CDATA\[.*?\]\]></KOD_KRAJU_ODBIORCY>",
                f"<KOD_KRAJU_ODBIORCY><![CDATA[{actual_country_code}]]></KOD_KRAJU_ODBIORCY>",
                rejestr_section
            )
        else:
            actual_country_code = kod_kraju_value

        # Replace <NIP_KRAJ> based on resolved country code if <NIP> is empty
        nip_kraj_pattern = re.compile(
            r"(<NIP_KRAJ><!\[CDATA\[.*?\]\]></NIP_KRAJ>)(\s*<NIP>\s*<!\[CDATA\[\]\]>\s*</NIP>)"
        )

        def replace_nip_kraj(nip_match):
            nip_kraj_part = nip_match.group(1)
            nip_part = nip_match.group(2)
            if actual_country_code:
                new_nip_kraj = f"<NIP_KRAJ><![CDATA[{actual_country_code}]]></NIP_KRAJ>"
                return new_nip_kraj + nip_part
            return nip_match.group(0)

        rejestr_section = nip_kraj_pattern.sub(replace_nip_kraj, rejestr_section)

        # Replace <FORMA_PLATNOSCI>
        rejestr_section = re.sub(
            r"<FORMA_PLATNOSCI><!\[CDATA\[.*?\]\]></FORMA_PLATNOSCI>",
            "<FORMA_PLATNOSCI><![CDATA[przelew]]></FORMA_PLATNOSCI>",
            rejestr_section
        )

        # Replace <FORMA_PLATNOSCI_ID>
        rejestr_section = re.sub(
            r"<FORMA_PLATNOSCI_ID><!\[CDATA\[.*?\]\]></FORMA_PLATNOSCI_ID>",
            "<FORMA_PLATNOSCI_ID><![CDATA[98843769]]></FORMA_PLATNOSCI_ID>",
            rejestr_section
        )

        # Replace <FORMA_PLATNOSCI_PLAT> inside <PLATNOSCI>
        rejestr_section = re.sub(
            r"<FORMA_PLATNOSCI_PLAT><!\[CDATA\[.*?\]\]></FORMA_PLATNOSCI_PLAT>",
            "<FORMA_PLATNOSCI_PLAT><![CDATA[przelew]]></FORMA_PLATNOSCI_PLAT>",
            rejestr_section
        )

        # Replace <FORMA_PLATNOSCI_ID_PLAT> inside <PLATNOSCI>
        rejestr_section = re.sub(
            r"<FORMA_PLATNOSCI_ID_PLAT><!\[CDATA\[.*?\]\]></FORMA_PLATNOSCI_ID_PLAT>",
            "<FORMA_PLATNOSCI_ID_PLAT><![CDATA[98843769]]></FORMA_PLATNOSCI_ID_PLAT>",
            rejestr_section
        )

        # Calculate new KWOTA_PLAT
        pozycje_matches = re.findall(r"<POZYCJA>.*?<NETTO>(.*?)</NETTO>.*?<VAT>(.*?)</VAT>.*?</POZYCJA>", rejestr_section, re.DOTALL)
        netto_vat_sum = sum(float(netto.strip().replace(',', '.')) + float(vat.strip().replace(',', '.'))
                            for netto, vat in pozycje_matches)

        id_zrodla_match = re.search(r"<ID_ZRODLA><!\[CDATA\[(.*?)\]\]></ID_ZRODLA>", rejestr_section)
        id_zrodla = id_zrodla_match.group(1).strip() if id_zrodla_match else "[UNKNOWN]"

        # Find current KWOTA_PLAT
        kwota_match = re.search(r"<KWOTA_PLAT>(.*?)</KWOTA_PLAT>", rejestr_section)
        current_kwota = float(kwota_match.group(1).replace(',', '.')) if kwota_match else None

        expected_kwota = round(abs(netto_vat_sum), 2)

        if current_kwota is None or abs(current_kwota - expected_kwota) >= 0.001:
            updated_kwota_count += 1
            difference = expected_kwota - (current_kwota or 0)
            print(f"Changed KWOTA_PLAT in ID_ZRODLA: {id_zrodla} (diff: {difference:+.2f})")
            new_kwota_str = f"{expected_kwota:.2f}"
            rejestr_section = re.sub(r"<KWOTA_PLAT>.*?</KWOTA_PLAT>",
                                     f"<KWOTA_PLAT>{new_kwota_str}</KWOTA_PLAT>",
                                     rejestr_section)

        return rejestr_section

    updated_content = rejestr_pattern.sub(process_rejestr_section, xml_content)

    with open(output_file, "w", encoding="utf-8") as file:
        file.write(updated_content)

    print(f"Updated XML saved as {output_file}")
    print(f"KWOTA_PLAT updated in {updated_kwota_count} entries")

    return updated_kwota_count


# =====================================
# GUI – DRAG & DROP + PRZYCISKI
# =====================================
class App(TkinterDnD.Tk):
    def __init__(self):
        super().__init__()

        self.title("POPRAWKI w społkach bez nipu")
        self.geometry("540x300")
        self.resizable(False, False)

        tk.Label(self, text="Przeciągnij plik XML tutaj:", font=("Arial", 12)).pack(pady=10)

        self.drop_area = tk.Text(self, height=4, width=60, relief="solid", borderwidth=2)
        self.drop_area.pack(pady=5)
        self.drop_area.insert("end", "Upuść plik tutaj...")
        self.drop_area.drop_target_register(DND_FILES)
        self.drop_area.dnd_bind("<<Drop>>", self.on_drop)

        tk.Button(self, text="Wybierz plik...", command=self.choose_file).pack(pady=5)

        tk.Button(self, text="Przetwórz i zapisz na pulpicie", command=self.process_file,
                  bg="#4CAF50", fg="white", font=("Arial", 11)).pack(pady=15)

        self.file_path = None

    def on_drop(self, event):
        path = event.data.strip("{}")
        self.file_path = path
        self.drop_area.delete("1.0", "end")
        self.drop_area.insert("end", path)

    def choose_file(self):
        path = filedialog.askopenfilename(filetypes=[("XML files", "*.xml")])
        if path:
            self.file_path = path
            self.drop_area.delete("1.0", "end")
            self.drop_area.insert("end", path)

    def process_file(self):
        if not self.file_path:
            messagebox.showerror("Błąd", "Nie wybrano pliku XML!")
            return

        try:
            filename = os.path.basename(self.file_path)
            name, ext = os.path.splitext(filename)

            desktop = os.path.join(os.path.expanduser("~"), "Desktop")
            output_file = os.path.join(desktop, f"{name}_v2{ext}")

            updated = update_xml(self.file_path, output_file)

            messagebox.showinfo(
                "Sukces",
                f"Plik zapisany jako:\n{output_file}\n\nZmieniono KWOTA_PLAT w {updated} rekordach."
            )

        except Exception as e:
            messagebox.showerror("Błąd podczas przetwarzania", str(e))


# =====================================
# START PROGRAMU
# =====================================
if __name__ == "__main__":
    App().mainloop()
