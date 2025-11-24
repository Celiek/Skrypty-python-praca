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

        id_zrodla_match = re.search(r"<ID_ZRODLA><!\[CDATA\[(.*?)\]\]></ID_ZRODLA>", section)
        id_zrodla = id_zrodla_match.group(1).strip() if id_zrodla_match else "[UNKNOWN]"

        stawka_match = re.search(
            r"<POZYCJE>\s*<POZYCJA>.*?<STAWKA_VAT>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</STAWKA_VAT>",
            section, re.DOTALL
        )
        stawka_vat = stawka_match.group(1).strip() if stawka_match else None

        if stawka_vat == "23":
            nip_kraj_value = ""
        elif stawka_vat == "0":
            kraj_match = re.search(r"<KRAJ><!\[CDATA\[(.*?)\]\]></KRAJ>", section)
            kraj_name = kraj_match.group(1).strip() if kraj_match else ""
            if kraj_name in country_code_map:
                nip_kraj_value = country_code_map[kraj_name]
            else:
                raise ValueError(
                    f"❌ Missing country code mapping for: '{kraj_name}' (ID_ZRODLA: {id_zrodla})"
                )
        else:
            raise ValueError(
                f"❌ Unexpected STAWKA_VAT value: '{stawka_vat}' (ID_ZRODLA: {id_zrodla})"
            )

        # NIP_KRAJ Modyfikacja
        if "<NIP_KRAJ>" in section:
            section = re.sub(
                r"<NIP_KRAJ><!\[CDATA\[.*?\]\]></NIP_KRAJ>",
                f"<NIP_KRAJ><![CDATA[{nip_kraj_value}]]></NIP_KRAJ>",
                section
            )
        else:
            section = re.sub(
                r"(</NIP>)",
                rf"\1\n<NIP_KRAJ><![CDATA[{nip_kraj_value}]]></NIP_KRAJ>",
                section
            )

        # Forma płatności
        section = re.sub(
            r"<FORMA_PLATNOSCI><!\[CDATA\[.*?\]\]></FORMA_PLATNOSCI>",
            "<FORMA_PLATNOSCI><![CDATA[przelew]]></FORMA_PLATNOSCI>",
            section
        )

        section = re.sub(
            r"<FORMA_PLATNOSCI_ID><!\[CDATA\[.*?\]\]></FORMA_PLATNOSCI_ID>",
            "<FORMA_PLATNOSCI_ID><![CDATA[98843769]]></FORMA_PLATNOSCI_ID>",
            section
        )

        section = re.sub(
            r"<FORMA_PLATNOSCI_PLAT><!\[CDATA\[.*?\]\]></FORMA_PLATNOSCI_PLAT>",
            "<FORMA_PLATNOSCI_PLAT><![CDATA[przelew]]></FORMA_PLATNOSCI_PLAT>",
            section
        )
        section = re.sub(
            r"<FORMA_PLATNOSCI_ID_PLAT><!\[CDATA\[.*?\]\]></FORMA_PLATNOSCI_ID_PLAT>",
            "<FORMA_PLATNOSCI_ID_PLAT><![CDATA[98843769]]></FORMA_PLATNOSCI_ID_PLAT>",
            section
        )

        # Wyliczenie kwoty
        pozycje_matches = re.findall(
            r"<POZYCJA>.*?<NETTO>(.*?)</NETTO>.*?<VAT>(.*?)</VAT>.*?</POZYCJA>",
            section,
            re.DOTALL
        )
        netto_vat_sum = sum(
            float(n.replace(",", ".")) + float(v.replace(",", "."))
            for n, v in pozycje_matches
        )

        expected_kwota = round(abs(netto_vat_sum), 2)

        kwota_match = re.search(r"<KWOTA_PLAT>(.*?)</KWOTA_PLAT>", section)
        if kwota_match:
            current_kwota = round(
                float(kwota_match.group(1).replace(",", ".").strip()), 2
            )
        else:
            current_kwota = None

        if current_kwota is None or abs(current_kwota - expected_kwota) >= 0.001:
            updated_kwota_count += 1
            new_kwota_str = f"{expected_kwota:.2f}"
            section = re.sub(
                r"<KWOTA_PLAT>.*?</KWOTA_PLAT>",
                f"<KWOTA_PLAT>{new_kwota_str}</KWOTA_PLAT>",
                section
            )

        return section

    updated_content = ""
    try:
        updated_content = rejestr_pattern.sub(process_rejestr_section, xml_content)
    except ValueError as e:
        raise e

    with open(output_file, "w", encoding="utf-8") as file:
        file.write(updated_content)

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
