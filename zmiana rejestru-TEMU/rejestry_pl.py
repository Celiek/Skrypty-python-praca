import re
import os

def update_xml(input_file, output_file):
    with open(input_file, "r", encoding="utf-8") as file:
        xml_content = file.read()

    rejestr_pattern = re.compile(r"(<REJESTR_SPRZEDAZY_VAT>.*?</REJESTR_SPRZEDAZY_VAT>)", re.DOTALL)
    updated_kwota_count = 0

    def process_rejestr_section(match):
        nonlocal updated_kwota_count
        rejestr_section = match.group(0)

        # Extract the <KOD_KRAJU_ODBIORCY> within this <REJESTR_SPRZEDAZY_VAT>
        kod_match = re.search(r"<KOD_KRAJU_ODBIORCY><!\[CDATA\[(.*?)\]\]></KOD_KRAJU_ODBIORCY>", rejestr_section)
        kod_kraju_value = kod_match.group(1) if kod_match else ""

        # Replace <NIP_KRAJ> based on <KOD_KRAJU_ODBIORCY> if <NIP> is empty
        nip_kraj_pattern = re.compile(
            r"(<NIP_KRAJ><!\[CDATA\[.*?\]\]></NIP_KRAJ>)(\s*<NIP>\s*<!\[CDATA\[\]\]>\s*</NIP>)"
        )

        def replace_nip_kraj(nip_match):
            nip_kraj_part = nip_match.group(1)
            nip_part = nip_match.group(2)
            if kod_kraju_value:
                new_nip_kraj = f"<NIP_KRAJ><![CDATA[{kod_kraju_value}]]></NIP_KRAJ>"
                return new_nip_kraj + nip_part
            return nip_match.group(0)

        rejestr_section = nip_kraj_pattern.sub(replace_nip_kraj, rejestr_section)

        # Replace or insert <FORMA_PLATNOSCI>
        rejestr_section = re.sub(
            r"<FORMA_PLATNOSCI><!\[CDATA\[.*?\]\]></FORMA_PLATNOSCI>",
            "<FORMA_PLATNOSCI><![CDATA[przelew]]></FORMA_PLATNOSCI>",
            rejestr_section
        )

        # Replace or insert <FORMA_PLATNOSCI_ID>
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
        '''print("\n--- DEBUG ---")
        print(f"ID_ZRODLA: {id_zrodla}")
        print(f"Found KWOTA_PLAT: {kwota_match.group(1) if kwota_match else 'None'}")
        print(f"Parsed current_kwota: {current_kwota}")
        print(f"Calculated netto_vat_sum: {netto_vat_sum}")
        print(f"Expected_kwota (abs): {expected_kwota}")
        print(f"Difference: {abs(current_kwota - expected_kwota) if current_kwota is not None else 'N/A'}")'''


        if kwota_match:
            current_kwota = round(float(kwota_match.group(1).replace(',', '.').strip()), 2)
        else:
            current_kwota = None

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

# Example usage
input_file = "report_20250801_20250831 (9).xml"
filename, ext = os.path.splitext(input_file)
output_file = f"{filename}_v2{ext}"
update_xml(input_file, output_file)
