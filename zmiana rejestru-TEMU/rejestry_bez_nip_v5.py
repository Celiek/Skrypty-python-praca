import re
import os

def update_xml(input_file, output_file):
    with open(input_file, "r", encoding="utf-8") as file:
        xml_content = file.read()

    rejestr_pattern = re.compile(r"(<REJESTR_SPRZEDAZY_VAT>.*?</REJESTR_SPRZEDAZY_VAT>)", re.DOTALL)
    updated_kwota_count = 0

    # Mapping from Polish country names to ISO 2-letter codes
    country_name_to_code = {
        "Polska": "PL",
        "Niemcy": "DE",
        "Francja": "FR",
        "Włochy": "IT",
        "Hiszpania": "ES",
        "Czechy": "CZ",
        "Słowacja": "SK",
        "Węgry": "HU",
        "Austria": "AT",
        "Belgia": "BE",
        "Niderlandy": "NL",
        "Holandia": "NL",
        "Litwa": "LT",
        "Łotwa": "LV",
        "Estonia": "EE",
        "Rumunia": "RO",
        "Bułgaria": "BG",
        "Szwecja": "SE",
        "Dania": "DK",
        "Finlandia": "FI",
        "Norwegia": "NO",
        "Irlandia": "IE",
        "Portugalia": "PT",
        "Grecja": "GR",
        "Szwajcaria": "CH",
        "Wielka Brytania": "GB",
        "Zjednoczone Królestwo": "GB",
        "USA": "US",
        "Stany Zjednoczone": "US",
    }

    def process_rejestr_section(match):
        nonlocal updated_kwota_count
        rejestr_section = match.group(0)

        # Extract <KOD_KRAJU_ODBIORCY> and <KRAJ>
        kod_match = re.search(r"<KOD_KRAJU_ODBIORCY><!\[CDATA\[(.*?)\]\]></KOD_KRAJU_ODBIORCY>", rejestr_section)
        kraj_match = re.search(r"<KRAJ><!\[CDATA\[(.*?)\]\]></KRAJ>", rejestr_section)

        kod_kraju_value = kod_match.group(1).strip() if kod_match else ""
        kraj_value = kraj_match.group(1).strip() if kraj_match else ""

        if kod_kraju_value == "PL" and kraj_value in country_name_to_code:
            actual_country_code = country_name_to_code[kraj_value]

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

# Example usage
input_file = "report_20250801_20250831 (10).xml"
filename, ext = os.path.splitext(input_file)
output_file = f"{filename}_v2{ext}"
update_xml(input_file, output_file)
