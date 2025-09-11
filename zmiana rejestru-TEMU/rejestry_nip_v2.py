import re
import os

# Map of Polish country names to ISO 2-letter country codes
country_code_map = {
    "Rumunia": "RO",
    "Niemcy": "DE",
    "Francja": "FR",
    "Włochy": "IT",
    "Hiszpania": "ES",
    "Czechy": "CZ",
    "Słowacja": "SK",
    "Węgry": "HU",
    "Holandia": "NL",
    "Belgia": "BE",
    "Austria": "AT",
    "Dania": "DK",
    "Szwecja": "SE",
    "Norwegia": "NO",
    "Finlandia": "FI",
    "Litwa": "LT",
    "Łotwa": "LV",
    "Estonia": "EE",
    "Grecja": "GR",
    "Irlandia": "IE",
    "Portugalia": "PT",
    "Chorwacja": "HR",
    "Słowenia": "SI",
    "Bułgaria": "BG",
}

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

        stawka_match = re.search(r"<POZYCJE>\s*<POZYCJA>.*?<STAWKA_VAT>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</STAWKA_VAT>", section, re.DOTALL)
        stawka_vat = stawka_match.group(1).strip() if stawka_match else None

        if stawka_vat == "23":
            nip_kraj_value = ""
        elif stawka_vat == "0":
            kraj_match = re.search(r"<KRAJ><!\[CDATA\[(.*?)\]\]></KRAJ>", section)
            kraj_name = kraj_match.group(1).strip() if kraj_match else ""
            if kraj_name in country_code_map:
                nip_kraj_value = country_code_map[kraj_name]
            else:
                raise ValueError(f"❌ Missing country code mapping for: '{kraj_name}' (ID_ZRODLA: {id_zrodla}) – please add it to country_code_map")
        else:
            raise ValueError(f"❌ Unexpected STAWKA_VAT value: '{stawka_vat}' (ID_ZRODLA: {id_zrodla}) – cannot determine <NIP_KRAJ>")

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

        pozycje_matches = re.findall(r"<POZYCJA>.*?<NETTO>(.*?)</NETTO>.*?<VAT>(.*?)</VAT>.*?</POZYCJA>", section, re.DOTALL)
        netto_vat_sum = sum(float(netto.strip().replace(',', '.')) + float(vat.strip().replace(',', '.')) for netto, vat in pozycje_matches)

        kwota_match = re.search(r"<KWOTA_PLAT>(.*?)</KWOTA_PLAT>", section)
        current_kwota = float(kwota_match.group(1).replace(',', '.')) if kwota_match else None

        expected_kwota = round(abs(netto_vat_sum), 2)

        kwota_match = re.search(r"<KWOTA_PLAT>(.*?)</KWOTA_PLAT>", section)
        if kwota_match:
            current_kwota = round(float(kwota_match.group(1).replace(',', '.').strip()), 2)
        else:
            current_kwota = None

        if current_kwota is None or abs(current_kwota - expected_kwota) >= 0.001:
            updated_kwota_count += 1
            difference = expected_kwota - (current_kwota or 0)
            print(f"Changed KWOTA_PLAT in ID_ZRODLA: {id_zrodla} (diff: {difference:+.2f})")
            new_kwota_str = f"{expected_kwota:.2f}"
            section = re.sub(
                r"<KWOTA_PLAT>.*?</KWOTA_PLAT>",
                f"<KWOTA_PLAT>{new_kwota_str}</KWOTA_PLAT>",
                section
    )
        return section

    try:
        updated_content = rejestr_pattern.sub(process_rejestr_section, xml_content)
    except ValueError as e:
        print(e)
        return

    with open(output_file, "w", encoding="utf-8") as file:
        file.write(updated_content)

    print(f"✅ Updated XML saved as {output_file}")
    print(f"✉️ KWOTA_PLAT updated in {updated_kwota_count} entries")

input_file = "great zagranica korekty z nip.xml"
filename, ext = os.path.splitext(input_file)
output_file = f"{filename}_v2{ext}"
update_xml(input_file, output_file)
