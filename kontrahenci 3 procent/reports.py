from utils import _slugify_filename
import os
import pandas as pd
import logging
from decimal import Decimal, ROUND_HALF_UP


def export_grouped_excels(df, spolka, data_wystawienia=None, out_root="raporty_xlsx"):
    from datetime import date

    if not data_wystawienia:
        data_wystawienia = date.today().isoformat()

    out_dir = os.path.join(out_root, spolka.lower(), data_wystawienia)
    os.makedirs(out_dir, exist_ok=True)

    # Kolumny do raportu
    keep_cols = [
        "NIP",
        "Data wystawienia",
        "Kontrahent",
        "Numer dokumentu",
        "Netto",
        "VAT",
        "Brutto",
    ]
    df = df[[c for c in keep_cols if c in df.columns]].copy()

    results = {}

    # Raport zbiorczy (każdy NIP jako osobny arkusz)
    summary_writer_path = os.path.join(out_dir, "raport_zbiorczy.xlsx")
    writer = pd.ExcelWriter(summary_writer_path, engine="openpyxl")

    # Grupowanie po NIP
    for nip, sub in df.groupby("NIP"):
        nip_str = str(nip).strip() or "BRAK_NIP"
        kontrahent = str(sub["Kontrahent"].iloc[0]).strip()
        kontrahent_slug = _slugify_filename(kontrahent)

        sub = sub.copy()

        # PRECYZYJNE LICZENIE NA DECIMAL
        sub["Netto_dec"] = sub["Netto"].apply(lambda x: Decimal(str(x)))

        # Bezbłędna suma netto
        suma_netto_dec = sum(sub["Netto_dec"])

        # Globalna prowizja jak na FV – Decimal, bankowe zaokrąglenie
        suma_globalna_dec = (suma_netto_dec * Decimal("0.03")).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )

        if suma_globalna_dec < Decimal("0.00"):
            logging.warning(
                f"[INFO] [XLSX] [Ujemna prowizja] dla NIP {nip_str} "
                f"({suma_globalna_dec}) — raport pominięty."
            )
            continue

        # Prowizje per pozycja – Decimal
        sub["Prowizja_3proc"] = sub["Netto_dec"].apply(
            lambda x: (x * Decimal("0.03")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        )

        partial_sum_dec = sum(sub["Prowizja_3proc"])
        fix_diff = suma_globalna_dec - partial_sum_dec

        # Jeśli jest różnica grosza → dopisz do ostatniej pozycji
        if fix_diff != Decimal("0.00"):
            sub.loc[sub.index[-1], "Prowizja_3proc"] += fix_diff
            # logging.info(
            #     f"[FIX] Korekta grosza dla NIP {nip_str}: {fix_diff} "
            #     f"→ prowizja = {suma_globalna_dec}"
            # )

        # Zamiana na string dla XLSX
        sub["Prowizja_3proc"] = sub["Prowizja_3proc"].astype(str)

        # Usuwamy tymczasową kolumnę
        sub.drop(columns=["Netto_dec"], inplace=True)

        # Podsumowanie
        summary = pd.DataFrame([{
            "Kontrahent": kontrahent,
            "NIP": nip_str,
            "Suma_prowizji": str(suma_globalna_dec)
        }])

        # Raport indywidualny
        raport = pd.concat([sub, pd.DataFrame([{}]), summary], ignore_index=True)

        filename = f"raport_{nip_str}_{kontrahent_slug}.xlsx"
        fpath = os.path.join(out_dir, filename)
        raport.to_excel(fpath, index=False, sheet_name="Raport")

        results[nip_str] = fpath
        logging.info(f"[XLSX] Raport zapisany: {fpath}")

        # Wpis do raportu zbiorczego
        sub.to_excel(writer, sheet_name=nip_str[-10:], index=False)

    # Zapis raportu zbiorczego
    writer.close()
    logging.info(f"[XLSX] Raport zbiorczy zapisano: {summary_writer_path}")

    return results
