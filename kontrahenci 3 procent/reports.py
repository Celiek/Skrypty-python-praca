from utils import _slugify_filename
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
import os
import logging
import pandas as pd


def export_grouped_excels(
    df,
    spolka,
    out_root="raporty_xlsx",
    out_ready_root="raporty_gotowe",
):

    # if not data_wystawienia:
    data_wystawienia = date.today().isoformat()

    spolka = spolka.lower()

    # ===============================
    # FOLDERY WYJŚCIOWE
    # ===============================
    out_dir = os.path.join(out_root, spolka, data_wystawienia)
    ready_dir = os.path.join(out_ready_root, spolka, data_wystawienia)

    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(ready_dir, exist_ok=True)

    # ===============================
    # KOLUMNY WEJŚCIOWE
    # ===============================
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

    # ===============================
    # RAPORT ZBIORCZY (1 ARKUSZ)
    # ===============================
    summary_writer_path = os.path.join(out_dir, "raport_zbiorczy.xlsx")
    writer = pd.ExcelWriter(summary_writer_path, engine="openpyxl")

    summary_frames = []  # TU ZBIERAMY WSZYSTKIE POZYCJE

    # ===============================
    # GRUPOWANIE PO NIP
    # ===============================
    for nip, sub in df.groupby("NIP"):
        nip_str = str(nip).strip() or "BRAK_NIP"
        kontrahent = str(sub["Kontrahent"].iloc[0]).strip()
        kontrahent_slug = _slugify_filename(kontrahent)

        sub = sub.copy()

        # ===============================
        # LICZENIE PROWIZJI DECIMAL
        # ===============================
        sub["Netto_dec"] = sub["Netto"].apply(lambda x: Decimal(str(x)))

        suma_netto_dec = sum(sub["Netto_dec"])
        suma_globalna_dec = (suma_netto_dec * Decimal("0.03")).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )

        # DEBUG dla nip 5261032852 FAKTUR
        # print("[DEBUG] faktury dla nipu: 5261032852")
        # print("Liczba wszystkich wierszy:", len(df))
        # print(
        #     df["NIP"]
        #     .value_counts()
        #     .get("5261032852", 0)
        # )
        #
        # print(f"Suma dla {nip_str} wynosi {suma_netto_dec}")

        if suma_globalna_dec < Decimal("0.00"):
            logging.warning(
                f"[XLSX] Ujemna prowizja dla NIP {nip_str} – pominięto"
            )
            continue

        sub["Kwota 3% Netto"] = sub["Netto_dec"].apply(
            lambda x: (x * Decimal("0.03")).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
        )

        diff = suma_globalna_dec - sum(sub["Kwota 3% Netto"])
        if diff != Decimal("0.00"):
            sub.loc[sub.index[-1], "Kwota 3% Netto"] += diff

        sub["Kwota 3% Netto"] = sub["Kwota 3% Netto"].astype(str)
        sub.drop(columns=["Netto_dec"], inplace=True)

        # ===============================
        # RAPORTY INDYWIDUALNE
        # ===============================
        summary = pd.DataFrame([{
            "Kontrahent": kontrahent,
            "NIP": nip_str,
            "Suma_prowizji": str(suma_globalna_dec)
        }])

        raport_indywidualny = pd.concat(
            [sub, pd.DataFrame([{}]), summary],
            ignore_index=True
        )

        path_std = os.path.join(
            out_dir,
            f"raport_{nip_str}_{kontrahent_slug}.xlsx"
        )
        raport_indywidualny.to_excel(
            path_std,
            index=False,
            sheet_name="Raport"
        )

        path_ready = os.path.join(
            ready_dir,
            f"{nip_str}.xlsx"
        )
        raport_indywidualny.to_excel(
            path_ready,
            index=False,
            sheet_name="Raport"
        )

        logging.info(f"[XLSX] Zapisano: {path_std}")
        logging.info(f"[XLSX] Zapisano (gotowe): {path_ready}")

        results[nip_str] = {
            "standard": path_std,
            "gotowy": path_ready,
        }

        # ===============================
        # DODANIE DO RAPORTU ZBIORCZEGO
        # ===============================
        summary_frames.append(
            sub[[
                "Data wystawienia",
                "Kontrahent",
                "NIP",
                "Numer dokumentu",
                "Netto",
                "VAT",
                "Brutto",
                "Kwota 3% Netto",
            ]]
        )

    # ===============================
    # ZAPIS RAPORTU ZBIORCZEGO
    # ===============================
    if summary_frames:
        df_summary = pd.concat(summary_frames, ignore_index=True)

        df_summary.to_excel(
            writer,
            sheet_name="RAPORT_ZBIORCZY",
            index=False
        )

    writer.close()
    logging.info(f"[XLSX] Raport zbiorczy zapisano: {summary_writer_path}")

    return results
