from utils import _slugify_filename
import os
import pandas as pd
import logging


def export_grouped_excels(
    df: pd.DataFrame,
    spolka: str,
    data_wystawienia: str | None = None,
    out_root: str = "faktury"
) -> dict[str, str]:
    """
    Eksportuje raporty XLSX per kontrahent do struktury:
    <out_root>/<spółka>/<data>/raport_NIP_NAZWA.xlsx

    Kolumny w raporcie:
    Kontrahent, NIP, Numer dokumentu, Netto, VAT, Brutto, Prowizja_3proc, Suma_prowizji
    """

    if not data_wystawienia:
        from datetime import date
        data_wystawienia = date.today().isoformat()

    out_dir = os.path.join(out_root, spolka.lower(), data_wystawienia)
    os.makedirs(out_dir, exist_ok=True)
    result = {}

    # Kolumny wymagane do raportu
    desired_cols = [
        "NIP", "Kontrahent", "Numer dokumentu",
        "Netto", "VAT", "Brutto"
    ]
    existing_cols = [c for c in desired_cols if c in df.columns]
    df = df[existing_cols].copy()

    # Konwersja numeryczna
    for col in ["Netto", "VAT", "Brutto"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Grupowanie wg NIP
    for nip, sub in df.groupby("NIP", dropna=False):
        nip_str = str(nip).strip() or "BRAK_NIP"
        kontrahent = (
            str(sub["Kontrahent"].iloc[0]).strip()
            if "Kontrahent" in sub.columns else ""
        )

        # ✅ prowizja 3% dla każdego wiersza
        sub = sub.copy()
        sub["Prowizja_3proc"] = (sub["Netto"] * 0.03).round(2)

        # ✅ suma prowizji dla kontrahenta
        suma_prowizji = sub["Prowizja_3proc"].sum().round(2)

        # Wiersz podsumowania
        summary = pd.DataFrame([{
            "Kontrahent": kontrahent,
            "NIP": nip_str,
            "Suma_prowizji": suma_prowizji
        }])

        # Zbuduj raport: wiersze + pusta linia + podsumowanie
        raport = pd.concat([
            sub,
            pd.DataFrame([{}]),  # pusta linia
            summary
        ], ignore_index=True)

        # Ścieżka zapisu
        filename = f"raport_{nip_str}_{_slugify_filename(kontrahent)}.xlsx"
        path = os.path.join(out_dir, filename)

        raport.to_excel(path, index=False, sheet_name="Raport")
        result[nip_str] = os.path.abspath(path)

        logging.info(f"[XLSX] Zapisano raport dla NIP {nip_str}: {path}")

    return result
