import os
import logging
import pandas as pd
from utils import _slugify_filename

def export_grouped_excels(df: pd.DataFrame, out_dir: str = "raporty_kontrahenci") -> dict[str, str]:
    os.makedirs(out_dir, exist_ok=True)
    result = {}
    for nip, sub in df.groupby("NIP", dropna=False):
        nip_str = str(nip).strip() or "BRAK_NIP"
        kontrahent = str(sub["Kontrahent"].iloc[0]) if "Kontrahent" in sub.columns else ""
        for c in ["Netto", "VAT", "Brutto"]:
            sub[c] = pd.to_numeric(sub[c], errors="coerce").fillna(0)
        sums = pd.DataFrame([{
            "NIP": nip_str,
            "Suma netto": sub["Netto"].sum(),
            "Suma VAT": sub["VAT"].sum(),
            "Suma brutto": sub["Brutto"].sum(),
        }])
        raport = pd.concat([sub, pd.DataFrame([{}]), sums], ignore_index=True)
        path = os.path.join(out_dir, f"raport_{nip_str}_{_slugify_filename(kontrahent)}.xlsx")
        raport.to_excel(path, index=False, sheet_name="Raport")
        result[nip_str] = os.path.abspath(path)
        logging.info(f"[XLSX] {path}")
    return result
