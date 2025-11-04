import json
import re
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List

import pandas as pd


# -----------------------------
# Pomocnicze
# -----------------------------
def digits_only(x) -> str:
    return "".join(ch for ch in str(x) if ch.isdigit())

def to_pln(val) -> float:
    try:
        return round(float(val), 2)
    except Exception:
        return 0.0

def parse_number_series(s: pd.Series) -> pd.Series:
    """
    Zamienia europejskie formaty liczb na float:
    '1 234,56' -> 1234.56, usuwa &nbsp;/wąskie spacje.
    """
    return pd.to_numeric(
        s.astype(str)
         .str.replace("\u00A0", "", regex=False)  # nbsp
         .str.replace("\u202F", "", regex=False)  # thin space
         .str.replace(" ", "", regex=False)
         .str.replace(",", ".", regex=False),
        errors="coerce"
    ).fillna(0.0)

def slug_name(s: str, limit: int = 40) -> str:
    s = (s or "").strip().upper()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Z0-9_]+", "", s)
    s = s[:limit] if s else "N_A"
    return s or "N_A"

def ensure_date_iso(s) -> Optional[str]:
    """
    Próbuje z parsowaniem daty w popularnych formatach.
    Zwraca 'YYYY-MM-DD' albo None.
    """
    if pd.isna(s):
        return None
    for dayfirst in (True, False):
        try:
            dt = pd.to_datetime(s, errors="raise", dayfirst=dayfirst)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            continue
    return None


# -----------------------------
# Główna funkcja
# -----------------------------
def build_payments_json(
    xlsx_path: str,
    *,
    company: str = "shumee",
    date_column: str = "Data zakupu",
    nip_column: str = "NIP",
    kontrahent_column: str = "Kontrahent",
    netto_column: str = "Netto",
    vat_column: str = "VAT",
    brutto_column: str = "Brutto",
    docno_column: str = "Numer dokumentu",
    extra_columns: Optional[List[str]] = None,
    nip_to_nrb: Optional[Dict[str, str]] = None,  # opcjonalna mapa NIP->NRB
) -> Dict[str, Any]:
    """
    Buduje słownik JSON:
      { 'przelew_<company>_<NIP|NAME__...>': { company, nip, kontrahent, konto_nrb, przelewy:[...] , podsumowanie:{...} } }
    gdzie każde 'przelewy[i]' = 1 przelew (dzień) z sumami i listą faktur.
    """
    nip_to_nrb = nip_to_nrb or {}

    df = pd.read_excel(xlsx_path)
    df.columns = [str(c).strip() for c in df.columns]

    # Sprawdź istnienie podstawowych kolumn
    needed = [date_column, nip_column, kontrahent_column, netto_column, vat_column, brutto_column, docno_column]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise ValueError(f"Brak kolumn w pliku: {', '.join(missing)}")

    # Kwoty -> float
    for col in (netto_column, vat_column, brutto_column):
        df[col] = parse_number_series(df[col])

    # NIP w formie 10 cyfr (lub pusty)
    df["__nip_clean"] = df[nip_column].map(digits_only)

    # Data -> ISO
    df["__day_iso"] = df[date_column].map(ensure_date_iso)

    # Grupowanie po NIP + dzień (jeśli brak daty, i tak zgrupujemy, ale dzień będzie None)
    group_cols = ["__nip_clean", "__day_iso"]
    grouped = (
        df.groupby(group_cols, dropna=False)
          .agg(
              suma_netto=(netto_column, "sum"),
              suma_vat=(vat_column, "sum"),
              suma_brutto=(brutto_column, "sum"),
              liczba_faktur=(docno_column, "count"),
              kontrahent=(kontrahent_column, "first"),
          )
          .reset_index()
    )

    # Zbierz wynik
    result: Dict[str, Any] = {}

    # Jakie dodatkowe kolumny faktury chcesz przenieść (jeśli istnieją)
    default_extras = ["Waluta", "Opis", "Id. księgowy", "Forma płatności", "Kody JPK_V7", "VAT-7", "VAT-UE"]
    extra_columns = extra_columns if extra_columns is not None else default_extras
    extra_columns = [c for c in extra_columns if c in df.columns]

    # Iteruj po każdej grupie = po jednym przelewie
    for _, row in grouped.iterrows():
        nip = str(row["__nip_clean"] or "")
        valid_nip = nip.isdigit() and len(nip) == 10
        kontrahent = str(row["kontrahent"] or "")

        # klucz w słowniku wynikowym
        dict_key = (
            f"przelew_{company}_{nip}"
            if valid_nip else
            f"przelew_{company}_NAME__{slug_name(kontrahent)}"
        )

        # pierwszy raz dla kontrahenta -> zainicjuj
        if dict_key not in result:
            konto_nrb = nip_to_nrb.get(nip, "") if valid_nip else ""
            result[dict_key] = {
                "company": company,
                "nip": nip if valid_nip else "",
                "kontrahent": kontrahent,
                "konto_nrb": konto_nrb,
                "przelewy": [],
                "podsumowanie": {
                    "liczba_przelewow": 0,
                    "liczba_faktur": 0,
                    "łączna_brutto": 0.0
                }
            }

        # wybierz faktury należące do tej grupy
        mask = (df["__nip_clean"] == nip) & (df["__day_iso"] == row["__day_iso"])
        faktury = []
        for _, fr in df.loc[mask].iterrows():
            item = {
                "numer": str(fr.get(docno_column, "")),
                "data": ensure_date_iso(fr.get(date_column)),
                "netto": to_pln(fr.get(netto_column, 0)),
                "vat": to_pln(fr.get(vat_column, 0)),
                "brutto": to_pln(fr.get(brutto_column, 0)),
                "kontrahent": str(fr.get(kontrahent_column, "") or "")
            }
            for c in extra_columns:
                # pod kluczami w snake'u nie kombinujemy – daj oryginał nagłówka
                item[c] = None if pd.isna(fr.get(c)) else fr.get(c)
            faktury.append(item)

        # zbuduj „jeden przelew” (dla tego dnia)
        przelew_item = {
            "dzien": row["__day_iso"],  # 'YYYY-MM-DD'
            "suma_netto": to_pln(row["suma_netto"]),
            "suma_vat": to_pln(row["suma_vat"]),
            "suma_brutto": to_pln(row["suma_brutto"]),
            "faktury": faktury
        }

        result[dict_key]["przelewy"].append(przelew_item)

        # aktualizuj podsumowanie kontrahenta
        pod = result[dict_key]["podsumowanie"]
        pod["liczba_przelewow"] += 1
        pod["liczba_faktur"] += int(row["liczba_faktur"])
        pod["łączna_brutto"] = round(pod["łączna_brutto"] + to_pln(row["suma_brutto"]), 2)

    return result


def save_json(data: Dict[str, Any], out_path: str) -> None:
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# -----------------------------
# CLI / przykład użycia
# -----------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Buduje JSON przelewów z pliku XLSX (master: przelew_<company>_<NIP>).")
    parser.add_argument("xlsx", help="Ścieżka do pliku XLSX")
    parser.add_argument("-o", "--output", default=None, help="Ścieżka wyjściowego JSON (domyślnie obok XLSX)")
    parser.add_argument("-c", "--company", default="shumee", help="Nazwa firmy do kluczy JSON (domyślnie: shumee)")
    parser.add_argument("--date-col", default="Data zakupu", help="Kolumna z datą (domyślnie: 'Data zakupu')")
    args = parser.parse_args()

    out_path = args.output or str(Path(args.xlsx).with_suffix("").with_name(
        f"{Path(args.xlsx).stem}_przelewy_grouped.json"))

    data = build_payments_json(
        args.xlsx,
        company=args.company,
        date_column=args.date_col,
        # jeśli masz mapę NIP->NRB z DB, przekaż tu:
        # nip_to_nrb={"5252805616":"26114020040000330280429939", ...}
    )
    save_json(data, out_path)
    print(f"✅ Zapisano: {out_path}")
