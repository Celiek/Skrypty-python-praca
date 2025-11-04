# reconcile_fix_report.py
import argparse
import re
from pathlib import Path
from typing import Optional, Tuple, List

import pandas as pd
from glob import glob



# ============== Utils & parsing ==============

def to_int_grosze(x) -> int:
    if pd.isna(x):
        return 0
    s = str(x).strip().replace(" ", "").replace("\u00A0", "")
    s = s.replace(",", ".")
    try:
        return int(round(float(s) * 100))
    except Exception:
        return 0

def clean_nip(s: Optional[str]) -> Optional[str]:
    if s is None or pd.isna(s):
        return None
    digits = re.sub(r"\D", "", str(s))
    return digits if len(digits) == 10 else None

def parse_elixir(path: Path, encoding_hint: str = "iso-8859-2") -> pd.DataFrame:
    """
    Parsuje plik ELIXIR generowany przez Twój skrypt (CSV z polami, 'szczegóły' w cudzysłowie).
    Zwraca kolumny: file, data_platnosci, kwota_gr, nrb_kontrahenta, nazwa_kontrahenta, szczegoly, nip, ddmmyy
    """
    # Spróbuj kilku kodowań
    last_err = None
    for enc in [encoding_hint, "latin2", "cp1250", "utf-8"]:
        try:
            df = pd.read_csv(path, header=None, dtype=str, encoding=enc, engine="python")
            break
        except Exception as e:
            last_err = e
            df = None
    if df is None:
        raise last_err

    # Mapowanie kolumn wg build_payment_record z Twojego generatora
    df = df.rename(columns={
        1: "data_platnosci",
        2: "kwota_gr",
        6: "nrb_kontrahenta",
        8: "nazwa_kontrahenta",
        11: "szczegoly",
        14: "klasyfikacja",
    })
    # liczby w groszach to już int (w Twoim generatorze są bez separatorów)
    df["kwota_gr"] = df["kwota_gr"].apply(lambda x: int(str(x).strip()))
    # wyciągamy NIP i "FVddmmyy"
    def extract(pat: str, txt: str) -> Optional[str]:
        m = re.search(pat, str(txt) if pd.notna(txt) else "")
        return m.group(1) if m else None
    df["nip"] = df["szczegoly"].apply(lambda t: extract(r"/IDC/(\d{10})", t))
    df["ddmmyy"] = df["szczegoly"].apply(lambda t: extract(r"/INV/FV(\d{6})", t))
    df["file"] = path.name
    # bierzemy tylko dodatnie kwoty (płatności)
    df = df[df["kwota_gr"] > 0].reset_index(drop=True)
    return df

def load_invoices(path: Path) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
    """
    Wczytuje Arkusz1 (wymagany) i Duplikaty (opcjonalny).
    Normalizuje kolumny i wylicza: brutto_gr, ddmmyy_wyst, ddmmyy_wplyw.
    """
    xls = pd.ExcelFile(path)
    arkusz1_name = "Arkusz1" if "Arkusz1" in xls.sheet_names else xls.sheet_names[0]
    dups_name = "Duplikaty" if "Duplikaty" in xls.sheet_names else None
    df = pd.read_excel(path, sheet_name=arkusz1_name)
    df_dups = pd.read_excel(path, sheet_name=dups_name) if dups_name else None

    # dopasowanie nagłówków
    colmap = {
        "Data wystawienia": "data_wystawienia",
        "Data wpływu": "data_wplywu",
        "Data zakupu": "data_zakupu",
        "Numer dokumentu": "numer_dokumentu",
        "Kontrahent": "kontrahent",
        "Netto": "netto",
        "VAT": "vat",
        "Brutto": "brutto",
        "NIP": "nip",
    }
    rename_map = {}
    # najpierw exact
    for orig, newc in colmap.items():
        if orig in df.columns:
            rename_map[orig] = newc
    # potem fuzzy dla brakujących
    for orig, newc in colmap.items():
        if newc in rename_map.values():
            continue
        for c in df.columns:
            if str(c).strip().lower().startswith(orig.lower()[:8]):
                if c not in rename_map:
                    rename_map[c] = newc
                    break
    df = df.rename(columns=rename_map)

    # weryfikacja
    required = ["numer_dokumentu", "kontrahent", "brutto", "nip"]
    missing = [r for r in required if r not in df.columns]
    if missing:
        raise ValueError(f"Brak wymaganych kolumn w Excelu: {missing}")

    # normalizacje
    df["nip"] = df["nip"].apply(clean_nip)
    df["brutto_gr"] = df["brutto"].apply(to_int_grosze)

    def to_ddmmyy(x):
        try:
            dt = pd.to_datetime(x, dayfirst=True, errors="coerce")
            if pd.isna(dt):
                dt = pd.to_datetime(x, errors="coerce")
            if pd.isna(dt):
                return None
            return dt.strftime("%d%m%y")
        except Exception:
            return None

    df["ddmmyy_wyst"] = df["data_wystawienia"].apply(to_ddmmyy) if "data_wystawienia" in df.columns else None
    df["ddmmyy_wplyw"] = df["data_wplywu"].apply(to_ddmmyy)       if "data_wplywu" in df.columns else None

    # duplikaty – ujednolić nazwę kolumny numer_dokumentu
    if df_dups is not None:
        for c in df_dups.columns:
            if str(c).strip().lower().startswith("numer dok"):
                df_dups = df_dups.rename(columns={c: "numer_dokumentu"})
                break

    return df, df_dups


# ============== Matching (subset-sum) ==============

def subset_sum_indices(values: List[int], target: int, max_items: int, tolerance: int) -> Optional[List[int]]:
    """
    Spróbuj złożyć target z values:
      1) exact single
      2) exact two-sum
      3) backtracking ograniczony do max_items (po posortowaniu malejąco)
    Tolerancja: jeśli target nie trafia dokładnie, spróbuj w [target-tolerance, target+tolerance]
    """
    # szybkie ścieżki (±tolerance)
    def exact_try(vals, tgt):
        # single
        for i, v in enumerate(vals):
            if abs(v - tgt) <= tolerance:
                return [i]
        # two-sum
        seen = {}
        for i, v in enumerate(vals):
            need = tgt - v
            # sprawdzamy również zakres tolerancji
            for delta in range(-tolerance, tolerance + 1):
                if (need + delta) in seen:
                    return [seen[need + delta], i]
            seen[v] = i
        return None

    # exact first
    res = exact_try(values, target)
    if res is not None:
        return res

    # backtracking w ograniczeniu
    if len(values) > max_items:
        idx_sorted_base = sorted(range(len(values)), key=lambda i: values[i], reverse=True)[:max_items]
    else:
        idx_sorted_base = list(range(len(values)))

    vals = [values[i] for i in idx_sorted_base]
    idx_sorted = sorted(range(len(vals)), key=lambda i: vals[i], reverse=True)
    vals_sorted = [vals[i] for i in idx_sorted]
    map_back = [idx_sorted_base[i] for i in idx_sorted]

    solution = None

    def dfs(start: int, curr: int, chosen: List[int], tgt: int) -> bool:
        nonlocal solution
        # trafienie w tolerancji
        if abs(curr - tgt) <= tolerance:
            solution = chosen[:]
            return True
        if curr > tgt + max(0, tolerance):  # proste odcięcie
            return False
        for j in range(start, len(vals_sorted)):
            if dfs(j + 1, curr + vals_sorted[j], chosen + [map_back[j]], tgt):
                return True
        return False

    if dfs(0, 0, [], target):
        return solution

    # nie znaleziono
    return None


def match_elixir_to_invoices(inv_df: pd.DataFrame,
                             elx_df: pd.DataFrame,
                             max_items: int,
                             tolerance: int) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Dopasowuje każdą pozycję z ELIXIR (po NIP, ignorując datę) do kombinacji faktur danego NIP-u.
    Zwraca: matched_df, unmatched_elixir, unmatched_invoices
    """
    matches = []
    unmatched_elixir_idx = []
    used_invoice_idx = set()

    # grupujemy faktury po NIP, by ograniczyć zakres backtrackingu
    inv_by_nip = {nip: grp for nip, grp in inv_df.groupby("nip")}

    for ei, er in elx_df.reset_index().iterrows():
        nip = er.get("nip")
        target = int(er["kwota_gr"])
        if not nip or nip not in inv_by_nip:
            unmatched_elixir_idx.append(ei)
            continue

        cand = inv_by_nip[nip]
        cand = cand.loc[~cand.index.isin(used_invoice_idx)]
        if cand.empty:
            unmatched_elixir_idx.append(ei)
            continue

        values = cand["brutto_gr"].tolist()
        ids = cand.index.tolist()
        chosen = subset_sum_indices(values, target, max_items=max_items, tolerance=tolerance)
        if chosen is None:
            unmatched_elixir_idx.append(ei)
            continue

        # zapisujemy dopasowania – każdy wiersz to jedna faktura pod jeden ELIXIR
        for ci in chosen:
            inv_i = ids[ci]
            used_invoice_idx.add(inv_i)
            inv_row = inv_df.loc[inv_i]
            matches.append({
                "elixir_file": er["file"],
                "data_platnosci": er["data_platnosci"],
                "nip": nip,
                "ddmmyy_from_elixir": er.get("ddmmyy"),
                "elixir_kwota_gr": target,
                "numer_dokumentu": inv_row["numer_dokumentu"],
                "kontrahent": inv_row["kontrahent"],
                "brutto_gr": inv_row["brutto_gr"],
                "ddmmyy_wyst": inv_row.get("ddmmyy_wyst"),
                "ddmmyy_wplyw": inv_row.get("ddmmyy_wplyw"),
            })

    matched_df = pd.DataFrame(matches)
    unmatched_elixir = elx_df.reset_index().loc[~elx_df.reset_index().index.isin(unmatched_elixir_idx)].copy()
    unmatched_elixir = elx_df.reset_index().loc[elx_df.reset_index().index.isin(unmatched_elixir_idx)].copy()
    unmatched_invoices = inv_df.loc[~inv_df.index.isin(set(matched_df.index) if matched_df.empty else set())]
    # poprawka: unmatched_invoices to faktury nieużyte:
    unmatched_invoices = inv_df.loc[~inv_df.index.isin(set(matched_df.merge(inv_df.reset_index(), left_on="numer_dokumentu", right_on="numer_dokumentu")["index"]))] if not matched_df.empty else inv_df

    return matched_df, unmatched_elixir, unmatched_invoices

def collect_elixir_files(path_str: str) -> list[Path]:
    """
    Zwraca listę plików .txt:
    - jeśli podano plik -> [plik]
    - jeśli podano katalog -> wszystkie *.txt w katalogu (bez podkatalogów)
    - jeśli podano wzorzec (glob) -> dopasowane pliki
    """
    p = Path(path_str)
    files: list[Path] = []
    if p.is_file():
        files = [p]
    elif p.is_dir():
        files = [Path(x) for x in glob(str(p / "*.txt"))]
    else:
        # wzorzec typu C:\...\temp\*.txt
        files = [Path(x) for x in glob(path_str)]
    if not files:
        raise FileNotFoundError(f"Nie znaleziono plików ELIXIR dla: {path_str}")
    return files


# ============== Raport ==============

def save_report(out_path: Path,
                elx_df: pd.DataFrame,
                inv_df: pd.DataFrame,
                matched_df: pd.DataFrame,
                unmatched_elixir: pd.DataFrame,
                unmatched_invoices: pd.DataFrame,
                dup_df: Optional[pd.DataFrame]) -> None:
    with pd.ExcelWriter(out_path, engine="xlsxwriter") as w:

        # Podsumowanie po NIP – Excel vs ELIXIR
        elx_sum = elx_df.groupby("nip", as_index=False)["kwota_gr"].sum().rename(columns={"kwota_gr": "elixir_sum_gr"})
        inv_sum = inv_df.groupby("nip", as_index=False)["brutto_gr"].sum().rename(columns={"brutto_gr": "excel_sum_gr"})
        recon = elx_sum.merge(inv_sum, on="nip", how="outer")
        recon["elixir_sum_gr"] = recon["elixir_sum_gr"].fillna(0).astype(int)
        recon["excel_sum_gr"] = recon["excel_sum_gr"].fillna(0).astype(int)
        recon["diff_gr"] = recon["excel_sum_gr"] - recon["elixir_sum_gr"]
        recon["elixir_sum"] = recon["elixir_sum_gr"] / 100.0
        recon["excel_sum"] = recon["excel_sum_gr"] / 100.0
        recon["diff_pln"] = recon["diff_gr"] / 100.0
        recon.sort_values(["diff_gr", "nip"], ascending=[False, True]).to_excel(w, sheet_name="NIP_Summary", index=False)

        # Dopasowania
        if not matched_df.empty:
            tmp = matched_df.copy()
            tmp["brutto"] = tmp["brutto_gr"] / 100.0
            tmp["elixir_kwota"] = tmp["elixir_kwota_gr"] / 100.0
            tmp = tmp.sort_values(["elixir_file", "data_platnosci", "nip", "numer_dokumentu"])
            tmp.to_excel(w, sheet_name="Matched", index=False)

            # Kontrola: suma złożonych faktur vs kwota ELIXIR
            agg_check = (
                tmp.groupby(["elixir_file", "data_platnosci", "nip", "ddmmyy_from_elixir", "elixir_kwota_gr"], as_index=False)
                  .agg(suma_brutto_gr=("brutto_gr", "sum"),
                       liczba_faktur=("numer_dokumentu", "count"))
            )
            agg_check["RÓŻNICA_gr"] = agg_check["elixir_kwota_gr"] - agg_check["suma_brutto_gr"]
            agg_check["RÓŻNICA_pln"] = agg_check["RÓŻNICA_gr"] / 100.0
            agg_check.to_excel(w, sheet_name="Matched_Summary", index=False)

        # Niedopasowane
        if not unmatched_elixir.empty:
            tmp = unmatched_elixir.copy()
            sel_cols = ["file", "data_platnosci", "nip", "ddmmyy", "kwota_gr", "szczegoly"]
            for c in sel_cols:
                if c not in tmp.columns:
                    tmp[c] = ""
            tmp["kwota"] = tmp["kwota_gr"] / 100.0
            tmp = tmp[sel_cols + ["kwota"]].sort_values(["file", "data_platnosci"])
            tmp.to_excel(w, sheet_name="Unmatched_ELIXIR", index=False)

        if not unmatched_invoices.empty:
            tmp = unmatched_invoices.copy()
            sel = ["numer_dokumentu", "kontrahent", "nip", "ddmmyy_wyst", "ddmmyy_wplyw", "brutto_gr"]
            for c in sel:
                if c not in tmp.columns:
                    tmp[c] = ""
            tmp["brutto"] = tmp["brutto_gr"] / 100.0
            tmp = tmp[sel + ["brutto"]].sort_values(["nip", "ddmmyy_wyst", "numer_dokumentu"])
            tmp.to_excel(w, sheet_name="Unmatched_Invoices", index=False)

        # Duplikaty, które faktycznie uznano za opłacone (po numerze dokumentu)
        if dup_df is not None and not dup_df.empty and not matched_df.empty:
            dups = dup_df.copy()
            if "numer_dokumentu" not in dups.columns:
                for c in dups.columns:
                    if str(c).strip().lower().startswith("numer dok"):
                        dups = dups.rename(columns={c: "numer_dokumentu"})
                        break
            if "numer_dokumentu" in dups.columns:
                paid_dups = dups.merge(matched_df[["numer_dokumentu"]].drop_duplicates(),
                                       on="numer_dokumentu", how="inner")
                if not paid_dups.empty:
                    paid_dups.to_excel(w, sheet_name="Duplicates_Paid", index=False)

        # Surowe dane do audytu (opcjonalnie, przydaje się)
        elx_df.to_excel(w, sheet_name="ELIXIR_raw", index=False)
        inv_df.to_excel(w, sheet_name="Excel_raw", index=False)


# ============== CLI ==============

def main():
    ap = argparse.ArgumentParser(description="Raport naprawczy: ELIXIR ↔ Excel (faktury).")
    ap.add_argument("--xlsx", required=True, help="Ścieżka do Excela (Arkusz1, opcjonalnie Duplikaty).")
    ap.add_argument("--elixir", required=True, help="Ścieżka do pliku ELIXIR (.txt).")
    ap.add_argument("--out", default="raport_naprawczy.xlsx", help="Plik wyjściowy .xlsx")
    ap.add_argument("--tolerance-grosze", type=int, default=0, help="Tolerancja dopasowania sum (w groszach).")
    ap.add_argument("--max-items", type=int, default=20, help="Limit pozycji na NIP do backtrackingu.")
    ap.add_argument("--encoding", default="iso-8859-2", help="Podpowiedź kodowania pliku ELIXIR.")
    args = ap.parse_args()

    xlsx_path = Path(args.xlsx)
    elx_path = Path(args.elixir)
    out_path = Path(args.out)

    inv_df, dup_df = load_invoices(xlsx_path)
    elixir_files = collect_elixir_files(args.elixir)
    elx_parts = []
    for f in elixir_files:
        df_part = parse_elixir(f, encoding_hint=args.encoding)
        df_part["file"] = f.name  # nazwa pliku w raporcie
        elx_parts.append(df_part)
    elx_df = pd.concat(elx_parts, ignore_index=True) if elx_parts else pd.DataFrame()


    matched, un_elx, un_inv = match_elixir_to_invoices(
        inv_df, elx_df, max_items=args.max_items, tolerance=args.tolerance_grosze
    )
    save_report(out_path, elx_df, inv_df, matched, un_elx, un_inv, dup_df)

    print("✅ Zapisano:", out_path)
    print("   Dopasowane faktury:", len(matched))
    print("   Niedopasowane wiersze ELIXIR:", len(un_elx))
    print("   Niedopasowane faktury:", len(un_inv))


if __name__ == "__main__":
    main()
