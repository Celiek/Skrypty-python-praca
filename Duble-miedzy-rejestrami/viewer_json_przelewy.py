# viewer_json_przelewy.py
import json
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional


# =============== Pomocnicze ===============

def parse_date_iso(s: str) -> Optional[datetime]:
    """Przyjmuje 'YYYY-MM-DD' i zwraca datetime albo None."""
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except Exception:
        return None

def fmt_pln(v: float) -> str:
    try:
        return f"{float(v):,.2f}".replace(",", " ").replace(".", ",")
    except Exception:
        return "0,00"

def coerce_float(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0

def norm_group(group: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalizuje pojedynczą grupę tak, aby miała:
      group_id, nip, kontrahent, data_wystawienia (YYYY-MM-DD), data_platnosci (YYYY-MM-DD),
      suma: netto, vat, brutto, netto_gr, vat_gr, brutto_gr
      pozycje: [{numer_dokumentu, data_wystawienia, netto, vat, brutto, nip, kontrahent}, ...]
    """
    g = dict(group)  # płytka kopia

    # daty - dopuszczamy różne warianty w wejściu
    def to_iso(d):
        if not d:
            return ""
        d = str(d)
        if len(d) == 8 and d.isdigit():  # YYYYMMDD
            return f"{d[:4]}-{d[4:6]}-{d[6:8]}"
        # jeśli wygląda już jak YYYY-MM-DD
        if "-" in d and len(d) >= 10:
            return d[:10]
        return d  # zostaw jak jest, filtr i tak sprawdzi poprawność

    data_w = to_iso(g.get("data_wystawienia") or g.get("data"))
    data_p = to_iso(g.get("data_platnosci") or g.get("platnosc"))

    suma = g.get("suma") or {}
    suma_norm = {
        "netto":      coerce_float(suma.get("netto", 0)),
        "vat":        coerce_float(suma.get("vat", 0)),
        "brutto":     coerce_float(suma.get("brutto", 0)),
        "netto_gr":   int(suma.get("netto_gr", 0) or 0),
        "vat_gr":     int(suma.get("vat_gr", 0) or 0),
        "brutto_gr":  int(suma.get("brutto_gr", 0) or 0),
    }

    items = []
    for it in g.get("pozycje", []):
        items.append({
            "numer_dokumentu": str(it.get("numer_dokumentu", "")),
            "data_wystawienia": str(it.get("data_wystawienia", "")),
            "netto": coerce_float(it.get("netto", 0)),
            "vat": coerce_float(it.get("vat", 0)),
            "brutto": coerce_float(it.get("brutto", 0)),
            "nip": str(it.get("nip", "")),
            "kontrahent": str(it.get("kontrahent", "")),
        })

    return {
        "group_id":     str(g.get("group_id", "")) or f"{g.get('nip','') or ''}:{data_w}",
        "nip":          str(g.get("nip", "")),
        "kontrahent":   str(g.get("kontrahent", "")),
        "data_wystawienia": data_w,
        "data_platnosci":   data_p,
        "suma":         suma_norm,
        "pozycje":      items,
    }

def load_groups_from_json(path: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Ładuje plik JSON i zwraca:
      meta (dict) oraz groups (lista znormalizowanych grup).
    Obsługuje:
      - nowy format: {"meta": {...}, "groups": [ ... ]}
      - stary format: [ ... ]  (lista grup w korzeniu)
    """
    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)

    meta = {}
    groups_raw = []

    if isinstance(obj, dict) and "groups" in obj:
        meta = obj.get("meta", {}) or {}
        groups_raw = obj.get("groups") or []
    elif isinstance(obj, list):
        meta = {}
        groups_raw = obj
    else:
        raise ValueError("Nieznany format JSON (oczekiwano {'groups': [...]} lub listy grup).")

    groups = [norm_group(g) for g in groups_raw if isinstance(g, dict)]
    return meta, groups


# =============== GUI ===============

class JsonViewerApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Przelewy JSON — podgląd (grupy → pozycje)")
        self.geometry("1150x720")

        # dane
        self.meta: Dict[str, Any] = {}
        self.all_groups: List[Dict[str, Any]] = []     # wszystkie z pliku
        self.filtered_groups: List[Dict[str, Any]] = []  # po filtrach

        self._build_ui()

    # -------- UI --------
    def _build_ui(self):
        # Top bar
        top = ttk.Frame(self, padding=8)
        top.pack(side=tk.TOP, fill=tk.X)

        ttk.Button(top, text="Otwórz JSON…", command=self.on_open).pack(side=tk.LEFT, padx=(0, 8))

        ttk.Label(top, text="Filtr (NIP/kontrahent):").pack(side=tk.LEFT)
        self.filter_text = ttk.Entry(top, width=26)
        self.filter_text.pack(side=tk.LEFT, padx=4)

        ttk.Label(top, text="Data od (YYYY-MM-DD):").pack(side=tk.LEFT, padx=(12, 0))
        self.date_from = ttk.Entry(top, width=12)
        self.date_from.pack(side=tk.LEFT, padx=4)

        ttk.Label(top, text="do:").pack(side=tk.LEFT)
        self.date_to = ttk.Entry(top, width=12)
        self.date_to.pack(side=tk.LEFT, padx=4)

        ttk.Label(top, text="Brutto min:").pack(side=tk.LEFT, padx=(12, 0))
        self.brutto_min = ttk.Entry(top, width=10)
        self.brutto_min.pack(side=tk.LEFT, padx=4)

        ttk.Label(top, text="max:").pack(side=tk.LEFT)
        self.brutto_max = ttk.Entry(top, width=10)
        self.brutto_max.pack(side=tk.LEFT, padx=4)

        ttk.Button(top, text="Zastosuj filtr", command=self.apply_filters).pack(side=tk.LEFT, padx=(12, 4))
        ttk.Button(top, text="Wyczyść", command=self.clear_filters).pack(side=tk.LEFT, padx=4)
        ttk.Button(top, text="Rozwiń wszystko", command=lambda: self._expand_collapse_all(True)).pack(side=tk.LEFT, padx=(12, 4))
        ttk.Button(top, text="Zwiń wszystko", command=lambda: self._expand_collapse_all(False)).pack(side=tk.LEFT, padx=4)

        # Środek — Treeview
        mid = ttk.Frame(self, padding=(8, 0, 8, 0))
        mid.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

        cols = ("Data", "NIP", "Kontrahent", "Netto", "VAT", "Brutto", "Pozycji")
        self.tree = ttk.Treeview(mid, columns=cols, show="tree headings")  # WAŻNE: tree + headings
        self.tree.heading("#0", text="Grupa / Pozycje")
        self.tree.column("#0", width=420, anchor="w")

        for c in cols:
            self.tree.heading(c, text=c)
            anchor = "w" if c in ("Data", "NIP", "Kontrahent") else "e"
            width = 90 if c in ("Netto", "VAT", "Brutto") else (80 if c == "Pozycji" else 120)
            self.tree.column(c, width=width, anchor=anchor, stretch=True)

        vsb = ttk.Scrollbar(mid, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(mid, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscroll=vsb.set, xscroll=hsb.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        mid.rowconfigure(0, weight=1)
        mid.columnconfigure(0, weight=1)

        # Double-click toggle expand/collapse
        self.tree.bind("<Double-1>", self._on_row_double_click)

        # Status / suma
        bottom = ttk.Frame(self, padding=8)
        bottom.pack(side=tk.BOTTOM, fill=tk.X)

        self.status_lbl = ttk.Label(bottom, text="Wczytaj plik JSON…")
        self.status_lbl.pack(side=tk.LEFT)

    # -------- Handlery --------
    def on_open(self):
        path = filedialog.askopenfilename(
            title="Wybierz plik JSON z przelewami",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
        )
        if not path:
            return
        try:
            self.meta, self.all_groups = load_groups_from_json(path)
        except Exception as e:
            messagebox.showerror("Błąd", f"Nie udało się wczytać pliku:\n{e}")
            return

        # reset filtrów
        self.filter_text.delete(0, tk.END)
        self.date_from.delete(0, tk.END)
        self.date_to.delete(0, tk.END)
        self.brutto_min.delete(0, tk.END)
        self.brutto_max.delete(0, tk.END)

        self.apply_filters()
        company = self.meta.get("company") or ""
        self._set_status(f"Wczytano: {Path(path).name}  •  Firma: {company or '—'}  •  Grup: {len(self.all_groups)}")

    def apply_filters(self):
        txt = (self.filter_text.get() or "").strip().lower()
        d_from = parse_date_iso(self.date_from.get().strip())
        d_to = parse_date_iso(self.date_to.get().strip())
        b_min = self._parse_amount(self.brutto_min.get().strip())
        b_max = self._parse_amount(self.brutto_max.get().strip())

        out: List[Dict[str, Any]] = []
        for g in self.all_groups:
            # filtr tekstowy (nip/kontrahent)
            if txt:
                hay = f"{g.get('nip','')}".lower() + " " + f"{g.get('kontrahent','')}".lower()
                if txt not in hay:
                    continue

            # filtr daty (po dacie wystawienia grupy)
            gd = parse_date_iso(g.get("data_wystawienia", ""))
            if d_from and (not gd or gd < d_from):
                continue
            if d_to and (not gd or gd > d_to):
                continue

            # filtr brutto (po sumie grupy)
            brutto = float(g.get("suma", {}).get("brutto", 0.0))
            if b_min is not None and brutto < b_min:
                continue
            if b_max is not None and brutto > b_max:
                continue

            out.append(g)

        self.filtered_groups = out
        self._rebuild_tree()

    def clear_filters(self):
        self.filter_text.delete(0, tk.END)
        self.date_from.delete(0, tk.END)
        self.date_to.delete(0, tk.END)
        self.brutto_min.delete(0, tk.END)
        self.brutto_max.delete(0, tk.END)
        self.apply_filters()

    def _parse_amount(self, s: str) -> Optional[float]:
        if not s:
            return None
        s = s.replace(" ", "").replace(",", ".")
        try:
            return float(s)
        except Exception:
            return None

    # -------- Tree --------
    def _rebuild_tree(self):
        self.tree.delete(*self.tree.get_children())

        sum_brutto = 0.0
        for g in self.filtered_groups:
            parent_iid = g.get("group_id") or f"{g.get('nip','')}:{g.get('data_wystawienia','')}"
            nip = g.get("nip", "")
            kontr = g.get("kontrahent", "")
            data_iso = g.get("data_wystawienia", "")
            brutto_f = float(g.get("suma", {}).get("brutto", 0.0))
            netto_f  = float(g.get("suma", {}).get("netto", 0.0))
            vat_f    = float(g.get("suma", {}).get("vat", 0.0))
            cnt      = len(g.get("pozycje", []))
            sum_brutto += brutto_f

            parent_text = f"{data_iso}  |  {nip or '—'}  |  {kontr or '—'}"
            self.tree.insert(
                "", "end", iid=parent_iid, text=parent_text,
                values=(data_iso, nip, kontr, fmt_pln(netto_f), fmt_pln(vat_f), fmt_pln(brutto_f), cnt),
                open=False
            )

            # dzieci (pozycje)
            for idx, it in enumerate(g.get("pozycje", []), start=1):
                numer = it.get("numer_dokumentu", "")
                data_w = it.get("data_wystawienia", "")
                n2 = fmt_pln(it.get("netto", 0.0))
                v2 = fmt_pln(it.get("vat", 0.0))
                b2 = fmt_pln(it.get("brutto", 0.0))

                child_text = f"— {numer}  ({data_w})"
                self.tree.insert(
                    parent_iid, "end", iid=f"{parent_iid}::{idx}", text=child_text,
                    values=("", "", "", n2, v2, b2, "")
                )

        self._set_status(f"Widocznych grup: {len(self.filtered_groups)}  •  Suma brutto: {fmt_pln(sum_brutto)}")

    def _expand_collapse_all(self, expand: bool):
        for iid in self.tree.get_children(""):
            self.tree.item(iid, open=expand)

    def _on_row_double_click(self, _evt):
        sel = self.tree.focus()
        if not sel:
            return
        # jeśli to rodzic ma dzieci — przełącz open
        if self.tree.get_children(sel):
            self.tree.item(sel, open=not self.tree.item(sel, "open"))

    # -------- Status --------
    def _set_status(self, text: str):
        self.status_lbl.config(text=text)


if __name__ == "__main__":
    app = JsonViewerApp()
    app.mainloop()
