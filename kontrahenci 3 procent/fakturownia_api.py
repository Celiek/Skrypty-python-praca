import json
import logging
import os
import re
from datetime import datetime
from datetime import timedelta, date
from pathlib import Path
from typing import Dict

import requests
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from filelock import FileLock
from tqdm import tqdm

from utils import _safe_name

from db_ops import (
    get_addresses_from_db,
    get_invoice_details,
    reserve_commission_invoice,
    finalize_commission_invoice,
)
from utils import clean_nip

load_dotenv()
API_KEY = os.getenv("API_KEY")

# plik przechowujący liczbę faktur
BASE_DIR = Path(__file__).resolve().parent
COUNTER_FILE = BASE_DIR / "utils" / "licznik_faktur.json"
LOCK_FILE = COUNTER_FILE.with_suffix(".lock")

# Prefixy spółek
COMPANY_PREFIX = {
    "SHUMEE": "SM",
    "GREATSTORE": "GS",
    "EXTRASTORE": "EX",
    # spółka testowa:
    "TSM3": "TSM3"
}

FAKTUROWNIA_URL = os.getenv("FAKTUROWNIA_URL", "https://shumee.fakturownia.pl")
FAKTUROWNIA_API = os.getenv("FAKTUROWNIA_API", "https://shumee.fakturownia.pl")
FAKTUROWNIA_TOKEN = os.getenv("FAKTUROWNIA_TOKEN")

MIESIACE_PL = {
    "January": "styczeń",
    "February": "luty",
    "March": "marzec",
    "April": "kwiecień",
    "May": "maj",
    "June": "czerwiec",
    "July": "lipiec",
    "August": "sierpień",
    "September": "wrzesień",
    "October": "październik",
    "November": "listopad",
    "December": "grudzień"
}


def parse_address(addr: str):
    """Rozdziela adres z bazy (np. 'ul. Warszawska 12 | 00-123 Warszawa') na ulica,nr , miasto."""
    if not addr:
        return "", "", ""
    addr = str(addr).replace("|", ",").replace("–", "-")
    parts = [a.strip() for a in addr.split(",") if a.strip()]

    street = ""
    post_code = ""
    city = ""

    # wysukiwanie kodu pocztowego (np. XX-YYY)
    for p in parts:
        if re.match(r"\d{2}-\d{3}", p):
            post_code = re.search(r"\d{2}-\d{3}", p).group()
            city = p.replace(post_code, "").strip(" ,")
        elif not street:
            street = p
        elif not city and not post_code:
            city = p

    # print(f"[DEBUG] ulica: {street}, postcode: {post_code},miasto: {city}")

    return street, post_code, city

def _load_counter() -> Dict[str, int]:
    if COUNTER_FILE.exists():
        try:
            # poprawka: json.load() na  json.loads(COUNTER_FILE.read_text())
            return json.loads(COUNTER_FILE.read_text())
        except json.JSONDecodeError:
            logging.error("⚠️ licznik_faktur.json uszkodzony — start od zera")
            return {}
    return {}

def _save_counter(counter: Dict[str, int]):
    COUNTER_FILE.write_text(json.dumps(counter, indent=4))

def get_invoice_number(company: str, previous_month: date) -> str:
    """
    Zwraca kolejny numer faktury w formacie:
        X/MM/YYYY
    z podziałem na spółki i miesiące.
    """
    with FileLock(str(LOCK_FILE)):
        company_key = company.upper().strip()
        prefix = COMPANY_PREFIX.get(company_key, company_key[:3])  # fallback

        key = f"{company_key}/{previous_month.year}-{previous_month.month:02d}"

        counter = _load_counter()
        current_no = counter.get(key, 0) + 1
        counter[key] = current_no
        _save_counter(counter)
        issue_date = datetime.now().date()
        # na czas 1 stycznia 2025
        #year_yy = str(previous_month.year)[-2:]
        year_yy = str(issue_date.year)[-2:]
        return f"{current_no}/{issue_date.month}/{year_yy}/{prefix}"

def get_invoice_public_url(invoice_id: int) -> str | None:
    url = f"{FAKTUROWNIA_URL}/invoices/{invoice_id}.json"
    try:
        r = requests.get(url, params={"api_token": API_KEY}, timeout=30)
        r.raise_for_status()
        data = r.json()
        for key in ("view_url", "public_url", "print_url", "download_url"):
            if data.get(key):
                return data[key]
    except Exception as e:
        logging.error(f"[Fakturownia] Nie udało się pobrać linku do faktury {invoice_id}: {e}")
    return None

def get_invoice_number_from_api(invoice_id: int) -> str:
    """Dociąga numer faktury z API Fakturowni po jej ID."""
    try:
        url = f"{FAKTUROWNIA_API}/invoices/{invoice_id}.json?api_token={FAKTUROWNIA_TOKEN}"
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        return data.get("number") or data.get("full_number") or f"FAKTURA-{invoice_id}"
    except Exception as e:
        logging.warning(f"[FAKTUROWNIA] Nie udało się pobrać numeru faktury {invoice_id}: {e}")
        return f"FAKTURA-{invoice_id}"


def get_faktur(
    date_from: str,
    date_to: str,
    company_suffix: tuple[str, ...],
    out_root="faktury",
    out_ready_root="raporty_gotowe",
):

    base_url = f"{FAKTUROWNIA_URL}/invoices.json"
    api_token = API_KEY

    all_invoices = []
    page = 1

    # ===============================
    # POBIERANIE LISTY FAKTUR
    # ===============================

    with tqdm(desc="📡 Pobieranie listy faktur", unit="strona") as pbar:
        while True:
            params = {
                "api_token": api_token,
                "period": "more",
                "date_from": date_from,
                "date_to": date_to,
                "per_page": 100,
                "page": page,
            }

            r = requests.get(base_url, params=params, timeout=10)
            r.raise_for_status()
            data = r.json()

            if not data:
                break

            all_invoices.extend(data)
            page += 1
            pbar.update(1)

    # ==================================================
    # FILTR PO SUFFIXIE SPÓŁKI ( konćówce nazyw faktury)
    # ==================================================
    filtered = [
        inv for inv in all_invoices
        if str(inv.get("number", ""))
        .strip()
        .upper()
        .endswith(company_suffix)
    ]

    logging.info(
        f"[Fakturownia] Znaleziono {len(filtered)} faktur "
        f"({company_suffix}) między {date_from}–{date_to}"
    )

    today_dir = datetime.today().strftime("%Y-%m-%d")

    out_dir = os.path.join(out_root, today_dir)
    ready_dir = os.path.join(out_ready_root, today_dir)

    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(ready_dir, exist_ok=True)

    pobrane = []

    # ==================================
    # POBIERANIE faktur w formacie PDF
    # ==================================
    for inv in tqdm(
        filtered,
        desc="Pobieranie faktur kontrahentów",
        unit="fv",
        ncols=100,
    ):
        inv_id = inv.get("id")
        pdf_url = f"{FAKTUROWNIA_URL}/invoices/{inv_id}.pdf"

        nip = inv.get("buyer_tax_no", "brak_nip")
        name = inv.get("buyer_name", "brak_nazwy")
        num = inv.get("number", "brak_numeru")

        # standardyzcja nazwy pliku pdf dla nas
        # nazwa kontrahenta + nip + numer faktury
        std_name = _safe_name(f"{name}_{nip}_{num}.pdf")
        std_path = os.path.join(out_dir, std_name)

        # standaryzacja nazwy pliku pdf dla kontrahenta
        # tylko nip.pdf
        ready_name = f"{nip}.pdf"
        ready_path = os.path.join(ready_dir, ready_name)

        try:
            with requests.get(
                pdf_url,
                params={"api_token": api_token},
                stream=True,
                timeout=60,
            ) as r:
                r.raise_for_status()

                content = b"".join(r.iter_content(8192))

                # zapis faktury dla nas do pliku
                with open(std_path, "wb") as f:
                    f.write(content)

                # zapis faktury dla kontrahenta
                with open(ready_path, "wb") as f:
                    f.write(content)

            pobrane.append({
                "id": inv_id,
                "buyer_tax_no": nip,
                "number": num,
                "path": std_path,
                "ready_path": ready_path,
                "ok": True,
            })

        except Exception as e:
            logging.error(f"[PDF] Błąd pobierania {num}: {e}")
            pobrane.append({
                "id": inv_id,
                "buyer_tax_no": nip,
                "number": num,
                "path": std_path,
                "ready_path": ready_path,
                "ok": False,
                "error": str(e),
            })

    return filtered, pobrane


def dodaj_faktury(spolka: str, items: list[dict], department_id: int, issue_date: datetime.date) -> list[dict]:
    """
    Wystawia faktury przez API Fakturowni i zwraca ich dane.
    Zabezpieczenie anty-duplikat: blokada w DB (spółka+nip+okres).
    """

    api_token = os.getenv("API_KEY")
    base_url = os.getenv("FAKTUROWNIA_URL", "https://shumee.fakturownia.pl")

    # {NIP: adres}
    adresy = get_addresses_from_db()

    today = datetime.today()
    poprzedni = today - relativedelta(months=1)   # “okres prowizji” = poprzedni miesiąc
    okres = f"{poprzedni.year}-{poprzedni.month:02d}"
    payment_to = today + timedelta(days=14)

    url = f"{base_url}/invoices.json"
    headers = {"Accept": "application/json", "Content-Type": "application/json"}

    # --- deduplikacja items po NIP (żeby jeden NIP nie poszedł dwa razy do fakturowni) ---
    dedup = {}
    for it in items:
        nip_key = clean_nip(it.get("buyer_tax_no"))
        if not nip_key:
            continue
        dedup[nip_key] = it  # ostatni wygrywa
    items_unique = list(dedup.values())

    results = []
    pominieci = 0

    with requests.Session() as s:
        s.headers.update(headers)

        for it in items_unique:
            nip_clean = clean_nip(it.get("buyer_tax_no"))
            if not nip_clean:
                results.append({"nip": it.get("buyer_tax_no"), "ok": False, "error": "Brak/nieprawidłowy NIP"})
                continue

            # 1) DB LOCK / niezmienna
            # jeśli już było wystawione (lub inny proces zarezerwował) -> SKIP
            if not reserve_commission_invoice(spolka, nip_clean, okres):
                results.append({"nip": nip_clean, "ok": False, "skipped": True, "reason": "already_issued_or_reserved"})
                continue

            # 2) adres
            addr_raw = adresy.get(nip_clean)
            if not addr_raw:
                logging.warning(f"[FAKTUROWNIA] ⚠️ Pominięto NIP {nip_clean} – brak adresu w bazie.")
                pominieci += 1
                results.append({"nip": nip_clean, "ok": False, "skipped": True, "reason": "no_address"})
                continue

            street, post_code, city = parse_address(addr_raw)

            # 3) numer FV
            nr_faktury = get_invoice_number(spolka, poprzedni)

            payload = {
                "api_token": api_token,
                "invoice": {
                    "kind": "vat",
                    "number": nr_faktury,
                    "issue_date": today.strftime("%Y-%m-%d"),
                    "sell_date": issue_date.strftime("%Y-%m-%d"),
                    "payment_to": payment_to.strftime("%Y-%m-%d"),
                    "department_id": department_id,
                    "buyer_name": it["buyer_name"],
                    "buyer_tax_no": nip_clean,   # sformatowany nip
                    "buyer_email": it.get("buyer_email"),
                    "buyer_street": street,
                    "buyer_post_code": post_code,
                    "buyer_city": city,
                    "positions": [{
                        "name": f"Prowizja 3% od sprzedanych towarów za {MIESIACE_PL[poprzedni.strftime('%B')]} {poprzedni.year}",
                        "tax": 23,
                        "total_price_gross": it["amount_gross"],
                        "total_price_net": it["amount_net"],
                        "quantity": 1
                    }]
                }
            }

            try:
                r = s.post(url, json=payload, timeout=30)

                # sprawdzanie odpowiedzi serwera na dane
                # przesłane od klienta
                # 200 Jest okej
                # 300 jest błąd
                if 200 <= r.status_code < 300:
                    data = r.json()
                    faktura_id = data.get("id")
                    details = get_invoice_details(faktura_id)
                    nr = details.get("number") or details.get("full_number")

                    # 4) FINALIZE w DB
                    try:
                        finalize_commission_invoice(spolka, nip_clean, okres, int(faktura_id), nr)
                    except Exception as e:
                        logging.exception(f"[IDEMPOTENCY] Nie udało się zapisać finalize do DB: {e}")

                    results.append({
                        "ok": True,
                        "id": faktura_id,
                        "number": nr,
                        "issue_date": details.get("issue_date"),
                        "netto": details.get("price_net"),
                        "vat": details.get("price_tax"),
                        "brutto": details.get("price_gross"),
                        "nip": details.get("buyer_tax_no"),
                        "buyer_name": details.get("buyer_name"),
                        "buyer_address": details.get("buyer_street"),
                        "buyer_post_code": details.get("buyer_post_code"),
                        "buyer_city": details.get("buyer_city"),
                    })

                    logging.info(
                        f"[FAKTUROWNIA] ✅ Faktura dla {it['buyer_name']} ({nip_clean}) "
                        f"→ {street}, {post_code} {city} (nr: {nr})"
                    )

                else:
                    logging.error(f"[FAKTUROWNIA] ❌ Błąd {r.status_code}: {r.text[:200]}")
                    results.append({"nip": nip_clean, "ok": False, "error": r.text})

            except Exception as e:
                logging.error(f"[FAKTUROWNIA] Błąd wysyłki dla {nip_clean}: {e}")
                results.append({"nip": nip_clean, "ok": False, "error": str(e)})

    logging.info(f"[FAKTUROWNIA] Pominięto {pominieci} kontrahentów bez adresu w bazie.")
    return results