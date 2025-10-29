import os
import logging
import re

import requests
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from db_ops import get_addresses_from_db
from utils import _safe_name

from dotenv import load_dotenv
load_dotenv()
API_KEY = os.getenv("API_KEY")


FAKTUROWNIA_URL = os.getenv("FAKTUROWNIA_URL", "https://shumee.fakturownia.pl")


def parse_address(addr: str):
    """Rozdziela adres z bazy (np. 'ul. Warszawska 12 | 00-123 Warszawa') na street, post_code, city."""
    if not addr:
        return "", "", ""
    addr = str(addr).replace("|", ",").replace("–", "-")
    parts = [a.strip() for a in addr.split(",") if a.strip()]

    street = ""
    post_code = ""
    city = ""

    # szukamy kodu pocztowego (np. 00-123)
    for p in parts:
        if re.match(r"\d{2}-\d{3}", p):
            post_code = re.search(r"\d{2}-\d{3}", p).group()
            city = p.replace(post_code, "").strip(" ,")
        elif not street:
            street = p
        elif not city and not post_code:
            city = p

    return street, post_code, city

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

def get_faktur(date_from: str, date_to: str):
    base_url = f"{FAKTUROWNIA_URL}/invoices.json"
    api_token = API_KEY
    all_invoices = []
    page = 1
    while True:
        params = {
            "api_token": api_token,
            "period": "more",
            "date_from": date_from,
            "date_to": date_to,
            "per_page": 100,
            "page": page,
        }
        r = requests.get(base_url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        if not data:
            break
        all_invoices.extend(data)
        page += 1
    filtered = [inv for inv in all_invoices if str(inv.get("number", "")).strip().upper().endswith(("/SM","/GS","/EX","/TSM3"))]
    logging.info(f"[Fakturownia] Znaleziono {len(filtered)} faktur między {date_from}–{date_to}")
    out_dir = os.path.join("faktury", datetime.today().strftime("%Y-%m-%d"))
    os.makedirs(out_dir, exist_ok=True)
    pobrane = []
    for inv in filtered:
        inv_id = inv.get("id")
        pdf_url = f"{FAKTUROWNIA_URL}/invoices/{inv_id}.pdf"
        nip = inv.get("buyer_tax_no")
        name = inv.get("buyer_name")
        num = inv.get("number")
        out_path = os.path.join(out_dir, f"{_safe_name(f'{name}_{nip}_{num}')}.pdf")
        try:
            with requests.get(pdf_url, params={"api_token": api_token}, stream=True, timeout=60) as r:
                r.raise_for_status()
                with open(out_path, "wb") as f:
                    for chunk in r.iter_content(8192):
                        f.write(chunk)
            pobrane.append({"id": inv_id, "buyer_tax_no": nip, "path": out_path, "ok": True})
        except Exception as e:
            logging.error(f"[PDF] Błąd pobierania {num}: {e}")
    return filtered, pobrane

def dodaj_faktury(spolka: str, items: list[dict], department_id: int) -> list[dict]:
    import requests, os
    from datetime import datetime, timedelta
    from dateutil.relativedelta import relativedelta

    api_token = os.getenv("API_KEY")
    base_url = os.getenv("FAKTUROWNIA_URL", "https://shumee.fakturownia.pl")

    adresy = get_addresses_from_db()

    today = datetime.today()
    poprzedni = today - relativedelta(months=1)
    payment_to = today + timedelta(days=14)

    url = f"{base_url}/invoices.json"
    headers = {"Accept": "application/json", "Content-Type": "application/json"}

    results = []
    with requests.Session() as s:
        s.headers.update(headers)
        for it in items:
            nip_clean = str(it["buyer_tax_no"]).replace("PL", "").strip()
            addr_raw = adresy.get(nip_clean, "")
            street, post_code, city = parse_address(addr_raw)

            payload = {
                "api_token": api_token,
                "invoice": {
                    "kind": "vat",
                    "issue_date": today.strftime("%Y-%m-%d"),
                    "sell_date": today.strftime("%Y-%m-%d"),
                    "payment_to": payment_to.strftime("%Y-%m-%d"),
                    "department_id": department_id,
                    "buyer_name": it["buyer_name"],
                    "buyer_tax_no": it["buyer_tax_no"],
                    "buyer_email": it.get("buyer_email"),
                    "buyer_street": street,
                    "buyer_post_code": post_code,
                    "buyer_city": city,
                    "positions": [{
                        "name": f"Prowizja 3% od sprzedanych towarów za {poprzedni.strftime('%B %Y')}",
                        "tax": 23,
                        "total_price_gross": it["amount_gross"],
                        "quantity": 1
                    }]
                }
            }

            try:
                r = s.post(url, json=payload, timeout=30)
                if 200 <= r.status_code < 300:
                    data = r.json()
                    results.append({
                        "nip": it["buyer_tax_no"],
                        "ok": True,
                        "id": data.get("id"),
                        "buyer_street": street,
                        "buyer_post_code": post_code,
                        "buyer_city": city
                    })
                    logging.info(f"[FAKTUROWNIA] ✅ Faktura dla {it['buyer_name']} ({nip_clean}) z adresem: {street}, {post_code} {city}")
                else:
                    logging.error(f"[FAKTUROWNIA] ❌ Błąd {r.status_code}: {r.text[:200]}")
                    results.append({"nip": it["buyer_tax_no"], "ok": False, "error": r.text})
            except Exception as e:
                logging.error(f"[FAKTUROWNIA] Błąd wysyłki dla {it['buyer_tax_no']}: {e}")
                results.append({"nip": it["buyer_tax_no"], "ok": False, "error": str(e)})

    return results

