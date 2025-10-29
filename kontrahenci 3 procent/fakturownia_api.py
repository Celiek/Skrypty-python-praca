import os
import logging
import requests
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from main import API_KEY
from utils import _safe_name

API_KEY = os.getenv("API_KEY")

FAKTUROWNIA_URL = os.getenv("FAKTUROWNIA_URL", "https://shumee.fakturownia.pl")

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

def dodaj_faktury(spolka: str, items: list[dict], department_id: int):
    url = f"{FAKTUROWNIA_URL}/invoices.json"
    headers = {"Content-Type": "application/json"}
    today = datetime.today()
    poprzedni = today - relativedelta(months=1)
    results = []
    for it in items:
        payload = {
            "api_token": API_KEY,
            "invoice": {
                "kind": "vat",
                "sell_date": today.strftime("%Y-%m-%d"),
                "issue_date": today.strftime("%Y-%m-%d"),
                "payment_to": (today + timedelta(days=14)).strftime("%Y-%m-%d"),
                "buyer_name": it["buyer_name"],
                "buyer_tax_no": it["buyer_tax_no"],
                "department_id": department_id,
                "positions": [{
                    "name": f"Prowizja 3% za {poprzedni.strftime('%B %Y')}",
                    "tax": 23,
                    "total_price_gross": it["amount_gross"],
                    "quantity": 1
                }]
            }
        }
        try:
            r = requests.post(url, json=payload, headers=headers, timeout=30)
            r.raise_for_status()
            data = r.json()
            inv_id = data.get("id")
            link = get_invoice_public_url(inv_id)
            results.append({"nip": it["buyer_tax_no"], "id": inv_id, "ok": True, "link": link})
        except Exception as e:
            results.append({"nip": it["buyer_tax_no"], "ok": False, "error": str(e)})
    return results