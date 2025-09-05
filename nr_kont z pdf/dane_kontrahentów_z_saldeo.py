# saldeo_contractors_to_json.py
# Pobiera kontrahentów z API Saldeo dla wielu firm i zapisuje dane do plików JSON.
# Wymaga: pip install -r requirements.txt

import os
import json
import time
import uuid
import hashlib
import logging
import argparse
from urllib.parse import quote_plus
from typing import List, Dict, Any, Optional

import requests
import xmltodict
from dotenv import load_dotenv

# ----------------------------
# Helpers
# ----------------------------

def fetch_companies(username: str, api_token: str, session: Optional[requests.Session] = None) -> List[Dict[str, Any]]:
    """Pobiera listę firm (company.list) i zwraca jako listę słowników."""
    sess = session or requests.Session()
    req_id = unique_req_id()
    params = {
        "username": username,
        "req_id": req_id,
    }
    params["req_sig"] = build_req_sig(params, api_token)

    resp = sess.get(
        "https://saldeo.brainshare.pl/api/xml/1.0/company/list",
        params=params,
        headers={"Accept-Encoding": "gzip"},
        timeout=30,
    )
    resp.raise_for_status()
    print("=== RAW company.list ===")
    print(resp.text)
    print("========================")
    parsed = xmltodict.parse(resp.text)
    companies = parsed.get("RESPONSE", {}).get("COMPANIES", {}).get("COMPANY", [])
    if isinstance(companies, dict):
        companies = [companies]
    return json.loads(json.dumps(companies))

def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

def url_encode_per_spec(s: str) -> str:
    """Specyficzne kodowanie wymagane przez Saldeo."""
    enc = quote_plus(s, safe="*")
    enc = enc.replace("~", "%7E")
    return enc.upper() if enc.startswith("%") else enc

def build_req_sig(params: Dict[str, str], api_token: str) -> str:
    """Buduje req_sig wg dokumentacji Saldeo."""
    base = "".join(f"{k}={params[k]}" for k in sorted(params))
    enc = quote_plus(base, safe="*").replace("~", "%7E")
    return hashlib.md5((enc + api_token).encode("utf-8")).hexdigest()

def unique_req_id() -> str:
    ts = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    return f"{ts}-{uuid.uuid4().hex[:8]}"

def parse_contractors_xml(xml_text: str) -> List[Dict[str, Any]]:
    parsed = xmltodict.parse(xml_text)
    contractors = parsed.get("RESPONSE", {}).get("CONTRACTORS", {}).get("CONTRACTOR", [])
    if isinstance(contractors, dict):
        contractors = [contractors]
    return json.loads(json.dumps(contractors))

def fetch_contractors_for_company(base_url: str, company_program_id: str, username: str, api_token: str,
                                  session: Optional[requests.Session] = None) -> List[Dict[str, Any]]:
    sess = session or requests.Session()
    req_id = unique_req_id()
    params = {
        "company_program_id": company_program_id,
        "username": username,
        "req_id": req_id,
    }
    params["req_sig"] = build_req_sig(params, api_token)

    resp = sess.get(base_url, params=params, headers={"Accept-Encoding": "gzip"}, timeout=30)
    resp.raise_for_status()
    print(resp.text)
    return parse_contractors_xml(resp.text)

def save_json(data: Any, path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# ----------------------------
# Main
# ----------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--companies", help="Lista company_program_id, np. ID1,ID2,ID3")
    parser.add_argument("--output-dir", default=".", help="Folder wyjściowy")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()
    base_url = "https://saldeo.brainshare.pl/api/xml/1.0/contractor/list"

    load_dotenv()
    setup_logging(args.verbose)

    username = os.getenv("SALDEO_USERNAME")
    api_token = os.getenv("SALDEO_API_TOKEN")

    if not username or not api_token:
        raise SystemExit("Brak danych logowania (SALDEO_USERNAME i SALDEO_API_TOKEN w .env)")

    session = requests.Session()

    # 👉 Wypisujemy listę firm
    logging.info("Pobieram listę firm...")
    companies_list = fetch_companies(username, api_token, session=session)
    print("\n=== LISTA FIRM W SALDEO ===")
    for c in companies_list:
        print(f"NAZWA: {c.get('NAME')}, COMPANY_PROGRAM_ID: {c.get('COMPANY_PROGRAM_ID')}")
    print("===========================\n")

    # Kontrahenci tylko dla wskazanych firm
    companies_csv = args.companies or os.getenv("SALDEO_COMPANY_IDS", "")
    companies = [c.strip() for c in companies_csv.split(",") if c.strip()]
    if not companies:
        raise SystemExit("Brak SALDEO_COMPANY_IDS w .env albo --companies")

    os.makedirs(args.output_dir, exist_ok=True)
    all_contractors = []

    for cid in companies:
        logging.info("Pobieram kontrahentów dla firmy %s", cid)
        contractors = fetch_contractors_for_company(base_url, cid, username, api_token, session)
        out_file = os.path.join(args.output_dir, f"contractors_{cid}.json")
        save_json(contractors, out_file)
        logging.info("Zapisano %s (%d rekordów)", out_file, len(contractors))
        for c in contractors:
            c["__company_program_id"] = cid
        all_contractors.extend(contractors)
        time.sleep(0.5)

    save_json(all_contractors, os.path.join(args.output_dir, "contractors_all.json"))
    logging.info("Gotowe. Łącznie %d rekordów", len(all_contractors))


if __name__ == "__main__":
    main()
