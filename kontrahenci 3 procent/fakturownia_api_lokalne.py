import os

from flask import Flask

app = Flask(__name__)

# lokalne api fakturownaia do testowania
#

# — prosta baza "w pamięci" —
INVOICES = []

@app.route("/api/addinvoice", methods=["POST"])
def add_invoice_fakturownia_mock():
    """
    Symuluje zachowanie Fakturowni dla endpointa POST /invoices.json
    Odbiera JSON:
      {
        "api_token": "TEST_API_KEY",
        "invoice": { ... }
      }
    Zwraca JSON z utworzoną fakturą.
    """
    data = request.get_json(silent=True) or {}
    api_token = data.get("api_token")
    invoice = data.get("invoice", {})

    if api_token != "TEST_API_KEY":
        return jsonify({"error": "Niepoprawny token API"}), 401

    # Walidacja minimalnych pól
    required = ["sell_date","issue_date","issue_date","payment_to","buyer_name", "buyer_tax_no", "sell_date", "issue_date", "amount_gross" if "amount_gross" in invoice else "positions"]
    missing = [r for r in required if r not in invoice]
    if missing:
        return jsonify({"error": f"Brak wymaganych pól: {', '.join(missing)}"}), 400

    # Generowanie ID i numeru
    invoice_id = len(INVOICES) + 1
    invoice_number = f"FV/{datetime.now().year}/{invoice_id:04d}"
    invoice["id"] = invoice_id
    invoice["number"] = invoice_number

    INVOICES.append(invoice)

    base_url = f"http://127.0.0.1:5000/public/invoices/{invoice_id}"

    # response = {
    #     "id": invoice_id,
    #     "number": invoice_number,
    #     "buyer_name": invoice.get("buyer_name"),
    #     "buyer_tax_no": invoice.get("buyer_tax_no"),
    #     "issue_date": invoice.get("issue_date"),
    #     "sell_date": invoice.get("sell_date"),
    #     "amount_gross": invoice.get("amount_gross") or invoice["positions"][0]["total_price_gross"],
    #     "view_url": f"{base_url}/view",
    #     "public_url": base_url,
    #     "print_url": f"{base_url}/print",
    #     "download_url": f"{base_url}.pdf"
    # }

    return 200

@app.route("/api/invoices", methods=["GET"])
def list_invoices():
    """Zwróć wszystkie faktury."""
    return jsonify(INVOICES), 200


@app.route("/api/invoices/<int:invoice_id>", methods=["GET"])
def get_invoice(invoice_id):
    """Zwróć jedną fakturę po id."""
    invoice = next((i for i in INVOICES if int(i.get("id", -1)) == invoice_id), None)
    if not invoice:
        return jsonify({"error": f"Nie znaleziono faktury {invoice_id}"}), 404
    return jsonify(invoice), 200

@app.route("/api/invoices/link", methods=["GET"])
def get_invoice_link():
    """
    Zwraca przykładowe publiczne linki do faktury (dla testów funkcji get_invoice_public_url()).
    Parametry:
      - invoice_id (opcjonalny)
      - api_token (opcjonalny, tylko do symulacji)
    """
    invoice_id = request.args.get("invoice_id", "TEST123")
    api_token = request.args.get("api_token", None)

    # prosty test tokena (symulacja autoryzacji)
    if api_token != "TEST_API_KEY":
        return jsonify({"error": "Niepoprawny token API"}), 401

    base_url = f"http://127.0.0.1:5000/public/invoices/{invoice_id}"

    return jsonify({
        "view_url": f"{base_url}/view",
        "public_url": f"{base_url}",
        "print_url": f"{base_url}/print",
        "download_url": f"{base_url}.pdf"
    }), 200

@app.route("/api/invoices/<int:invoice_id>", methods=["DELETE"])
def delete_invoice(invoice_id):
    """Usuń fakturę po id."""
    global INVOICES
    before = len(INVOICES)
    INVOICES = [i for i in INVOICES if int(i.get("id", -1)) != invoice_id]
    if len(INVOICES) == before:
        return jsonify({"error": f"Nie znaleziono faktury {invoice_id}"}), 404
    return jsonify({"message": f"Faktura {invoice_id} usunięta"}), 200


@app.route("/api/invoices/add", methods=["GET"])
def add_invoice_via_link():
    """Dodaj fakturę przez parametry w URL (np. do testów)."""
    data = request.args.to_dict()
    if not data:
        return jsonify({"error": "Brak parametrów w URL"}), 400
    INVOICES.append(data)
    return jsonify({"message": "Faktura dodana przez link", "data": data}), 201


@app.route("/api/reset", methods=["POST"])
def reset():
    """Czyści wszystkie dane (do testów)."""
    INVOICES.clear()
    return jsonify({"message": "Wyczyszczono dane"}), 200

@app.route("/api/invoices.json", methods=["GET"])
def fakturownia_mock_list():
    """
    Symuluje endpoint Fakturowni: GET /invoices.json
    Zwraca listę faktur w bieżącym miesiącu (mock danych z INVOICES).
    """
    api_token = request.args.get("api_token")
    period = request.args.get("period")
    page = request.args.get("page", 1)

    # prosty test autoryzacji
    if api_token != "TEST_API_KEY":
        return jsonify({"error": "Niepoprawny token API"}), 401

    # symulacja filtra po okresie
    if period == "this_month":
        # np. filtr po dacie wystawienia (tu uproszczony)
        data = INVOICES
    else:
        data = INVOICES

    # symulacja zwracanych danych (jak w Fakturowni)
    invoices_list = [
        {
            "id": inv.get("id"),
            "number": inv.get("invoice_number"),
            "buyer_name": inv.get("buyer_name"),
            "buyer_tax_no": inv.get("buyer_tax_no"),
            "total_price_gross": inv.get("amount_gross"),
            "issue_date": inv.get("issue_date"),
            "sell_date": inv.get("sell_date"),
        }
        for inv in data
    ]

    return jsonify(invoices_list), 200

from flask import Flask, request, jsonify, send_file
from datetime import datetime
import io
from reportlab.pdfgen import canvas  # do mockowych PDF-ów

app = Flask(__name__)
INVOICES = []  # Twoja pamięciowa baza faktur


@app.route("/api/invoices.json", methods=["GET"])
def fakturownia_list():
    """
    Symuluje GET /invoices.json z Fakturowni.
    Obsługuje paginację, filtr period=this_month, i token API.
    """
    api_token = request.args.get("api_token")
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 100))
    period = request.args.get("period", "this_month")

    # prosta autoryzacja
    if api_token != "TEST_API_KEY":
        return jsonify({"error": "Niepoprawny token API"}), 401

    # filtr po bieżącym miesiącu (jeśli daty są w invoice["issue_date"])
    now = datetime.now()
    filtered = []
    for inv in INVOICES:
        try:
            d = datetime.strptime(inv.get("issue_date", ""), "%Y-%m-%d")
            if period == "this_month" and (d.year == now.year and d.month == now.month):
                filtered.append(inv)
        except Exception:
            continue

    # paginacja
    start = (page - 1) * per_page
    end = start + per_page
    paged = filtered[start:end]

    return jsonify(paged), 200

@app.route("/api/invoices/<int:invoice_id>.pdf", methods=["GET"])
def fakturownia_invoice_pdf(invoice_id):
    """
    Symuluje GET /invoices/<id>.pdf (Fakturownia).
    Generuje prosty plik PDF w locie.
    """
    api_token = request.args.get("api_token")
    if api_token != "TEST_API_KEY":
        return jsonify({"error": "Niepoprawny token API"}), 401

    invoice = next((i for i in INVOICES if int(i.get("id", 0)) == invoice_id), None)
    if not invoice:
        return jsonify({"error": f"Nie znaleziono faktury {invoice_id}"}), 404

    buffer = io.BytesIO()
    p = canvas.Canvas(buffer)
    p.setFont("Helvetica-Bold", 16)
    p.drawString(100, 800, f"FAKTURA NR {invoice.get('number', '---')}")
    p.setFont("Helvetica", 12)
    p.drawString(100, 770, f"Kontrahent: {invoice.get('buyer_name')}")
    p.drawString(100, 750, f"NIP: {invoice.get('buyer_tax_no')}")
    p.drawString(100, 730, f"Kwota brutto: {invoice.get('amount_gross')}")
    p.drawString(100, 710, f"Data wystawienia: {invoice.get('issue_date')}")
    p.showPage()
    p.save()

    buffer.seek(0)
    return send_file(
        buffer,
        mimetype="application/pdf",
        as_attachment=True,
        download_name=f"faktura_{invoice_id}.pdf",
    )




if __name__ == "__main__":
    app.run(debug=True, port=int(os.getenv("PORT", 5000)))