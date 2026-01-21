import pandas as pd
from main import build_items_from_merchants_and_invoices

def test_gregoo_should_be_active(monkeypatch):
    monkeypatch.setattr(
        "main.get_names_from_db_for_nips",
        lambda _: {"6572694018": "GREGOO"}
    )
    items = build_items_from_merchants_and_invoices(
        df_faktury, df_merch, adresy_z_bazy={}
    )
    assert len(items) == 1
    assert items[0]["buyer_tax_no"] == "6572694018"
