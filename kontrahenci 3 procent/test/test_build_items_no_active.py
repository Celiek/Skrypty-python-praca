import pandas as pd

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from main import build_items_from_merchants_and_invoices

def test_no_active_merchants():
    df_faktury = pd.DataFrame({
        "NIP": ["123"],
        "Data wystawienia": ["01.10.2025"],
        "Netto": [100],
        "Kontrahent": ["X"]
    })

    df_merch = pd.DataFrame({
        "NIP": ["123"],
        "Od kiedy prowizja 3%": ["14.09.2050"],  # przyszłość
        "email": ["x@x.pl"]
    })

    items = build_items_from_merchants_and_invoices(df_faktury, df_merch, {})

    assert items == []
