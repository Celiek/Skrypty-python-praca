import pandas as pd

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from main import build_items_from_merchants_and_invoices


def test_build_items_filtering():
    # Faktury wejściowe
    df_faktury = pd.DataFrame({
        "NIP": ["5741949102", "5741949102", "5741949102"],
        "Data wystawienia": ["16.09.2025", "02.10.2025", "06.10.2025"],
        "Netto": [100, 200, 300],
        "Kontrahent": ["TEST", "TEST", "TEST"]
    })

    # Lista kontrahentów + data startu
    df_merch = pd.DataFrame({
        "NIP": ["5741949102"],
        "Od kiedy prowizja 3%": ["14.09.2025"],
        "email": ["test@test.com"]
    })

    adresy = {"5741949102": "Testowa 123"}

    items = build_items_from_merchants_and_invoices(df_faktury, df_merch, adresy)

    # Powinny wejść **tylko faktury październikowe**, bo start = 01.10
    assert len(items) == 1
    assert items[0]["amount_net"] == "15.00"    # (200+300) * 0.03
    assert items[0]["amount_gross"] == "18.45"  # * 1.23
