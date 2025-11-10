import pandas as pd

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from main import parse_date_series


def test_parse_date_series_polish_format():
    data = pd.Series(["01.10.2025", "02.10.2025", "16.09.2025"])

    parsed = parse_date_series(data)

    assert str(parsed.iloc[0]) == "2025-10-01 00:00:00"
    assert str(parsed.iloc[1]) == "2025-10-02 00:00:00"
    assert str(parsed.iloc[2]) == "2025-09-16 00:00:00"
