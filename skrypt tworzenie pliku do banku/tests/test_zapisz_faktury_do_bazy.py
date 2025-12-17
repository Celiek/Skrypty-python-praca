import os
import sys

import pandas as pd
import pytest

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from contextlib import contextmanager

import elixr_bo_banku as elixir



class FakeCursor:
    def __init__(self, existing=None, kontrahent_exists=True, raise_on_insert=False):
        # existing: zbiór (id_kontrahenta, numer_faktury)
        self.existing = existing or set()
        self.kontrahent_exists = kontrahent_exists
        self.raise_on_insert = raise_on_insert
        self.last_query = None
        self.insert_calls = []
        self._mode = None  # 'faktury' lub 'merchanci'

    def execute(self, query, params=None):
        self.last_query = query
        q = " ".join(query.lower().split())

        if "from faktury" in q:
            self._mode = "faktury"
        elif "from merchanci" in q:
            self._mode = "merchanci"
            self._merch_nip = params[0] if params else None
        elif "insert into faktury" in q:
            self._mode = "insert"
            if self.raise_on_insert:
                # symulujemy błąd bazy
                raise elixir.psycopg2.Error("fake insert error")
            self.insert_calls.append((query, params))

    def fetchall(self):
        if self._mode == "faktury":
            # zwracamy listę słowników jak RealDictCursor
            return [
                {"id_kontrahenta": id_k, "numer_faktury": nr}
                for (id_k, nr) in self.existing
            ]
        return []

    def fetchone(self):
        if self._mode == "merchanci":
            if self.kontrahent_exists:
                return {"id": 123}  # przykładowe id
            return None
        return None

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        pass


class FakeConn:
    def __init__(self, cursor: FakeCursor):
        self._cursor = cursor
        self.commits = 0
        self.rollbacks = 0

    def cursor(self, cursor_factory=None):
        return self._cursor

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        pass


# ========================
#  POMOCNICZE: fake db_conn
# ========================
def make_fake_db_conn(cursor: FakeCursor):
    @contextmanager
    def _fake_db_conn():
        conn = FakeConn(cursor)
        try:
            yield conn
        finally:
            conn.close()
    return _fake_db_conn


# ========================
#  POMOCNICZY DataFrame
# ========================
def make_sample_df():
    data = {
        "Numer dokumentu": ["FV/1"],
        "Data wystawienia": ["01.11.2025"],
        "Netto": [100.00],
        "VAT": [23.00],
        "Brutto": [123.00],
        "NIP": ["123-456-32-18"],
        "__netto_gr": [10000],
        "__vat_gr": [2300],
        "__brutto_gr": [12300],
    }
    return pd.DataFrame(data)


# ========================
# TEST 1 – pusty DF -> nic się nie wywala
# ========================
def test_zapis_faktury_empty_df_does_not_crash(monkeypatch, caplog):
    df = pd.DataFrame()

    # monkeypatch db_conn, ale nie powinien się nawet wywołać
    monkeypatch.setattr(elixir, "db_conn", make_fake_db_conn(FakeCursor()))

    elixir.zapisz_faktury_do_bazy(df, "TEST_S")
    # brak asercji – test przejdzie, jeśli nie będzie wyjątku


# ========================
# TEST 2 – brak kolumn -> ValueError
# ========================
def test_zapis_faktury_missing_columns_raises_valueerror(monkeypatch):
    df = pd.DataFrame({"Numer dokumentu": ["FV/1"]})
    monkeypatch.setattr(elixir, "db_conn", make_fake_db_conn(FakeCursor()))

    with pytest.raises(ValueError):
        elixir.zapisz_faktury_do_bazy(df, "TEST_S")


# ========================
# TEST 3 – poprawny zapis, brak duplikatów
# ========================
def test_zapis_faktury_insert_ok(monkeypatch):
    df = make_sample_df()

    fake_cursor = FakeCursor(existing=set(), kontrahent_exists=True, raise_on_insert=False)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        pass

    monkeypatch.setattr(elixir, "db_conn", make_fake_db_conn(fake_cursor))

    # jeśli coś się wywali – test nie przejdzie
    elixir.zapisz_faktury_do_bazy(df, "TEST_S")

    # powinniśmy mieć 1 próbę INSERT
    assert len(fake_cursor.insert_calls) == 1


# ========================
# TEST 4 – duplikat w bazie -> pominięty, bez wyjątku
# ========================
def test_zapis_faktury_duplicate_skipped(monkeypatch):
    df = make_sample_df()
    numer = df["Numer dokumentu"].iloc[0].strip()

    existing = {(123, numer)}  # w bazie już jest FV/1 dla kontrahenta 123
    fake_cursor = FakeCursor(existing=existing, kontrahent_exists=True, raise_on_insert=False)
    monkeypatch.setattr(elixir, "db_conn", make_fake_db_conn(fake_cursor))

    # kod NIE powinien rzucać wyjątku
    elixir.zapisz_faktury_do_bazy(df, "TEST_S")

    # nie powinno być żadnych nowych INSERT-ów
    assert len(fake_cursor.insert_calls) == 0


# ========================
# TEST 5 – błąd DB przy INSERT -> złapany, brak crasha
# ========================
def test_zapis_faktury_db_error_on_insert_does_not_crash(monkeypatch):
    df = make_sample_df()

    fake_cursor = FakeCursor(existing=set(), kontrahent_exists=True, raise_on_insert=True)
    fake_db_conn = make_fake_db_conn(fake_cursor)
    monkeypatch.setattr(elixir, "db_conn", fake_db_conn)

    # jeśli funkcja sama łapie psycopg2.Error, to tu nie powinno polecieć nic na zewnątrz
    elixir.zapisz_faktury_do_bazy(df, "TEST_S")

    # był co najmniej jeden INSERT, ale z wyjątkiem
    assert len(fake_cursor.insert_calls) == 0  # bo przerwane przez raise