import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import db_conn
from decimal import Decimal


def test_types_from_db(limit: int = 5):
    sql = """
        SELECT
            nip,
            numer_faktury,
            data_wystawienia,
            kwota_netto,
            kwota_vat,
            kwota_brutto
        FROM faktury_do_prowizji
        LIMIT %s
    """

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (limit,))
        rows = cur.fetchall()

    print(f"\nPobrano {len(rows)} rekordów\n")

    for i, row in enumerate(rows, start=1):
        nip, nr, d, net, vat, brut = row

        print(f"--- REKORD {i} ---")
        print(f"nip              = {nip!r} | typ: {type(nip)}")
        print(f"numer_faktury    = {nr!r} | typ: {type(nr)}")
        print(f"data_wystawienia = {d!r} | typ: {type(d)}")
        print(f"kwota_netto      = {net!r} | typ: {type(net)}")
        print(f"kwota_vat        = {vat!r} | typ: {type(vat)}")
        print(f"kwota_brutto     = {brut!r} | typ: {type(brut)}")

        # twarda asercja — to MUSI być Decimal
        assert isinstance(net, Decimal), "❌ kwota_netto NIE jest Decimal"
        assert isinstance(vat, Decimal), "❌ kwota_vat NIE jest Decimal"
        assert isinstance(brut, Decimal), "❌ kwota_brutto NIE jest Decimal"

    print("\n✅ TEST OK — typy z DB są poprawne (Decimal)")
