import logging
from decimal import Decimal
from utils import db_conn

def zapisz_faktury_do_bazy(df, company):
    if df.empty:
        return
    zapisane, duplikaty = 0, 0
    with db_conn() as conn:
        cur = conn.cursor()
        for _, r in df.iterrows():
            try:
                cur.execute("""
                    INSERT INTO faktury (numer_faktury, data_wystawienia, kwota_netto, kwota_vat, kwota_brutto, typ_faktury, nazwa_spolki)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (numer_faktury) DO NOTHING
                """, (r["Numer dokumentu"], r["Data wystawienia"], Decimal(str(r["Netto"]).replace(",", ".")),
                      Decimal(str(r["VAT"]).replace(",", ".")), Decimal(str(r["Brutto"]).replace(",", ".")),
                      "POJEDYNCZA", company))
                zapisane += 1
            except Exception:
                duplikaty += 1
        conn.commit()
    logging.info(f"[DB] Zapisano {zapisane}, duplikatów {duplikaty}")

def zapisz_powiazania_do_bazy(df, wyniki, company):
    with db_conn() as conn:
        cur = conn.cursor()
        for w in wyniki:
            if not w.get("ok"): continue
            inv_id = w["id"]
            nip = w["nip"]
            sub = df[df["NIP"] == nip]
            for _, row in sub.iterrows():
                cur.execute("""
                    INSERT INTO faktura_powiazania (id_faktury_zbiorczej, id_faktury_skladnikowej)
                    VALUES (%s,%s)
                    ON CONFLICT DO NOTHING
                """, (inv_id, row["Numer dokumentu"]))
        conn.commit()
