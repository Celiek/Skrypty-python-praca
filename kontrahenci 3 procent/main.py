import logging
import os
import re
import smtplib
import ssl
from pathlib import Path

import unicodedata
from argparse import ArgumentParser
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from decimal import Decimal, ROUND_HALF_UP
from typing import List, Dict, Tuple, Optional

import pandas as pd
import psycopg2
import requests
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

# =========================
# Konfiguracja / stałe
# =========================
load_dotenv()

API_KEY = os.getenv("API_KEY")
OUTPUT_ENCODING = os.getenv("OUTPUT_ENCODING", "utf-8-sig")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", ".")

EMAIL_HTML_TEMPLATE ="""
        <!DOCTYPE html>
<html lang="en">

<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="x-apple-disable-message-reformatting">
    <link href="https://fonts.googleapis.com/css?family=Montserrat:400,700&display=swap" rel="stylesheet"
        type="text/css">
    <style>
        @media only screen and (min-width: 520px) {
            .u-row {
                width: 500px !important;
            }

            .u-row .u-col {
                vertical-align: top;
            }


            .u-row .u-col-22p53 {
                width: 112.65px !important;
            }


            .u-row .u-col-22p74 {
                width: 113.7px !important;
            }


            .u-row .u-col-23p13 {
                width: 115.65px !important;
            }


            .u-row .u-col-23p14 {
                width: 115.7px !important;
            }


            .u-row .u-col-50 {
                width: 250px !important;
            }


            .u-row .u-col-54p13 {
                width: 270.65px !important;
            }


            .u-row .u-col-54p33 {
                width: 271.65px !important;
            }


            .u-row .u-col-100 {
                width: 500px !important;
            }

        }



        .font_color {
            color: #0077DA;
        }
    </style>
</head>

<body class="clean-body u_body"
    style="margin: 0;padding: 0;-webkit-text-size-adjust: 100%;background-color: #ffffff;color: #000000">
    <table role="presentation" id="u_body"
        style="border-collapse: collapse;table-layout: fixed;border-spacing: 0;mso-table-lspace: 0pt;mso-table-rspace: 0pt;vertical-align: top;min-width: 320px;Margin: 0 auto;background-color: #ffffff;width:100%"
        cellpadding="0" cellspacing="0">
        <tbody>
            <tr style="vertical-align: top">
                <td style="word-break: break-word;border-collapse: collapse !important;vertical-align: top">
                    <div class="u-row-container" style="padding: 0px;background-color: transparent">
                        <div class="u-row"
                            style="margin: 0 auto;min-width: 320px;max-width: 500px;overflow-wrap: break-word;word-wrap: break-word;word-break: break-word;background-color: transparent;">
                            <div
                                style="border-collapse: collapse;display: table;width: 100%;height: 100%;background-color: transparent;">
                                <div class="u-col u-col-100"
                                    style="max-width: 320px;min-width: 500px;display: table-cell;vertical-align: top;">
                                    <div
                                        style="background-color: #f7fbfc;height: 100%;width: 100% !important;border-radius: 0px;-webkit-border-radius: 0px; -moz-border-radius: 0px;">
                                        <div
                                            style="box-sizing: border-box; height: 100%; padding: 0px;border-top: 0px solid transparent;border-left: 0px solid transparent;border-right: 0px solid transparent;border-bottom: 0px solid transparent;border-radius: 0px;-webkit-border-radius: 0px; -moz-border-radius: 0px;">
                                            <table style="font-family:arial,helvetica,sans-serif;" role="presentation"
                                                cellpadding="0" cellspacing="0" width="100%" border="0">
                                                <tbody>
                                                    <tr>
                                                        <td style="overflow-wrap:break-word;word-break:break-word;padding:40px 10px 10px;font-family:arial,helvetica,sans-serif;"
                                                            align="left">

                                                            <table role="presentation" width="100%" cellpadding="0"
                                                                cellspacing="0" border="0">
                                                                <tr>
                                                                    <td style="padding-right: 0px;padding-left: 0px;"
                                                                        align="center">

                                                                        <img align="center" border="0"
                                                                            src="zdj/logo.svg" alt="Super Merchant"
                                                                            title="Cart Icon"
                                                                            style="outline: none;text-decoration: none;-ms-interpolation-mode: bicubic;clear: both;display: inline-block !important;border: none;height: auto;float: none;width: 15%;max-width: 72px;" />

                                                                    </td>
                                                                </tr>
                                                            </table>

                                                        </td>
                                                    </tr>
                                                </tbody>
                                            </table>

                                            <table style="font-family:arial,helvetica,sans-serif;" role="presentation"
                                                cellpadding="0" cellspacing="0" width="100%" border="0">
                                                <tbody>
                                                    <tr>
                                                        <td style="overflow-wrap:break-word;word-break:break-word;padding:10px;font-family:arial,helvetica,sans-serif;"
                                                            align="left">

                                                            <div
                                                                style="font-size: 14px; line-height: 140%; text-align: center; word-wrap: break-word;">
                                                                <p
                                                                    style="font-size: 14px; line-height: 140%; margin: 0px;">
                                                                    <span
                                                                        style="font-size: 18px; line-height: 25.2px; font-family: Montserrat, sans-serif;"><strong><span
                                                                                style="line-height: 25.2px; font-size: 18px;">Miesiąc
                                                                                dobiegł końca !</span></strong></span>
                                                                </p>
                                                            </div>

                                                        </td>
                                                    </tr>
                                                </tbody>
                                            </table>

                                            <table style="font-family:arial,helvetica,sans-serif;" role="presentation"
                                                cellpadding="0" cellspacing="0" width="100%" border="0">
                                                <tbody>
                                                    <tr>
                                                        <td style="overflow-wrap:break-word;word-break:break-word;padding:0px 10px 10px;font-family:arial,helvetica,sans-serif;"
                                                            align="left">

                                                            <div
                                                                style="font-size: 14px; line-height: 140%; text-align: center; word-wrap: break-word;">
                                                                <p
                                                                    style="font-size: 14px; line-height: 140%; margin: 0px;">
                                                                    <span
                                                                        style="font-family: Montserrat, sans-serif; font-size: 14px; line-height: 19.6px;">
                                                                        Poprzedni okres rozliczeniowy dobiegł końca,
                                                                        poniżej znajdziesz link do pobrania faktury 3%
                                                                        za sprzedane artykuły, a w załączniku listę
                                                                        faktur, na podstawie których została ona
                                                                        wystawiona.</span>
                                                                </p>
                                                                <p
                                                                    style="font-size: 14px; line-height: 140%; margin: 0px;">
                                                                    <span
                                                                        style="font-family: Montserrat, sans-serif; font-size: 14px; line-height: 19.6px;">
                                                                </p>
                                                            </div>

                                                        </td>
                                                    </tr>
                                                </tbody>
                                            </table>

                                            <table style="font-family:arial,helvetica,sans-serif;" role="presentation"
                                                cellpadding="0" cellspacing="0" width="100%" border="0">
                                                <tbody>
                                                    <tr>
                                                        <td style="overflow-wrap:break-word;word-break:break-word;padding:10px 10px 20px;font-family:arial,helvetica,sans-serif;"
                                                            align="left">
                                                            <div align="center">
                                                                <a href="https://www.unlayer.com" target="_blank"
                                                                    class="v-button"
                                                                    style="box-sizing: border-box; display: inline-block; text-decoration: none; text-size-adjust: none; text-align: center; color: rgb(255, 255, 255); background: #0077DA;; border-radius: 4px; width: auto; max-width: 100%; word-break: break-word; overflow-wrap: break-word; font-size: 14px; line-height: inherit;"><span
                                                                        style="display:block;padding:10px 20px;line-height:120%;"><span
                                                                            style="font-family: Montserrat, sans-serif; font-size: 14px; line-height: 16.8px;"><strong>Pobierz
                                                                                fakturę </strong></span></span>
                                                                </a>
                                                            </div>

                                                        </td>
                                                    </tr>
                                                </tbody>
                                            </table>

                                            <table style="font-family:arial,helvetica,sans-serif;" role="presentation"
                                                cellpadding="0" cellspacing="0" width="100%" border="0">
                                                <tbody>
                                                    <tr>
                                                        <td style="overflow-wrap:break-word;word-break:break-word;padding:10px;font-family:arial,helvetica,sans-serif;"
                                                            align="left">

                                                            <table role="presentation" aria-label="divider" height="0px"
                                                                align="center" border="0" cellpadding="0"
                                                                cellspacing="0" width="100%"
                                                                style="border-collapse: collapse;table-layout: fixed;border-spacing: 0;mso-table-lspace: 0pt;mso-table-rspace: 0pt;vertical-align: top;border-top: 2px solid #e7e7e7;-ms-text-size-adjust: 100%;-webkit-text-size-adjust: 100%">
                                                                <tbody>
                                                                    <tr style="vertical-align: top">
                                                                        <td
                                                                            style="word-break: break-word;border-collapse: collapse !important;vertical-align: top;font-size: 0px;line-height: 0px;mso-line-height-rule: exactly;-ms-text-size-adjust: 100%;-webkit-text-size-adjust: 100%">
                                                                            <span>&#160;</span>
                                                                        </td>
                                                                    </tr>
                                                                </tbody>
                                                            </table>

                                                        </td>
                                                    </tr>
                                                </tbody>
                                            </table>

                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </td>
            </tr>
        </tbody>

        <div class="u-col u-col-100" style="max-width: 320px;min-width: 500px;display: table-cell;vertical-align: top;">
            <div
                style="background-color: #f7fbfc;height: 100%;width: 100% !important;border-radius: 0px;-webkit-border-radius: 0px; -moz-border-radius: 0px;">
                <div
                    style="box-sizing: border-box; height: 100%; padding: 0px;border-top: 0px solid transparent;border-left: 0px solid transparent;border-right: 0px solid transparent;border-bottom: 0px solid transparent;border-radius: 0px;-webkit-border-radius: 0px; -moz-border-radius: 0px;">

                    <table style="font-family:arial,helvetica,sans-serif;" role="presentation" cellpadding="0"
                        cellspacing="0" width="100%" border="0">
                        <tbody>
                            <tr>
                                <td style="overflow-wrap:break-word;word-break:break-word;padding:20px 10px 10px;font-family:arial,helvetica,sans-serif;"
                                    align="left">

                                    <div
                                        style="font-size: 14px; line-height: 140%; text-align: center; word-wrap: break-word;">
                                        <p style="font-size: 14px; line-height: 140%; margin: 0px;"><span
                                                style="font-family: Montserrat, sans-serif; font-size: 14px; line-height: 19.6px;">Jeśli
                                                masz jakieś pytania skontaktuj sie z nami:</span></p>
                                        <p style="font-size: 14px; line-height: 140%; margin: 0px;"><span
                                                style="font-family: Montserrat, sans-serif; font-size: 14px; line-height: 19.6px;">
                                                <a href="mailto:kontakt@shumee.pl"
                                                    style="a:link{color : #0077DA}; ">contact@supermerchant.base.com</a></span></p>
                                    </div>

                                </td>
                            </tr>
                        </tbody>
                    </table>

                    <table style="font-family:arial,helvetica,sans-serif;" role="presentation" cellpadding="0"
                        cellspacing="0" width="100%" border="0">
                        <tbody>
                            <tr>
                                <td style="overflow-wrap:break-word;word-break:break-word;padding:10px 10px 40px;font-family:arial,helvetica,sans-serif;"
                                    align="left">

                                    <div align="center" style="direction: ltr;" aria-label="social">
                                        <div style="display: table; max-width:147px;">
                                            <table role="presentation" aria-label="Facebook icon" border="0"
                                                cellspacing="0" cellpadding="0" width="32" height="32"
                                                style="width: 32px !important;height: 32px !important;display: inline-block;border-collapse: collapse;table-layout: fixed;border-spacing: 0;mso-table-lspace: 0pt;mso-table-rspace: 0pt;vertical-align: top;margin-right: 5px">
                                                <tbody>
                                                    <tr style="vertical-align: top">
                                                        <td valign="middle"
                                                            style="word-break: break-word;border-collapse: collapse !important;vertical-align: top">
                                                            <a href="https://facebook.com/" title="Facebook"
                                                                target="_blank"
                                                                style="color: rgb(0, 0, 238); text-decoration: underline; line-height: inherit;"><img
                                                                    src="images/image-4.png" alt="Facebook icon"
                                                                    title="Facebook" width="32"
                                                                    style="outline: none;text-decoration: none;-ms-interpolation-mode: bicubic;clear: both;display: block !important;border: none;height: auto;float: none;max-width: 32px !important">
                                                            </a>
                                                        </td>
                                                    </tr>
                                                </tbody>
                                            </table>
                                            <table role="presentation" aria-label="Twitter icon" border="0"
                                                cellspacing="0" cellpadding="0" width="32" height="32"
                                                style="width: 32px !important;height: 32px !important;display: inline-block;border-collapse: collapse;table-layout: fixed;border-spacing: 0;mso-table-lspace: 0pt;mso-table-rspace: 0pt;vertical-align: top;margin-right: 5px">
                                                <tbody>
                                                    <tr style="vertical-align: top">
                                                        <td valign="middle"
                                                            style="word-break: break-word;border-collapse: collapse !important;vertical-align: top">
                                                            <a href="https://twitter.com/" title="Twitter"
                                                                target="_blank"
                                                                style="color: rgb(0, 0, 238); text-decoration: underline; line-height: inherit;"><img
                                                                    src="images/image-5.png" alt="Twitter icon"
                                                                    title="Twitter" width="32"
                                                                    style="outline: none;text-decoration: none;-ms-interpolation-mode: bicubic;clear: both;display: block !important;border: none;height: auto;float: none;max-width: 32px !important">
                                                            </a>
                                                        </td>
                                                    </tr>
                                                </tbody>
                                            </table>

                                            <table role="presentation" aria-label="LinkedIn icon" border="0"
                                                cellspacing="0" cellpadding="0" width="32" height="32"
                                                style="width: 32px !important;height: 32px !important;display: inline-block;border-collapse: collapse;table-layout: fixed;border-spacing: 0;mso-table-lspace: 0pt;mso-table-rspace: 0pt;vertical-align: top;margin-right: 5px">
                                                <tbody>
                                                    <tr style="vertical-align: top">
                                                        <td valign="middle"
                                                            style="word-break: break-word;border-collapse: collapse !important;vertical-align: top">
                                                            <a href="https://linkedin.com/" title="LinkedIn"
                                                                target="_blank"
                                                                style="color: rgb(0, 0, 238); text-decoration: underline; line-height: inherit;"><img
                                                                    src="images/image-6.png" alt="LinkedIn icon"
                                                                    title="LinkedIn" width="32"
                                                                    style="outline: none;text-decoration: none;-ms-interpolation-mode: bicubic;clear: both;display: block !important;border: none;height: auto;float: none;max-width: 32px !important">
                                                            </a>
                                                        </td>
                                                    </tr>
                                                </tbody>
                                            </table>
                                            <table role="presentation" aria-label="Instagram icon" border="0"
                                                cellspacing="0" cellpadding="0" width="32" height="32"
                                                style="width: 32px !important;height: 32px !important;display: inline-block;border-collapse: collapse;table-layout: fixed;border-spacing: 0;mso-table-lspace: 0pt;mso-table-rspace: 0pt;vertical-align: top;margin-right: 0px">
                                                <tbody>
                                                    <tr style="vertical-align: top">
                                                        <td valign="middle"
                                                            style="word-break: break-word;border-collapse: collapse !important;vertical-align: top">
                                                            <a href="https://instagram.com/" title="Instagram"
                                                                target="_blank"
                                                                style="color: rgb(0, 0, 238); text-decoration: underline; line-height: inherit;"><img
                                                                    src="images/image-7.png" alt="Instagram icon"
                                                                    title="Instagram" width="32"
                                                                    style="outline: none;text-decoration: none;-ms-interpolation-mode: bicubic;clear: both;display: block !important;border: none;height: auto;float: none;max-width: 32px !important">
                                                            </a>
                                                        </td>
                                                    </tr>
                                                </tbody>
                                            </table>
                                        </div>
                                    </div>

                                </td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
</body>
</table>
</html>
    """

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

COMPANIES = {
    "shumee": {
        "name_addr": os.getenv("SHUMEE_NAME_ADDR", "Shumee Sp. z.o.o. ..."),
        "name" : os.getenv("NAZWA","SHUMME"),
        "nrb": os.getenv("SHUMEE_NRB", "07114011080000314718001007"),
        "bank_code": os.getenv("SHUMEE_BANK_CODE", "11401108"),
        "email": os.getenv("SHUMEE_EMAIL", "faktury@shumee.pl"),
        "server_host":os.getenv("SHUMEE_SERVER_HOST", "imap.serwer1694120.home.pl"),
        "kontakt":os.getenv("SHUMEE_KONTAKT", "kontakt@shumee.pl"),
        "password":os.getenv("SHUMEE_PASS"),
    },
    "greatstore": {
        "name_addr": os.getenv("GREATSTORE_NAME_ADDR", "Greatstore Sp. z.o.o. ..."),
        "name" : os.getenv("NAZWA","GREATSTORE"),
        "nrb": os.getenv("GREATSTORE_NRB", "18102055610000310200035501"),
        "bank_code": os.getenv("GREATSTORE_BANK_CODE", "10205561"),
        "email": os.getenv("SHUMEE_EMAIL", "faktury@greatstore.pl"),
        "server_host":os.getenv("SHUMEE_SERVER_HOST", "imap.serwer1694120.home.pl"),
        "kontakt": os.getenv("SHUMEE_KONTAKT", "kontakt@greatstore.pl"),
        "password":os.getenv("GREATSTORE_PASS"),
    },
    "extrastore": {
        "name_addr": os.getenv("EXTRASTORE_NAME_ADDR", "Extrastore Sp. z.o.o. ..."),
        "name" : os.getenv("NAZWA","EXTRASTORE"),
        "nrb": os.getenv("EXTRASTORE_NRB", "05114020040000330280429939"),
        "bank_code": os.getenv("EXTRASTORE_BANK_CODE", "11402004"),
        "email": os.getenv("SHUMEE_EMAIL", "faktury_extra@shumee.pl"),
        "server_host":os.getenv("SHUMEE_SERVER_HOST", "imap.serwer1694120.home.pl"),
        "kontakt": os.getenv("SHUMEE_KONTAKT", "kontakt@greatstore.pl"),
        "password":os.getenv("EXTRASTORE_PASS"),
    },
}

DEPARTMENT_ID = {
    "shumee": 1705441,
    "extrastore": 1705460,
    "greatstore": 1705454,
}

os.makedirs(OUTPUT_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# =========================
# Utils
# =========================
def db_conn():
    return psycopg2.connect(**DB_CONFIG)

def nip_digits(nip: str) -> str:
    cleaned = re.sub(r"\D", "", str(nip or ""))
    if len(cleaned) != 10:
        logging.warning("NIP ma nieprawidłową długość: %s → %s", nip, cleaned)
    return cleaned

def prepare_recipients(rows_from_build: List[Dict], wyniki_faktur: List[Dict]) -> pd.DataFrame:
    """
    rows_from_build: elementy z build_invoice_rows (mają 'buyer_tax_no' oraz 'buyer_email')
    wyniki_faktur: output z dodaj_faktury (ma 'nip','ok','link')

    Zwraca DF z kolumnami: nip, email, kontrahent, invoice_link
    """
    # z 'rows' bierzemy maila i nazwę
    df_rows = pd.DataFrame(rows_from_build)
    df_rows.rename(columns={
        "buyer_tax_no": "nip",
        "buyer_email": "email",
        "buyer_name": "kontrahent"
    }, inplace=True)
    df_rows["nip"] = df_rows["nip"].astype(str).str.strip()

    # z wyników faktur bierzemy linki
    df_res = pd.DataFrame(wyniki_faktur)
    if df_res.empty:
        df_res = pd.DataFrame(columns=["nip","link","ok"])
    df_res["nip"] = df_res["nip"].astype(str).str.strip()

    out = df_rows.merge(df_res[["nip","link","ok"]], on="nip", how="left")
    # można odsiać tylko udane wystawienia
    out = out.loc[out["ok"] == True].copy()
    return out

def _slugify_filename(s: str, max_len: int = 40) -> str:
    if not s:
        return ""
    s = str(s).strip()
    s = unicodedata.normalize("NFKD",s)
    s = "".join (ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[A^Zz-z0-9_.-]+","_",s)
    return s[:max_len].strip("_") or "kontrahent"

def export_grouped_csvs(df: pd.DataFrame,out_dir: str, *, encoding: str ="utf-8-sig") -> Dict[str,str]:
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    wanted = ["Kontrahent", "NIP", "Numer dokumentu", "Data", "Netto", "VAT", "Brutto"]
    cols = [c for c in wanted if c in df.columns]
    if not cols:
        raise ValueError("Brak kolumn do eksportu CSV — sprawdź nazwy w DataFrame.")

    out_map: Dict[str, str] = {}
    g = df.groupby("NIP", dropna=False, as_index=False)
    for nip, sub in g:
        nip_str = str(nip).strip()
        kontrahent = ""
        if "Kontrahent" in sub.columns and not sub["Kontrahent"].isna().all():
            kontrahent = str(sub["Kontrahent"].iloc[0] or "")

        fname = f"{nip_str}_{_slugify_filename(kontrahent)}.csv"
        fpath = os.path.join(out_dir, fname)
        sub[cols].to_csv(fpath, index=False, encoding=encoding, sep=";")
        out_map[nip_str] = os.path.abspath(fpath)

    return out_map

def prepare_recipients(rows_from_build: List[Dict], wyniki_faktur: List[Dict], attachments_by_nip: Dict[str, str]) -> pd.DataFrame:
    df_rows = pd.DataFrame(rows_from_build)
    df_rows.rename(columns={
        "buyer_tax_no": "nip",
        "buyer_email": "email",
        "buyer_name": "kontrahent"
    }, inplace=True)
    if not df_rows.empty:
        df_rows["nip"] = df_rows["nip"].astype(str).str.strip()

    df_res = pd.DataFrame(wyniki_faktur) if wyniki_faktur else pd.DataFrame([])
    if df_res.empty:
        df_res = pd.DataFrame(columns=["nip","link","ok"])
    else:
        df_res["nip"] = df_res["nip"].astype(str).str.strip()

    out = df_rows.merge(df_res[["nip","link","ok"]], on="nip", how="left")
    out = out.loc[out["ok"] == True].copy()

    # dołącz ścieżkę do CSV
    out["attachment_path"] = out["nip"].map(attachments_by_nip).fillna("")
    return out

def render_email_html(invoice_link: str, company_name: str) -> str:
    return EMAIL_HTML_TEMPLATE.format(INVOICE_LINK=invoice_link, COMPANY_NAME=company_name)

def _money2(x) -> float:
    """Zaokrąglenie kwoty do 2 miejsc (HALF_UP), zwraca float do JSON."""
    return float(Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

def _norm_doc_no(x: str) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip()
    s = re.sub(r"\s+", " ", s)
    return s.upper()

def handle_duplicates(df: pd.DataFrame, action: str = "warn") -> pd.DataFrame:
    before = len(df)
    df2 = df.drop_duplicates(subset=["NIP", "Numer dokumentu"], keep="first")
    after = len(df2)
    if action == "warn" and before != after:
        logging.info("[DUP] Usunięto %d duplikatów. Zostało %d rekordów.", before - after, after)
    return df2

def fetch_statusy_kontrahentow(nipy: List[str]) -> Dict[str, str]:
    """SELECT nip, status FROM merchanci WHERE nip IN (...)"""
    nums = [re.sub(r"\D", "", str(n)) for n in nipy if n]
    nums = [n for n in nums if n]
    if not nums:
        return {}
    placeholders = ",".join(["%s"] * len(nums))
    query = f"SELECT nip, status FROM merchanci WHERE nip IN ({placeholders})"
    result = {}
    with db_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, tuple(nums))
        for row in cur.fetchall():
            result[str(row["nip"])] = row["status"]
    return result

def fetch_emails(nipy) -> pd.DataFrame:
    """Zwraca DF kolumny: nip, email (dla listy NIP-ów)."""
    if isinstance(nipy, pd.Series):
        nipy = nipy.dropna().astype(str).str.strip().unique().tolist()
    elif isinstance(nipy, pd.DataFrame):
        nipy = nipy["NIP"].dropna().astype(str).str.strip().unique().tolist()
    elif isinstance(nipy, (list, tuple)):
        nipy = [str(n).strip() for n in nipy if n]
    else:
        nipy = [str(nipy).strip()]

    if not nipy:
        return pd.DataFrame(columns=["nip", "email"])

    query = "SELECT nip, email FROM merchanci WHERE nip = ANY(%s::bigint[])"
    with db_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, (nipy,))
        rows = cur.fetchall()
    return pd.DataFrame(rows)

# =========================
# Fakturownia helpers
# =========================
def lista_faktur_sm3() -> List[dict]:
    """Zwraca faktury z bieżącego miesiąca, których number kończy się na '/sm3'."""
    url = "https://shumee.fakturownia.pl/invoices.json"
    params = {
        "period": "this_month",
        "api_token": API_KEY,
        "page": 1,
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    return [f for f in data if str(f.get("number", "")).endswith("/sm3")]

def build_invoice_rows(df: pd.DataFrame) -> List[Dict]:
    """
    Przygotowuje rekordy do wystawienia faktur:
    - agreguje po NIP (Netto sum, Kontrahent first),
    - liczona stawka 3% netto i brutto (VAT 23%),
    - dociąga email z DB.
    Zwraca listę dict: {buyer_name, buyer_tax_no, buyer_email, amount_gross}
    """
    grouped = (
        df.groupby("NIP", as_index=False)
          .agg({"Netto": "sum", "Kontrahent": "first"})
    )
    grouped["stawka_netto_3p"]  = grouped["Netto"] * 0.03
    grouped["stawka_brutto_3p"] = grouped["stawka_netto_3p"] * 1.23

    emails_df = fetch_emails(grouped["NIP"])
    emails_df["nip"] = emails_df["nip"].astype(str)
    grouped["NIP"]   = grouped["NIP"].astype(str)
    merged = grouped.merge(emails_df, left_on="NIP", right_on="nip", how="left")

    rows = []
    for _, r in merged.iterrows():
        rows.append({
            "buyer_name":   str(r["Kontrahent"]).strip(),
            "buyer_tax_no": str(r["NIP"]).strip(),
            "buyer_email":  (str(r["email"]).strip() if pd.notna(r.get("email")) else None),
            "amount_gross": _money2(r["stawka_brutto_3p"]),
        })
    return rows

def get_invoice_public_url(invoice_id: int, api_key: str) -> Optional[str]:
    """
    Zwraca publiczny link (np. 'view_url' lub 'public_url') do faktury w Fakturowni.
    """
    url = f"https://shumee.fakturownia.pl/invoices/{invoice_id}.json"
    try:
        r = requests.get(url, params={"api_token": api_key}, timeout=30)
        r.raise_for_status()
        data = r.json()
        # Fakturownia zazwyczaj zwraca 'view_url' (link do podglądu) oraz np. 'print_url'
        for key in ("view_url", "public_url", "print_url", "download_url"):
            if data.get(key):
                return data[key]
    except Exception as e:
        logging.error("Nie udało się pobrać linku do faktury id=%s: %s", invoice_id, e)
    return None


def send_Email(spolka: str,recipents_df: pd.DataFrame,*,subject: Optional[str] = None) -> list[dict]:

    cfg = get_spolka_config(spolka)
    from_addr = cfg["email"]
    password = cfg["password"]
    host = cfg["server_host"]
    port = int(os.getenv("SMTP_PORT","465"))
    use_ssl = os.getenv("SMTP_USE_SSL", "1") == "1"
    company_name = cfg.get("name") or spolka.upper()
    if not subject:
        subject = f"{company_name} - faktura"

    results = []

    if recipents_df is None or recipents_df.empty:
        logging.info("[MAIL] Brak odbiorców do wysyłki.")
        return results

    context = ssl.create_default_context()
    server = None

    try:
        if use_ssl:
            server = smtplib.SMTP_SSL(host=host, port=port, context=context, timeout = 60)
        else:
            server = smtplib.SMTP(host=host, port=port, timeout=60)
            server.ehlo()
            server.starttls(context=context)
            server.ehlo()

        if from_addr and password:
            server.login(from_addr, password)

        for row in recipents_df.itertuples(index = False):
            email_to = (getattr(row, "email", None) or "").strip()
            invoice_link = (getattr(row, "link", None) or getattr(row, "invoice_link", None) or "").strip()
            kontrahent = (getattr(row, "kontrahent", "") or "").strip()

            if not email_to:
                results.append({"email": None, "ok": False, "error": "Brak adresu email"})
                continue
            if not invoice_link:
                results.append({"email": email_to, "ok": False, "error": "Brak linku do faktury"})
                continue

            html_body = render_email_html(invoice_link, company_name)
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = from_addr
            msg["To"] = email_to

            msg.attach(MIMEText(html_body, "html", "utf-8"))

            try:
                server.sendmail(from_addr, [email_to], msg.as_string())
                logging.info("[MAIL] OK -> %s (kontrahent: %s)", email_to, kontrahent or "-")
                results.append({"email": email_to, "ok": True})
            except Exception as e:
                logging.error("[MAIL] BŁĄD -> %s: %s", email_to, e)
                results.append({"email": email_to, "ok": False, "error": str(e)})
    finally:
        if server:
            try:
                server.quit()
            except Exception:
                pass
    return results


def dodaj_faktury(spolka: str, items: List[Dict]) -> List[Dict]:
    """
    items: [{"buyer_name","buyer_tax_no","buyer_email","amount_gross"}]
    Wystawia faktury w Fakturowni. Zwraca listę wyników: {"nip","ok", "id"|"error"}.
    """
    if spolka not in DEPARTMENT_ID:
        raise ValueError(f"Nieznana spółka: {spolka}")
    dept_id = DEPARTMENT_ID[spolka]

    today = datetime.today()
    payment_to = today + timedelta(days=14)
    miesiace = {
        1:"styczeń",2:"luty",3:"marzec",4:"kwiecień",5:"maj",6:"czerwiec",
        7:"lipiec",8:"sierpień",9:"wrzesień",10:"październik",11:"listopad",12:"grudzień"
    }
    poprzedni = today - relativedelta(months=1)

    url = "https://shumee.fakturownia.pl/invoices.json"
    headers = {"Accept": "application/json", "Content-Type": "application/json"}

    results = []
    with requests.Session() as s:
        s.headers.update(headers)
        for it in items:
            payload = {
                "api_token": API_KEY,
                "invoice": {
                    "kind": "vat",
                    "number": None,
                    "sell_date":  today.strftime("%Y-%m-%d"),
                    "issue_date": today.strftime("%Y-%m-%d"),
                    "payment_to": payment_to.strftime("%Y-%m-%d"),
                    "buyer_name":   it["buyer_name"],
                    "buyer_tax_no": it["buyer_tax_no"],
                    "department_id": dept_id,
                    **({"buyer_email": it["buyer_email"]} if it.get("buyer_email") else {}),
                    "positions": [
                        {
                            "name": f"płatność za usługę za okres {miesiace[poprzedni.month]}",
                            "tax": 23,
                            "total_price_gross": it["amount_gross"],
                            "quantity": 1
                        }
                    ]
                }
            }
            try:
                r = s.post(url, json=payload, timeout=30)
                if 200 <= r.status_code < 300:
                    data = r.json()
                    inv_id = data.get("id")
                    link = get_invoice_public_url(inv_id, API_KEY) if inv_id else None
                    results.append({
                        "nip": it["buyer_tax_no"],
                        "ok": True,
                        "id": inv_id,
                        "link": link
                    })
                    results.append({
                        "nip": it["buyer_tax_no"],
                        "ok": False,
                        "error": f"{r.status_code} {r.text[:500]}"
                    })
                else:
                    results.append({
                        "nip": it["buyer_tax_no"],
                        "ok": False,
                        "error": f"{r.status_code} {r.text[:500]}"
                    })
            except Exception as e:
                results.append({"nip": it["buyer_tax_no"], "ok": False, "error": str(e)})
    return results

def get_spolka_config(spolka: str) -> dict:
    klucz = spolka.strip().lower()
    try:
        return COMPANIES[klucz]
    except KeyError:
        raise ValueError(
            f"Nieznana firma: {klucz}. Dozwolone: {', '.join(COMPANIES)}"
        )

# =========================
# Główna logika
# =========================
def czytaj_plik(
    file: str,
    *,
    spolka: str,
    key: str,
    output_file: Optional[str] = None,
) -> Optional[pd.DataFrame]:

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")



    # 1) wczytanie
    df = pd.read_excel(file)
    if df is None or df.empty:
        raise ValueError("Pusty DataFrame – sprawdź plik wejściowy.")

    # 2) czyszczenia
    df = df.replace("", pd.NA)
    df = handle_duplicates(df, action="warn")

    # puste NIPy -> raport i usunięcie
    mask_empty_nip = df["NIP"].isna() | (df["NIP"].astype(str).str.strip() == "")
    if mask_empty_nip.any():
        out = os.path.join(OUTPUT_DIR, f"brak_nipu_{ts}.csv")
        df.loc[mask_empty_nip].to_csv(out, index=False, encoding=OUTPUT_ENCODING)
        logging.warning("[WARN] Pominięto %d wierszy z pustym NIP-em → %s",
                        int(mask_empty_nip.sum()), out)
    df = df.loc[~mask_empty_nip].copy()

    # normalizacja NIP
    df["NIP"] = df["NIP"].apply(nip_digits)

    # ujemne kwoty → raport i usunięcie
    mask_negative = (df["Netto"] < 0) | (df["VAT"] < 0) | (df["Brutto"] < 0)
    if mask_negative.any():
        out = os.path.join(OUTPUT_DIR, f"ujemne_{ts}.csv")
        df.loc[mask_negative].to_csv(out, index=False, encoding=OUTPUT_ENCODING)
        logging.warning("[WARN] Pominięto %d wierszy z ujemnymi kwotami → %s",
                        int(mask_negative.sum()), out)
    df = df.loc[~mask_negative].copy()

    # PREMERCHANT → raport i usunięcie
    status_map = fetch_statusy_kontrahentow(df["NIP"].unique())
    mask_prem = df["NIP"].astype(str).apply(
        lambda nip: (status_map.get(re.sub(r"\D", "", str(nip)), "") or "").lower() == "premerchant"
    )
    if mask_prem.any():
        out = os.path.join(OUTPUT_DIR, f"premerchant_{ts}.csv")
        df.loc[mask_prem].to_csv(out, index=False, encoding=OUTPUT_ENCODING)
        logging.warning("[WARN] Pominięto %d wierszy PREMERCHANT → %s",
                        int(mask_prem.sum()), out)
    df = df.loc[~mask_prem].copy()

    if df.empty:
        logging.info("[INFO] Po filtracjach brak wierszy do fakturowania.")
        return df

    # 3) przygotuj rekordy do fakturowania
    rows = build_invoice_rows(df)
    logging.info("[INFO] Do wystawienia faktur: %d rekordów.", len(rows))

    # 4) wystaw faktury
    wyniki = dodaj_faktury(spolka, rows)
    ok_cnt  = sum(1 for w in wyniki if w["ok"])
    bad_cnt = len(wyniki) - ok_cnt
    logging.info("[FAKTURY] OK: %d, BŁĘDY: %d", ok_cnt, bad_cnt)
    for w in wyniki:
        if not w["ok"]:
            logging.error("   NIP=%s → %s", w["nip"], w["error"])

    # 5) wysyłanie faktur emailem

    recipients_df = prepare_recipients(rows, wyniki)  # NIP->email + link
    mail_results = send_Email(spolka, recipients_df)
    mail_ok = sum(1 for r in mail_results if r["ok"])
    mail_bad = len(mail_results) - mail_ok
    logging.info("[MAIL] OK: %d, BŁĘDY: %d", mail_ok, mail_bad)

    # (opcjonalnie) zapis wyników
    if output_file:
        pd.DataFrame(wyniki).to_csv(output_file, index=False, encoding=OUTPUT_ENCODING)
        logging.info("[SAVE] Zapisano raport: %s", output_file)

    return df

# =========================
# CLI
# =========================
if __name__ == "__main__":
    if not API_KEY:
        raise RuntimeError("Brak API_KEY w .env lub zmiennych środowiskowych.")

    parser = ArgumentParser(description="Generowanie faktur 3% za poprzedni miesiąc")
    parser.add_argument("input", help="Ścieżka do pliku XLSX z danymi")
    parser.add_argument("-c", "--company", required=True, choices=sorted(COMPANIES.keys()),
                        help=f"Firma (nadawca): {', '.join(sorted(COMPANIES.keys()))}")
    parser.add_argument("-o", "--output", help="Ścieżka do raportu wynikowego CSV", default=None)
    args = parser.parse_args()

    czytaj_plik(
        file=args.input,
        spolka=args.company,
        key=args.company,
        output_file=args.output
    )
