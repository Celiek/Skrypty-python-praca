#!/usr/bin/env python3
import os
import time
import threading
import mimetypes
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple
from email import policy
from email.message import EmailMessage
from email.parser import BytesParser
from jinja2 import DictLoader

from flask import Flask, request, send_file, abort, render_template_string, redirect, url_for, Response
from aiosmtpd.controller import Controller

# --- USTAWIENIA ---
HOST_SMTP = os.getenv("SMTP_HOST", "127.0.0.1")
PORT_SMTP = int(os.getenv("SMTP_PORT", "1025"))

HOST_HTTP = os.getenv("HTTP_HOST", "127.0.0.1")
PORT_HTTP = int(os.getenv("HTTP_PORT", "8080"))

STORE_DIR = Path(os.getenv("STORE_DIR", "./_mails")).resolve()
STORE_DIR.mkdir(parents=True, exist_ok=True)
ATT_DIR = STORE_DIR / "attachments"
ATT_DIR.mkdir(parents=True, exist_ok=True)
MAX_IN_MEMORY = 500   # ile ostatnich maili trzymać w pamięci listy


# --- PAMIĘC NA WIADOMOŚCI (prosta, wątkowo-bezpieczna) ---
from threading import RLock
_lock = RLock()

class StoredMsg:
    def __init__(self, mid: str, raw: bytes, msg: EmailMessage,
                 peer: Tuple[str,int], mail_from: str, rcpt_tos: List[str],
                 eml_path: Path, att_dir: Path):
        self.id = mid
        self.raw = raw
        self.msg = msg
        self.peer = peer
        self.mail_from = mail_from
        self.rcpt_tos = rcpt_tos
        self.eml_path = eml_path
        self.att_dir = att_dir
        self.created = datetime.now()

    @property
    def subject(self) -> str:
        return self.msg.get("Subject", "(brak)")

    @property
    def from_(self) -> str:
        return self.msg.get("From", "(brak)")

    @property
    def to(self) -> str:
        return self.msg.get("To", ", ".join(self.rcpt_tos) if self.rcpt_tos else "(brak)")

    def get_text_part(self) -> Optional[str]:
        if self.msg.is_multipart():
            for part in self.msg.walk():
                if part.get_content_disposition() == "attachment":
                    continue
                if part.get_content_type() == "text/plain":
                    try:
                        return part.get_content()
                    except Exception:
                        return part.get_payload(decode=True).decode(errors="replace")
            return None
        else:
            if self.msg.get_content_type() == "text/plain":
                return self.msg.get_content()
        return None

    def get_html_part(self) -> Optional[bytes]:
        if self.msg.is_multipart():
            for part in self.msg.walk():
                if part.get_content_disposition() == "attachment":
                    continue
                if part.get_content_type() == "text/html":
                    payload = part.get_payload(decode=True)
                    return payload if isinstance(payload, (bytes, bytearray)) else (str(payload).encode("utf-8", "replace"))
            return None
        else:
            if self.msg.get_content_type() == "text/html":
                payload = self.msg.get_payload(decode=True)
                return payload if isinstance(payload, (bytes, bytearray)) else (str(payload).encode("utf-8", "replace"))
        return None

    def iter_attachments(self):
        idx = 0
        for part in self.msg.walk():
            if part.get_content_disposition() == "attachment":
                filename = part.get_filename() or f"attachment_{idx}"
                safe = "".join(c for c in filename if c not in r'\/:*?"<>|')
                data = part.get_payload(decode=True) or b""
                mimetype = part.get_content_type() or mimetypes.guess_type(safe)[0] or "application/octet-stream"
                yield idx, safe, data, mimetype
                idx += 1


MESSAGES: List[StoredMsg] = []


def store_message(raw: bytes, msg: EmailMessage, peer, mail_from, rcpt_tos) -> StoredMsg:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    mid = f"{ts}-{int(time.time()*1000)}"
    eml_path = STORE_DIR / f"{mid}.eml"
    att_dir = ATT_DIR / mid
    att_dir.mkdir(parents=True, exist_ok=True)

    eml_path.write_bytes(raw)

    stored = StoredMsg(mid, raw, msg, peer, mail_from, rcpt_tos, eml_path, att_dir)

    # zapisz fizycznie załączniki
    for idx, fname, data, _ in stored.iter_attachments():
        (att_dir / fname).write_bytes(data)

    with _lock:
        MESSAGES.insert(0, stored)  # najnowsze na górze
        if len(MESSAGES) > MAX_IN_MEMORY:
            MESSAGES.pop()

    return stored


# --- SMTP HANDLER ---
class DebugHandler:
    async def handle_DATA(self, server, session, envelope):
        msg = BytesParser(policy=policy.default).parsebytes(envelope.content)
        stored = store_message(
            raw=envelope.content,
            msg=msg,
            peer=session.peer,
            mail_from=envelope.mail_from,
            rcpt_tos=envelope.rcpt_tos,
        )
        print(f"[SMTP] Odebrano wiadomość {stored.id} FROM={stored.mail_from} TO={stored.rcpt_tos} SUBJECT={stored.subject}")
        return "250 OK"


# --- HTTP (Flask) ---
app = Flask(__name__)

HTML_BASE = """
<!doctype html>
<html lang="pl">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{ title }}</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@picocss/pico@2/css/pico.min.css">
<style>
iframe { width: 100%; height: 70vh; border: 1px solid #ddd; }
code { white-space: pre-wrap; }
.table-scroll { overflow:auto; max-height: 70vh;}
.small { font-size: .9rem; color: #666; }
.wrap { word-break: break-word; }
</style>
</head>
<body class="container">
<header>
  <hgroup>
    <h1>SMTP Viewer</h1>
    <p class="small">SMTP: smtp://{{smtp_host}}:{{smtp_port}} &nbsp;&middot;&nbsp; UI: http://{{http_host}}:{{http_port}}</p>
  </hgroup>
  <nav>
    <ul>
      <li><strong><a href="{{ url_for('index') }}">Wiadomości</a></strong></li>
      <li><a href="{{ url_for('about') }}">Info</a></li>
    </ul>
  </nav>
</header>

<main>
  {% block content %}{% endblock %}
</main>
<footer>
  <p class="small">Lokalny podgląd wiadomości. Do testów — bez TLS/AUTH.</p>
</footer>
</body>
</html>
"""

@app.route("/")
def index():
    with _lock:
        msgs = list(MESSAGES)

    tmpl = """
{% extends "base.html" %}
{% block content %}
<h2>Odebrane ({{ msgs|length }})</h2>
<div class="table-scroll">
<table>
  <thead>
    <tr><th>Czas</th><th>Od</th><th>Do</th><th>Temat</th><th>Akcje</th></tr>
  </thead>
  <tbody>
  {% for m in msgs %}
    <tr>
      <td class="small">{{ m.created.strftime("%Y-%m-%d %H:%M:%S") }}</td>
      <td class="wrap">{{ m.from_ }}</td>
      <td class="wrap">{{ m.to }}</td>
      <td class="wrap"><a href="{{ url_for('message_detail', mid=m.id) }}">{{ m.subject }}</a></td>
      <td>
        <a href="{{ url_for('download_eml', mid=m.id) }}">.eml</a>
      </td>
    </tr>
  {% endfor %}
  </tbody>
</table>
</div>
{% if msgs|length == 0 %}
<p>Brak wiadomości. Skonfiguruj aplikację, aby wysyłała na <code>{{smtp_host}}:{{smtp_port}}</code>.</p>
{% endif %}
{% endblock %}
"""
    return render_template_string(
        tmpl,
        msgs=msgs,
        smtp_host=HOST_SMTP,
        smtp_port=PORT_SMTP,
        http_host=HOST_HTTP,
        http_port=PORT_HTTP
    )

@app.route("/msg/<mid>")
def message_detail(mid: str):
    m = find_msg(mid)
    if not m:
        abort(404)

    # listuj załączniki (z dysku, bo już zapisane)
    attachments = sorted((m.att_dir.glob("*")))
    has_html = m.get_html_part() is not None
    has_text = m.get_text_part() is not None

    tmpl = """
{% extends "base.html" %}
{% block content %}
<h2>Szczegóły</h2>
<article>
  <header>
    <h3 class="wrap">{{ m.subject }}</h3>
    <p class="small">ID: {{ m.id }} &middot; {{ m.created.strftime("%Y-%m-%d %H:%M:%S") }}</p>
  </header>

  <ul>
    <li><strong>From:</strong> <span class="wrap">{{ m.from_ }}</span></li>
    <li><strong>To:</strong> <span class="wrap">{{ m.to }}</span></li>
    <li><strong>MAIL FROM:</strong> {{ m.mail_from }}</li>
    <li><strong>RCPT TO:</strong> {{ ', '.join(m.rcpt_tos) }}</li>
    <li><strong>Peer:</strong> {{ m.peer[0] }}:{{ m.peer[1] }}</li>
  </ul>

  <details open>
    <summary>Nagłówki</summary>
    <code>{{ m.msg.as_string()[:4000] }}{% if m.msg.as_string()|length > 4000 %}\n...\n{% endif %}</code>
  </details>

  <h4>Treść</h4>
  <p>
    {% if has_text %}<a href="{{ url_for('message_text', mid=m.id) }}" target="_blank">Otwórz text/plain</a>{% else %}<em>(brak text/plain)</em>{% endif %}
    &nbsp;|&nbsp;
    {% if has_html %}<a href="{{ url_for('message_html', mid=m.id) }}" target="_blank">Otwórz HTML</a>{% else %}<em>(brak text/html)</em>{% endif %}
    &nbsp;|&nbsp;
    <a href="{{ url_for('download_eml', mid=m.id) }}">Pobierz .eml</a>
  </p>

  {% if has_html %}
  <h5>Podgląd HTML (iframe)</h5>
  <iframe src="{{ url_for('message_html', mid=m.id) }}" sandbox="allow-same-origin"></iframe>
  {% elif has_text %}
  <h5>Podgląd text/plain</h5>
  <pre>{{ m.get_text_part() }}</pre>
  {% else %}
  <p><em>Brak treści do wyświetlenia.</em></p>
  {% endif %}

  <h4>Załączniki</h4>
  {% if attachments %}
    <ul>
      {% for a in attachments %}
        <li><a href="{{ url_for('download_attachment', mid=m.id, fname=a.name) }}">{{ a.name }}</a> ({{ a.stat().st_size }} B)</li>
      {% endfor %}
    </ul>
  {% else %}
    <p>(brak)</p>
  {% endif %}
</article>
<p><a href="{{ url_for('index') }}">⟵ powrót</a></p>
{% endblock %}
"""
    return render_template_string(
        tmpl,
        m=m,
        attachments=attachments,
        has_html=has_html,
        has_text=has_text,
        smtp_host=HOST_SMTP,
        smtp_port=PORT_SMTP,
        http_host=HOST_HTTP,
        http_port=PORT_HTTP
    )

@app.route("/msg/<mid>/html")
def message_html(mid: str):
    m = find_msg(mid)
    if not m:
        abort(404)
    html = m.get_html_part()
    if not html:
        return Response("<h3>Brak części HTML</h3>", mimetype="text/html; charset=utf-8")
    # Uwaga: to testowy viewer — renderujemy dostarczony HTML bez sanitizacji.
    return Response(html, mimetype="text/html; charset=utf-8")

@app.route("/msg/<mid>/text")
def message_text(mid: str):
    m = find_msg(mid)
    if not m:
        abort(404)
    text = m.get_text_part() or "(brak części text/plain)"
    return Response(text, mimetype="text/plain; charset=utf-8")

@app.route("/msg/<mid>/download.eml")
def download_eml(mid: str):
    m = find_msg(mid)
    if not m:
        abort(404)
    return send_file(m.eml_path, as_attachment=True, download_name=f"{mid}.eml")

@app.route("/msg/<mid>/att/<path:fname>")
def download_attachment(mid: str, fname: str):
    m = find_msg(mid)
    if not m:
        abort(404)
    path = (m.att_dir / fname).resolve()
    if not str(path).startswith(str(m.att_dir.resolve())):
        abort(403)
    if not path.exists():
        abort(404)
    return send_file(path, as_attachment=True, download_name=path.name)

@app.route("/about")
def about():
    tmpl = """
{% extends "base.html" %}
{% block content %}
<h2>Info</h2>
<p>To narzędzie uruchamia lokalny serwer SMTP (bez TLS/AUTH) oraz prosty interfejs WWW do podglądu wiadomości.</p>
<ul>
  <li>SMTP: <code>smtp://{{smtp_host}}:{{smtp_port}}</code></li>
  <li>UI: <code>http://{{http_host}}:{{http_port}}</code></li>
  <li>Katalog wiadomości: <code>{{ store }}</code></li>
</ul>
<p>Wyłącznie do testów lokalnych.</p>
{% endblock %}
"""
    return render_template_string(
        tmpl,
        store=str(STORE_DIR),
        smtp_host=HOST_SMTP,
        smtp_port=PORT_SMTP,
        http_host=HOST_HTTP,
        http_port=PORT_HTTP
    )

# Szablon bazowy w pamięci
app.jinja_loader = DictLoader({"base.html": HTML_BASE})


def find_msg(mid: str) -> Optional[StoredMsg]:
    with _lock:
        for m in MESSAGES:
            if m.id == mid:
                return m
    # jeśli nie ma w pamięci — spróbuj z dysku (minimalny tryb: tylko .eml)
    eml_path = STORE_DIR / f"{mid}.eml"
    if eml_path.exists():
        raw = eml_path.read_bytes()
        msg = BytesParser(policy=policy.default).parsebytes(raw)
        # nie mamy peer/mail_from/rcpt_tos — wypełnij symbolicznie
        return StoredMsg(mid, raw, msg, ("-", 0), "-", [], eml_path, ATT_DIR / mid)
    return None


# --- URUCHAMIANIE ---
def run_smtp():
    controller = Controller(DebugHandler(), hostname=HOST_SMTP, port=PORT_SMTP)
    controller.start()
    print(f"[SMTP] Nasłuchuję na smtp://{HOST_SMTP}:{PORT_SMTP}")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        pass
    finally:
        controller.stop()


def run_http():
    print(f"[HTTP] UI:    http://{HOST_HTTP}:{PORT_HTTP}")
    app.run(host=HOST_HTTP, port=PORT_HTTP, debug=False)


if __name__ == "__main__":
    # uruchom oba serwery równolegle (SMTP w wątku, HTTP w głównym)
    t = threading.Thread(target=run_smtp, daemon=True)
    t.start()
    run_http()
