import os, time, base64, logging, mimetypes
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/gmail.send"]

def gmail_service(creds_path: str, token_path: str):
    creds = None
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(creds_path, SCOPES)
            creds = flow.run_local_server(port=0)
        os.makedirs(os.path.dirname(token_path), exist_ok=True)
        with open(token_path, "w") as f:
            f.write(creds.to_json())
    return build("gmail", "v1", credentials=creds)

def send_email(service, sender: str, recipient: str, subject: str, html_body: str, attachments=None):
    msg = MIMEMultipart("mixed")
    msg["To"] = recipient
    msg["From"] = sender
    msg["Subject"] = subject
    alt = MIMEMultipart("alternative")
    alt.attach(MIMEText("Szanowni Państwo,\nProszę o zapoznanie się z załączoną fakturą.", "plain", "utf-8"))
    alt.attach(MIMEText(html_body, "html", "utf-8"))
    msg.attach(alt)
    for path in (attachments or []):
        if not os.path.exists(path): continue
        ctype, _ = mimetypes.guess_type(path)
        maintype, subtype = (ctype or "application/octet-stream").split("/", 1)
        with open(path, "rb") as f:
            part = MIMEBase(maintype, subtype)
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f'attachment; filename="{os.path.basename(path)}"')
        msg.attach(part)
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    for attempt in range(3):
        try:
            service.users().messages().send(userId="me", body={"raw": raw}).execute()
            logging.info(f"[MAIL] ✅ Wysłano do {recipient}")
            return True
        except Exception as e:
            logging.warning(f"[MAIL] Próba {attempt+1}/3 nieudana: {e}")
            time.sleep(2 ** attempt)
    return False
