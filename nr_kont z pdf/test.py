import hashlib, re
from urllib.parse import quote_plus

def url_encode_per_spec(s: str) -> str:
    enc = quote_plus(s, safe="*")
    enc = enc.replace("~","%7E")
    enc = re.sub(r"%[0-9a-f]{2}", lambda m: m.group(0).upper(), enc)
    return enc

def build_req_sig(params: dict, api_token: str) -> str:
    base = "".join(f"{k}={params[k]}" for k in sorted(params))
    enc = url_encode_per_spec(base)
    return hashlib.md5((enc + api_token).encode("utf-8")).hexdigest()

# przykład dla company/list
params = {
    "req_id": "2025-09-05T12:34:56Z-cmp",
    "username": "gabrielsm"
}
print(build_req_sig(params, "TWÓJ_API_TOKEN"))
