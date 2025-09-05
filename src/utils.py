from __future__ import annotations
import unicodedata, logging, pytz
from charset_normalizer import from_bytes
from datetime import datetime

TZ = pytz.timezone("America/Bogota")
logger = logging.getLogger("scraper")
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(handler); logger.setLevel(logging.INFO)

def detect_decode(b: bytes) -> str:
    if not b: return ""
    res = from_bytes(b).best()
    if res is None: return ""
    t = unicodedata.normalize("NFC", str(res))
    return "".join(ch for ch in t if ch.isprintable() or ch in "\n\t\r")

def to_iso(dt: datetime|None) -> str:
    if dt is None: return ""
    if dt.tzinfo is None: dt = TZ.localize(dt)
    return dt.astimezone(pytz.UTC).isoformat()

def safe_trim(s: str, n: int) -> str:
    if not s: return ""
    s = s.strip()
    return (s if len(s)<=n else s[:n-1].rstrip()+"…")
