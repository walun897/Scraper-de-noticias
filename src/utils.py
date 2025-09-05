from __future__ import annotations
import unicodedata, logging, hashlib
from charset_normalizer import from_bytes
from datetime import datetime
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode, urljoin
import pytz
from .config import RE_MULTISPACE

TZ = pytz.timezone("America/Bogota")

logger = logging.getLogger("scraper")
_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
if not logger.handlers:
    logger.addHandler(_handler)
logger.setLevel(logging.INFO)

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
    s = RE_MULTISPACE.sub(" ", s.strip())
    return (s if len(s)<=n else s[:n-1].rstrip()+"…")

def is_lowinfo_title(t: str) -> bool:
    if not t: return True
    tt = t.strip().lower()
    if len(tt) < 8: return True
    bad = {"última hora","en vivo","noticias","ver más","leer más","portada"}
    return tt in bad

def canonicalize_url(base_url: str, href: str="") -> str:
    try:
        abs_url = urljoin(base_url or "", href or "")
        p = urlparse(abs_url)
        q = urlencode(sorted(parse_qsl(p.query, keep_blank_values=True)))
        return urlunparse(p._replace(query=q, fragment=""))
    except Exception:
        return href or base_url

def content_fingerprint(*parts: str) -> str:
    h = hashlib.sha256()
    for part in parts:
        h.update((part or "").encode("utf-8"))
        h.update(b"\x1f")
    return h.hexdigest()

def replicable_url(canonical_url: str, hash_hex: str) -> str:
    try:
        p = urlparse(canonical_url)
        qs = dict(parse_qsl(p.query, keep_blank_values=True))
        qs.update({"src":"tesis","v":"1","h":hash_hex[:16]})
        q = urlencode(sorted(qs.items()))
        return urlunparse(p._replace(query=q))
    except Exception:
        return canonical_url
