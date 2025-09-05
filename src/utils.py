from __future__ import annotations
import unicodedata, logging, hashlib
from charset_normalizer import from_bytes
from datetime import datetime
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode, urljoin
import pytz
from .config import RE_MULTISPACE

# Zona horaria
TZ = pytz.timezone("America/Bogota")

# Logging
logger = logging.getLogger("scraper")
_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
if not logger.handlers:
    logger.addHandler(_handler)
logger.setLevel(logging.INFO)

def detect_decode(content_bytes: bytes) -> str:
    """Detecta encoding y devuelve str normalizado (UTF-8) evitando mojibake."""
    if not content_bytes:
        return ""
    result = from_bytes(content_bytes).best()
    if result is None:
        return ""
    text = str(result)
    text = unicodedata.normalize("NFC", text)
    text = "".join(ch for ch in text if ch.isprintable() or ch in "\n\t\r")
    return text

def to_iso(dt: datetime | None) -> str:
    if dt is None:
        return ""
    if dt.tzinfo is None:
        dt = TZ.localize(dt)
    return dt.astimezone(pytz.UTC).isoformat()

def safe_trim(text: str, max_len: int) -> str:
    if not text:
        return ""
    text = RE_MULTISPACE.sub(" ", text.strip())
    if len(text) > max_len:
        text = text[: max_len - 1].rstrip() + "…"
    return text

def is_lowinfo_title(title: str) -> bool:
    if not title:
        return True
    t = title.strip().lower()
    if len(t) < 8:
        return True
    bad = {"última hora","en vivo","noticias","ver más","leer más","portada"}
    return t in bad

def canonicalize_url(base_url: str, href: str) -> str:
    """Resuelve URL relativa, ordena query params, quita fragmento."""
    try:
        abs_url = urljoin(base_url or "", href or "")
        p = urlparse(abs_url)
        q = urlencode(sorted(parse_qsl(p.query, keep_blank_values=True)))
        p2 = p._replace(query=q, fragment="")
        return urlunparse(p2)
    except Exception:
        return href or base_url

def content_fingerprint(*parts: str) -> str:
    h = hashlib.sha256()
    for part in parts:
        h.update((part or "").encode("utf-8"))
        h.update(b"\x1f")
    return h.hexdigest()

def replicable_url(canonical_url: str, hash_hex: str) -> str:
    """Genera una variante con parámetros controlados (reproducible)."""
    try:
        p = urlparse(canonical_url)
        qs = dict(parse_qsl(p.query, keep_blank_values=True))
        qs.update({"src":"tesis","v":"1","h":hash_hex[:16]})
        q = urlencode(sorted(qs.items()))
        return urlunparse(p._replace(query=q))
    except Exception:
        return canonical_url
