import re

# FUENTES – empezamos con pocas que entregan RSS confiable.
# Puedes añadir más luego (misma forma).
FACTCHECK_SOURCES = [
    {"name":"AFP Factual ES", "type":"rss", "url":"https://factuel.afp.com/espanol/rss.xml", "label_source":"afp"},
]

NEWS_SOURCES = [
    {"name":"BBC Mundo",        "type":"rss", "url":"https://feeds.bbci.co.uk/mundo/rss.xml"},
    {"name":"RTVE",             "type":"rss", "url":"https://www.rtve.es/api/tematicos/noticias.rss"},
    {"name":"El País América",  "type":"rss", "url":"https://feeds.elpais.com/mrss-s/pages/ep/site/elpais.com/section/america/portada"},
]

# Campos estándar del dataset unificado
STD_FIELDS = [
    "source","source_type","title","summary","url","published_at",
    "label_raw","label","label_origin","url_canonical","content_hash"
]

# Limpieza
RE_MULTISPACE     = re.compile(r"\s+")
MAX_TITLE_LEN     = 400
MAX_SUMMARY_LEN   = 1500
MIN_TITLE_CHARS   = 8

# Ventanas
DEFAULT_LOOKBACK_DAYS  = 30
FALLBACK_LOOKBACK_DAYS = 60

# Red de seguridad de filas mínimas para no quedar en blanco
MIN_ROWS_FACTCHECK = 5
MIN_ROWS_NEWS      = 10

# HTTP
TIMEOUT_SECS = 30
