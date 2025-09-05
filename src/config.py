import re

# ======================
# FACT-CHECK (RSS preferido)
# ======================
# Nota: si alguna fuente no trae RSS fiable, puedes añadirla en HTML en pipeline.HTML_SELECTORS
FACTCHECK_SOURCES = [
    {"name":"AFP Factual ES","type":"rss","url":"https://factuel.afp.com/espanol/rss.xml","label_source":"afp"},
    {"name":"Chequeado","type":"rss","url":"https://chequeado.com/feed/","label_source":"chequeado"},
    {"name":"Newtral","type":"rss","url":"https://www.newtral.es/bulologia/feed/","label_source":"newtral"},
    {"name":"Maldita.es","type":"rss","url":"https://maldita.es/feed/","label_source":"maldita"},
    {"name":"EFE Verifica","type":"rss","url":"https://efe.com/verifica/feed/","label_source":"efe"},
    {"name":"Verificat","type":"rss","url":"https://www.verificat.cat/es/rss.xml","label_source":"verificat"},
    {"name":"Colombiacheck","type":"rss","url":"https://colombiacheck.com/rss.xml","label_source":"colombiacheck"},
    {"name":"Factchequeado","type":"rss","url":"https://factchequeado.com/feed/","label_source":"factchequeado"},
    {"name":"El Sabueso (Animal Político)","type":"rss","url":"https://www.animalpolitico.com/feed/","label_source":"sabueso"},
    {"name":"VerificadoMX","type":"rss","url":"https://verificado.com.mx/feed/","label_source":"verificadomx"},
    {"name":"Bolivia Verifica","type":"rss","url":"https://boliviaverifica.bo/feed/","label_source":"bolivia"},
    {"name":"Ecuador Chequea","type":"rss","url":"https://ecuadorchequea.com/feed/","label_source":"ecuador"},
]

# ======================
# NEWS (confiables → etiqueta true)
# ======================
NEWS_SOURCES = [
    {"name":"BBC Mundo","type":"rss","url":"https://feeds.bbci.co.uk/mundo/rss.xml"},
    {"name":"RTVE","type":"rss","url":"https://www.rtve.es/api/tematicos/noticias.rss"},
    {"name":"El País América","type":"rss","url":"https://feeds.elpais.com/mrss-s/pages/ep/site/elpais.com/section/america/portada"},
    {"name":"AP News Español","type":"rss","url":"https://apnews.com/hub/apf-latin-america?output=rss"},
    {"name":"DW Español","type":"rss","url":"https://www.dw.com/es/rss"},
    {"name":"France24 Español","type":"rss","url":"https://www.france24.com/es/rss"},
    {"name":"Reuters LatAm ES","type":"rss","url":"https://www.reuters.com/markets/latam/rss"},
    {"name":"Infobae América","type":"rss","url":"https://www.infobae.com/america/rss.xml"},
    {"name":"Clarín Mundo","type":"rss","url":"https://www.clarin.com/rss/lo-ultimo/"},
    {"name":"La Nación AR","type":"rss","url":"https://www.lanacion.com.ar/rss/ultimas-noticias/"},
    {"name":"El Tiempo (CO)","type":"rss","url":"https://www.eltiempo.com/rss/colombia.xml"},
    {"name":"El Espectador (CO)","type":"rss","url":"https://www.elespectador.com/feed/"},
    {"name":"La Silla Vacía (CO)","type":"rss","url":"https://www.lasillavacia.com/feed/"},
    {"name":"El Universal (MX)","type":"rss","url":"https://www.eluniversal.com.mx/rss/ultimas-noticias.xml"},
    {"name":"El Comercio (PE)","type":"rss","url":"https://elcomercio.pe/feed/"},
    {"name":"Semana (CO)","type":"rss","url":"https://www.semana.com/rss/"},
    {"name":"Caracol Radio","type":"rss","url":"https://caracol.com.co/rss/"},
    {"name":"RCN Radio","type":"rss","url":"https://www.rcnradio.com/rss"},
    {"name":"La República (CO)","type":"rss","url":"https://www.larepublica.co/rss"},
    {"name":"El Financiero (MX)","type":"rss","url":"https://www.elfinanciero.com.mx/arc/outboundfeeds/rss/?outputType=xml"},
]

# ======================
# CAMPOS / LIMPIEZA
# ======================
STD_FIELDS = [
    "source","source_type","title","summary","url","url_canonical","url_replicable",
    "published_at","label_raw","label","label_origin","content_hash"
]

TARGET_RATIOS = {"true": 0.40, "false": 0.40, "doubtful": 0.20}  # (por si luego balanceas)
RE_MULTISPACE = re.compile(r"\s+")
MAX_TITLE_LEN = 400
MAX_SUMMARY_LEN = 1500
MIN_TITLE_CHARS = 8

DEFAULT_LOOKBACK_DAYS = 15
FALLBACK_LOOKBACK_DAYS = 45
TIMEOUT_SECS = 30
