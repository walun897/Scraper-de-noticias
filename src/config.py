import re

# ======================
# FACT-CHECK (etiquetadas) — >=15
# ======================
FACTCHECK_SOURCES = [
    {"name":"Colombiacheck","type":"html","url":"https://colombiacheck.com/chequeos","label_source":"colombiacheck"},
    {"name":"AFP Factual ES","type":"rss","url":"https://factuel.afp.com/espanol/rss.xml","label_source":"afp"},
    {"name":"Maldita.es","type":"html","url":"https://maldita.es/malditobulo/","label_source":"maldita"},
    {"name":"Newtral","type":"html","url":"https://www.newtral.es/bulologia/","label_source":"newtral"},
    {"name":"Chequeado","type":"html","url":"https://chequeado.com/verificaciones/","label_source":"chequeado"},
    {"name":"EFE Verifica","type":"html","url":"https://efe.com/verifica/","label_source":"efe"},
    {"name":"Verificat","type":"html","url":"https://www.verificat.cat/es/tags/desinformacion","label_source":"verificat"},
    {"name":"El Sabueso (Animal Político)","type":"html","url":"https://www.animalpolitico.com/etiqueta/el-sabueso/","label_source":"sabueso"},
    {"name":"VerificadoMX","type":"html","url":"https://verificado.com.mx/","label_source":"verificadomx"},
    {"name":"Bolivia Verifica","type":"html","url":"https://boliviaverifica.bo/category/chequea/","label_source":"bolivia"},
    {"name":"Ecuador Chequea","type":"html","url":"https://ecuadorchequea.com/category/engano/","label_source":"ecuador"},
    {"name":"Factchequeado","type":"html","url":"https://factchequeado.com/","label_source":"factchequeado"},
    {"name":"Ojo Público (OjoBiónico)","type":"html","url":"https://ojo-publico.com/buscar?keys=&f%5B0%5D=type%3Afactchecking","label_source":"ojobionico"},
    {"name":"Fast Check CL","type":"html","url":"https://www.fastcheck.cl/category/chequeos/","label_source":"fastcheck"},
    {"name":"EsPaja (VE)","type":"html","url":"https://espaja.com/checamos/","label_source":"espaja"},
    {"name":"Cotejo.info (VE)","type":"html","url":"https://cotejo.info/category/chequeo/","label_source":"cotejo"},
    {"name":"UYCheck (UY)","type":"html","url":"https://uycheck.com/","label_source":"uycheck"},
]

# ======================
# NEWS (confiables, sin etiqueta) — >=20
# ======================
NEWS_SOURCES = [
    {"name":"El Tiempo","type":"html","url":"https://www.eltiempo.com/ultima-hora"},
    {"name":"El Espectador","type":"html","url":"https://www.elespectador.com/ultima-hora/"},
    {"name":"La Silla Vacía","type":"html","url":"https://www.lasillavacia.com/hoy/"},
    {"name":"BBC Mundo","type":"rss","url":"https://feeds.bbci.co.uk/mundo/rss.xml"},
    {"name":"RTVE","type":"rss","url":"https://www.rtve.es/api/tematicos/noticias.rss"},
    {"name":"El País América","type":"rss","url":"https://feeds.elpais.com/mrss-s/pages/ep/site/elpais.com/section/america/portada"},
    {"name":"AP News ES","type":"rss","url":"https://apnews.com/hub/apf-latin-america?output=rss"},
    {"name":"DW Español","type":"rss","url":"https://www.dw.com/es/rss"},
    {"name":"France24 Español","type":"rss","url":"https://www.france24.com/es/rss"},
    {"name":"Reuters LatAm ES","type":"rss","url":"https://www.reuters.com/markets/latam/?output=rss"},
    {"name":"Infobae América","type":"rss","url":"https://www.infobae.com/america/rss.xml"},
    {"name":"Clarín Mundo","type":"rss","url":"https://www.clarin.com/rss/lo-ultimo/"},
    {"name":"El Universal MX","type":"html","url":"https://www.eluniversal.com.mx/ultima-hora/"},
    {"name":"El Financiero MX","type":"rss","url":"https://www.elfinanciero.com.mx/arc/outboundfeeds/rss/?outputType=xml"},
    {"name":"El Comercio PE","type":"html","url":"https://elcomercio.pe/ultimas-noticias/"},
    {"name":"La Nación AR","type":"rss","url":"https://www.lanacion.com.ar/rss/ultimas-noticias/"},
    {"name":"Semana (CO)","type":"rss","url":"https://www.semana.com/rss/"},
    {"name":"Caracol Radio","type":"rss","url":"https://caracol.com.co/rss/"},
    {"name":"RCN Radio","type":"rss","url":"https://www.rcnradio.com/rss"},
    {"name":"La República (CO)","type":"rss","url":"https://www.larepublica.co/rss"},
]

# ======================
# CAMPOS / LIMPIEZA / UMBRALES
# ======================
STD_FIELDS = [
    "source","title","summary","url","url_canonical","url_replicable",
    "published_at","label_raw","label","content_hash","content_len"
]

TARGET_RATIOS = {"true": 0.40, "false": 0.40, "doubtful": 0.20}

RE_MULTISPACE = re.compile(r"\s+")
MAX_TITLE_LEN = 400
MAX_SUMMARY_LEN = 1500
MIN_TITLE_CHARS = 8  # descarta títulos demasiado cortos
DEFAULT_LOOKBACK_DAYS = 10
FALLBACK_LOOKBACK_DAYS = 30
MAX_PAGES_HTML = 3
TIMEOUT_SECS = 30

# Para la tesis (texto completo). Si True, extrae contenido con trafilatura para el hash.
ENABLE_CONTENT_EXTRACTION = False

# Si el día trae pocos datos, ampliamos ventana:
MIN_ROWS_FACTCHECK = 20
MIN_ROWS_NEWS = 40
