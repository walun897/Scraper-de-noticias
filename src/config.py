import re
FACTCHECK_SOURCES = [
    {"name": "Colombiacheck", "type": "html", "url": "https://colombiacheck.com/chequeos", "label_source": "colombiacheck"},
    {"name": "AFP Factual ES","type": "rss",  "url": "https://factuel.afp.com/espanol/rss.xml", "label_source": "afp"},
    {"name": "Maldita.es",    "type": "html", "url": "https://maldita.es/malditobulo/", "label_source": "maldita"},
    {"name": "Newtral",       "type": "html", "url": "https://www.newtral.es/bulologia/", "label_source": "newtral"},
    {"name": "Chequeado",     "type": "html", "url": "https://chequeado.com/verificaciones/", "label_source": "chequeado"},
    {"name": "EFE Verifica",  "type": "html", "url": "https://efe.com/verifica/", "label_source": "efe"},
    {"name": "Verificat",     "type": "html", "url": "https://www.verificat.cat/es/tags/desinformacion", "label_source": "verificat"},
    {"name": "El Sabueso (Animal Político)", "type": "html", "url": "https://www.animalpolitico.com/etiqueta/el-sabueso/", "label_source": "sabueso"},
    {"name": "VerificadoMX",  "type": "html", "url": "https://verificado.com.mx/", "label_source": "verificadomx"},
    {"name": "Bolivia Verifica","type":"html","url":"https://boliviaverifica.bo/category/chequea/","label_source":"bolivia"},
    {"name": "Ecuador Chequea","type":"html","url":"https://ecuadorchequea.com/category/engano/","label_source":"ecuador"},
    {"name": "Factchequeado","type":"html","url":"https://factchequeado.com/","label_source":"factchequeado"},
]
NEWS_SOURCES = [
    {"name":"El Tiempo","type":"html","url":"https://www.eltiempo.com/ultima-hora"},
    {"name":"El Espectador","type":"html","url":"https://www.elespectador.com/ultima-hora/"},
    {"name":"La Silla Vacía","type":"html","url":"https://www.lasillavacia.com/hoy/"},
    {"name":"BBC Mundo","type":"rss","url":"https://feeds.bbci.co.uk/mundo/rss.xml"},
    {"name":"RTVE","type":"rss","url":"https://www.rtve.es/api/tematicos/noticias.rss"},
    {"name":"El País América","type":"rss","url":"https://feeds.elpais.com/mrss-s/pages/ep/site/elpais.com/section/america/portada"},
    {"name":"El Universal MX","type":"html","url":"https://www.eluniversal.com.mx/ultima-hora/"},
    {"name":"El Comercio PE","type":"html","url":"https://elcomercio.pe/ultimas-noticias/"},
    {"name":"La Nación AR","type":"rss","url":"https://www.lanacion.com.ar/rss/ultimas-noticias/"},
]
STD_FIELDS = ["source","title","summary","url","published_at","label_raw","label"]
TARGET_RATIOS = {"true":0.40,"false":0.40,"doubtful":0.20}
RE_MULTISPACE = re.compile(r"\s+")
MAX_TITLE_LEN = 400
MAX_SUMMARY_LEN = 1500
DEFAULT_LOOKBACK_DAYS = 10
TIMEOUT_SECS = 30


