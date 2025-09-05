#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, asyncio, re, html, json, os, hashlib, glob
from datetime import timezone, datetime, date
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

import requests, httpx, chardet, feedparser, pandas as pd, tldextract
from dateutil import parser as dateparser
from selectolax.parser import HTMLParser
import unicodedata
from ftfy import fix_text
import re

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
TIMEOUT, RETRIES, CONCURRENCY = 35, 2, 10

# ====== util ======
def norm_ws(s:str)->str:
    if not s: return ""
    s = html.unescape(s)
    return re.sub(r"\s+", " ", s).strip()

def to_utf8(s): 
    if s is None: return ""
    if isinstance(s, bytes):
        try: return s.decode("utf-8", errors="ignore")
        except Exception: return s.decode(errors="ignore")
    return s.encode("utf-8", errors="ignore").decode("utf-8", errors="ignore")

def parse_date_any(s: Optional[str]) -> Optional[str]:
    if not s: return None
    try:
        dt = dateparser.parse(s, fuzzy=True)
        if not dt: return None
        return dt.isoformat() if not dt.tzinfo else dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return None

def decode_guess(raw: bytes) -> str:
    if not raw: return ""
    enc = (chardet.detect(raw).get("encoding") or "utf-8").strip()
    try: return raw.decode(enc, errors="replace")
    except Exception: return raw.decode("utf-8", errors="ignore")

def canonicalize_url(url: str) -> str:
    """Quita parámetros de tracking y resuelve redirecciones."""
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT, allow_redirects=True)
        final = r.url
    except Exception:
        final = url
    u = urlparse(final)
    qs = [(k,v) for k,v in parse_qsl(u.query, keep_blank_values=False)
          if k.lower() not in {"utm_source","utm_medium","utm_campaign","utm_term","utm_content","gclid","fbclid"}]
    new_qs = urlencode(qs)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_qs, ""))

def url_sha256(u: str) -> str:
    return hashlib.sha256(u.encode("utf-8")).hexdigest()

def domain_of(url: str) -> str:
    ext = tldextract.extract(url)
    return ".".join([p for p in [ext.domain, ext.suffix] if p])

def date_from_url(url: str) -> Optional[str]:
    pats = [
        r"/(20\d{2})[/-](0[1-9]|1[0-2])[/-](0[1-9]|[12]\d|3[01])",
        r"/(0[1-9]|[12]\d|3[01])[/-](0[1-9]|1[0-2])[/-](20\d{2})",
        r"(20\d{2})\.(0[1-9]|1[0-2])\.(0[1-9]|[12]\d|3[01])",
    ]
    for p in pats:
        m = re.search(p, url)
        if m:
            g = m.groups()
            if len(g)==3:
                if len(g[0])==4: y, mo, d = g[0], g[1], g[2]
                elif len(g[2])==4: y, mo, d = g[2], g[1], g[0]
                else: continue
                return f"{y}-{mo}-{d}T00:00:00"
    return None

def head_requests(url: str) -> Optional[requests.Response]:
    try:
        return requests.head(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT, allow_redirects=True)
    except Exception:
        return None
        
def sanitize_text(s: Optional[str]) -> str:
    """
    Corrige mojibake (Ã¡, Ã±, √±), normaliza NFC, quita controles invisibles
    y colapsa espacios y saltos. Seguro para textos largos.
    """
    if s is None:
        return ""
    if isinstance(s, bytes):
        try:
            s = s.decode("utf-8", errors="ignore")
        except Exception:
            s = s.decode(errors="ignore")
    s = html.unescape(str(s))
    try:
        s = fix_text(s)
    except Exception:
        pass
    s = unicodedata.normalize("NFC", s)
    s = "".join(ch for ch in s if ch == "\n" or ch == "\t" or ord(ch) >= 32)
    s = re.sub(r"[ \t]+\n", "\n", s)
    s = re.sub(r"[ \t]{2,}", " ", s).strip()
    return s

def looks_like_article(url: str, patterns: list[str] | None) -> bool:
    """True si la URL parece artículo según regex por fuente."""
    if not patterns:
        return True
    return any(re.search(p, url) for p in patterns)

def drop_listing_base(urls: list[str], listing_bases: list[str]) -> list[str]:
    """Quita la URL base del listado (portada) que a veces se cuela."""
    bases = {b.rstrip("/") for b in (listing_bases or [])}
    out = []
    for u in urls:
        if u.rstrip("/") in bases:
            continue
        out.append(u)
    return out

# ====== HTTP ======
async def fetch_httpx(client: httpx.AsyncClient, url: str) -> Optional[bytes]:
    last=None
    for _ in range(RETRIES+1):
        try:
            r = await client.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT, follow_redirects=True)
            if 200 <= r.status_code < 400 and r.content:
                return r.content
        except Exception as e:
            last=e
    if last: print(f"[WARN httpx] {url} -> {last}")
    return None

def fetch_requests(url:str)->Optional[bytes]:
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT)
        if 200 <= r.status_code < 400 and r.content:
            return r.content
    except Exception as e:
        print(f"[WARN requests] {url} -> {e}")
    return None

# ====== parsing ======
def extract_title(doc: HTMLParser) -> str:
    # 1) <article> h1 primero (más específico de artículo)
    art = doc.css_first("article")
    if art:
        h = art.css_first("h1")
        if h:
            t = norm_ws(h.text())
            if t:
                return t
    # 2) og:title
    el = doc.css_first('meta[property="og:title"]')
    if el and el.attributes.get("content"):
        return norm_ws(el.attributes["content"])
    # 3) <title>
    if doc.css_first("title"):
        return norm_ws(doc.css_first("title").text())
    # 4) h1 global
    if doc.css_first("h1"):
        return norm_ws(doc.css_first("h1").text())
    return ""

def extract_desc(doc: HTMLParser)->str:
    for sel in ['meta[property="og:description"]','meta[name="description"]']:
        el=doc.css_first(sel)
        if el and el.attributes.get("content"): return norm_ws(el.attributes["content"])
    return ""

def extract_text(doc: HTMLParser)->str:
    art=doc.css_first("article")
    if art:
        ps=[norm_ws(p.text()) for p in art.css("p")]; ps=[p for p in ps if p]
        if ps: return to_utf8("\n".join(ps))
    cands=doc.css('[class*="content"],[class*="post"],[class*="article"],[id*="content"],[id*="post"],[id*="article"]')
    best,score="",0
    for c in cands:
        ps=[norm_ws(p.text()) for p in c.css("p")]; ps=[p for p in ps if p]
        s=sum(len(p) for p in ps)
        if s>score and s>400: best,score="\n".join(ps),s
    return to_utf8(best)

def extract_date_generic(doc: HTMLParser)->Optional[str]:
    metas=[('meta[property="article:published_time"]',"content"),
           ('meta[name="article:published_time"]',"content"),
           ('meta[name="pubdate"]',"content"),
           ('meta[name="publishdate"]',"content"),
           ('meta[name="date"]',"content"),
           ('meta[itemprop="datePublished"]',"content"),
           ('meta[itemprop="dateModified"]',"content"),
           ('meta[property="og:updated_time"]',"content"),
           ('time[datetime]',"datetime")]
    for sel,attr in metas:
        el=doc.css_first(sel)
        if el:
            raw=(el.attributes.get(attr) or "").strip()
            if raw:
                iso=parse_date_any(raw)
                if iso: return iso
    for el in doc.css("time"):
        iso=parse_date_any(norm_ws(el.text()))
        if iso: return iso
    for s in doc.css('script[type="application/ld+json"]'):
        try:
            data=json.loads(s.text()); items=data if isinstance(data,list) else [data]
            for obj in items:
                if isinstance(obj,dict):
                    cand=obj.get("datePublished") or obj.get("dateCreated") or obj.get("dateModified")
                    if cand:
                        iso=parse_date_any(cand)
                        if iso: return iso
        except Exception: pass
    return None

VERDICT_PATTERNS=[
    (r"\b(falso|es\s+falso|bulo|fake|mentira|completamente\s+falso)\b","falso"),
    (r"\b(verdadero|es\s+verdadero|cierto|real)\b","verdadero"),
    (r"\b(engaños[oa]s?|cuestionable|inexact[oa]|imprecis[oa]|inchequeable|no[ -]?verificable|parcialmente\s+verdadero|verdadero\s+pero)\b","dudoso"),
]

def detect_verdict(doc: HTMLParser)->Optional[str]:
    buckets=["[class*='calificac']","[class*='veredicto']","[class*='rating']","[class*='badge']",
             "[id*='calificac']","[id*='veredicto']","[id*='rating']",
             "h1","h2","h3","strong","em","span","p"]
    for sel in buckets:
        for el in doc.css(sel):
            txt=norm_ws(el.text()).lower()
            if not txt: continue
            for pat,lab in VERDICT_PATTERNS:
                if re.search(pat, txt, flags=re.IGNORECASE): return lab
    for sel in ['meta[property="og:title"]','meta[name="description"]','meta[property="og:description"]']:
        el=doc.css_first(sel)
        if el:
            txt=norm_ws(el.attributes.get("content") or "").lower()
            for pat,lab in VERDICT_PATTERNS:
                if re.search(pat, txt, flags=re.IGNORECASE): return lab
    body=norm_ws(doc.body.text() if doc.body else "").lower()
    for pat,lab in VERDICT_PATTERNS:
        if re.search(pat, body, flags=re.IGNORECASE): return lab
    return None

# ====== fuentes ======
TRUSTED_SOURCES = [
    {"source":"ElTiempo","listing_url":"https://www.eltiempo.com/ultimas-noticias","link_selectors":["a[href^='https://www.eltiempo.com/']"]},
    {"source":"ElEspectador","listing_url":"https://www.elespectador.com/ultimas-noticias-colombia/","link_selectors":["a[href^='https://www.elespectador.com/']"]},
    {"source":"LaRepublica","rss":"https://www.larepublica.co/rss/"},
    {"source":"BloombergLinea","listing_url":"https://www.bloomberglinea.com/tags/las-ultimas/","link_selectors":["a[href^='https://www.bloomberglinea.com/']"]},
    {"source":"Semana","listing_url":"https://www.semana.com/ultimas-noticias/","link_selectors":["a[href^='https://www.semana.com/']"]},
    {"source":"NoticiasCaracol","listing_url":"https://noticias.caracoltv.com/ultimas-noticias","link_selectors":["a[href^='https://noticias.caracoltv.com/']"]},
    {"source":"RCNRadio","rss":"https://www.rcnradio.com/rss.xml"},
    {"source":"ElColombiano","listing_url":"https://www.elcolombiano.com/ultimas-noticias","link_selectors":["a[href^='https://www.elcolombiano.com/']"]},
    {"source":"Portafolio","rss":"https://www.portafolio.co/rss.xml"},
    {"source":"Infobae","listing_url":"https://www.infobae.com/colombia/","link_selectors":["a[href^='https://www.infobae.com/colombia/']"]},
]
FACTCHECK_SOURCES = [
    {"source":"Colombiacheck",
     "listing_urls":["https://colombiacheck.com/chequeos","https://colombiacheck.com/"],
     "link_selectors":["a[href*='/chequeo/']","a[href*='/chequeos/']"],
     "restrict":["/chequeo/","/chequeos/"],
     "article_patterns":[r"/chequeo/[^/]+/?$", r"/chequeos/[^/]+/?$"],
     "use_requests": True},

    {"source":"LaSillaVacia",
     "listing_urls":["https://www.lasillavacia.com/detector/","https://www.lasillavacia.com/"],
     "link_selectors":["a[href*='/detector/']"],
     "restrict":["/detector/"],
     "article_patterns":[r"/detector/[^/]+/?$"]},

    {"source":"AFP_Factual",
     "listing_urls":["https://factcheck.afp.com/es"],
     "link_selectors":["a[href^='https://factcheck.afp.com/']"],
     "restrict":["/es/"],
     "article_patterns":[r"/es/[^/]+/?$"]},

    {"source":"EFE_Verifica",
     "listing_urls":["https://efe.com/verifica/"],
     "link_selectors":["a[href*='/verifica/']"],
     "restrict":["/verifica/"],
     "article_patterns":[r"/verifica/[^/]+/?$"]},

    {"source":"Chequeado",
     "listing_urls":["https://chequeado.com/verificaciones/"],
     "link_selectors":["a[href^='https://chequeado.com/']"],
     "restrict":["/verificacion","/verificaciones/"],
     "article_patterns":[r"/verificacion(?:es)?/[^/]+/?$"]},

    {"source":"Maldita",
     "listing_urls":["https://maldita.es/malditobulo/"],
     "link_selectors":["a[href*='/malditobulo/']"],
     "restrict":["/malditobulo/"],
     "article_patterns":[r"/malditobulo/[^/]+/?$", r"/malditobulo/20\d{2}/\d{2}/\d{2}/"]},

    {"source":"Newtral",
     "listing_urls":["https://www.newtral.es/datos/"],
     "link_selectors":["a[href*='/verificacion/']", "a[href*='/bulos/']"],
     "restrict":["/verificacion/","/bulos/"],
     "article_patterns":[r"/verificacion/[^/]+/?$", r"/bulos/[^/]+/?$"]},
]

# ====== listados ======
def urls_from_rss(rss_url:str, limit:int)->List[Tuple[str, Optional[str], Optional[str], Optional[str]]]:
    try:
        fp=feedparser.parse(rss_url); out=[]
        for e in fp.entries[:limit]:
            u=e.get("link"); tt=e.get("title"); sm=e.get("summary")
            pub=e.get("published") or e.get("updated")
            out.append((u, tt, sm, parse_date_any(pub) if pub else None))
        return [x for x in out if x[0]]
    except Exception as e:
        print(f"[WARN RSS] {rss_url}: {e}"); return []

def parse_listing(html_text:str, base_url:str, selectors:List[str], restrict_paths:Optional[List[str]])->List[str]:
    doc=HTMLParser(html_text); urls=[]
    for selector in selectors:
        for a in doc.css(selector):
            href=(a.attributes.get("href") or "").strip()
            if not href: continue
            if href.startswith("//"): href="https:"+href
            if href.startswith("/"):
                parts=base_url.split("/",3)
                origin = parts[0]+"//"+parts[2] if len(parts)>=3 else base_url
                href = origin.rstrip("/") + href
            if any(href.endswith(ext) for ext in (".jpg",".png",".gif",".mp4",".pdf",".svg",".webp","#")): continue
            if restrict_paths and not any(seg in href for seg in restrict_paths): continue
            urls.append(href)
    seen,clean=set(),[]
    for u in urls:
        if u not in seen: seen.add(u); clean.append(u)
    return clean

def listing_pages(base_url:str, pages:int)->List[str]:
    out=[base_url]
    for p in range(2,pages+1):
        out += [f"{base_url.rstrip('/')}/page/{p}", f"{base_url}?page={p}", f"{base_url}?p={p}"]
    return out

def get_listing_html(url:str)->Optional[str]:
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT)
        if 200 <= r.status_code < 400:
            return decode_guess(r.content)
    except Exception as e:
        print(f"[WARN listing] {url} -> {e}")
    return None

# ====== scraping artículo ======
async def scrape_article(client: httpx.AsyncClient, url:str, fb:Dict[str,Any], default_label:Optional[str], use_requests:bool, html_dir:str) -> Dict[str,Any]:
    out={"fuente":fb.get("source",""),"fecha":"", "titulo":"", "texto":"", "estado": default_label or "", "url": url,
         "url_canonica":"", "url_hash":"", "html_raw_path":"", "fecha_crawl": datetime.utcnow().isoformat()}

    # descarga (parche Colombiacheck con requests)
    raw = fetch_requests(url) if use_requests else await fetch_httpx(client, url)
    if not raw: return out

    # canónica + hash
    can = canonicalize_url(url)
    out["url_canonica"] = can
    out["url_hash"] = url_sha256(can)

    # guardar HTML crudo
    os.makedirs(html_dir, exist_ok=True)
    html_path = os.path.join(html_dir, f"{out['url_hash']}.html")
    try:
        with open(html_path, "wb") as f:
            f.write(raw)
        out["html_raw_path"] = html_path.replace("\\","/")
    except Exception as e:
        print(f"[WARN save HTML] {url} -> {e}")

    doc=HTMLParser(decode_guess(raw))
    out["titulo"]=to_utf8(extract_title(doc) or fb.get("title","") or "")
    out["texto"]=to_utf8(extract_text(doc))
    out["titulo"] = sanitize_text(out["titulo"])
    out["texto"]  = sanitize_text(out["texto"])

    date_iso = extract_date_generic(doc) or fb.get("published_at") or date_from_url(can)
    if not date_iso:
        h=head_requests(can)
        if h:
            di = h.headers.get("Last-Modified") or h.headers.get("Date") or h.headers.get("date")
            date_iso = parse_date_any(di) if di else None
    out["fecha"]=date_iso or ""

    if default_label is None:
        v=detect_verdict(doc)
        if v: out["estado"]=v
    return out

# ====== main ======
async def run(outdir, target_per_class, max_fact, max_trusted, pages, target_total, ratios):
    os.makedirs(outdir, exist_ok=True)
    today = str(date.today())
    html_dir = os.path.join(outdir, "html", today)
    jsonl_path = os.path.join(outdir, "jsonl", f"dataset_{today}.jsonl")
    parquet_path = os.path.join(outdir, "parquet", f"dataset_{today}.parquet")
    csv_path = os.path.join(outdir, f"dataset_{today}.csv")
    csv_excel_path = os.path.join(outdir, f"dataset_{today}_excel.csv")
    for d in [os.path.dirname(jsonl_path), os.path.dirname(parquet_path), os.path.join(outdir, "csv"), html_dir]:
        os.makedirs(d, exist_ok=True)
    ratios = dict(item.split("=") for item in ap.parse_args().ratio.split(","))
    ratios = {k.strip(): float(v) for k, v in ratios.items()}

    asyncio.get_event_loop().run_until_complete(
        run(args.outdir, args.target_per_class, args.max_per_factcheck,
        args.max_per_trusted, args.pages, args.target_total, ratios)
    )


    # recolectar jobs
    jobs=[]
    # confiables
    for src in TRUSTED_SOURCES:
        name=src["source"]
        if "rss" in src:
            items=urls_from_rss(src["rss"], max_trusted)
            for (u,tt,sm,dt) in items:
                jobs.append((u, {"source":name, "title":tt, "summary":sm, "published_at":dt}, "verdadero", False))
        else:
            found=[]
            for u in listing_pages(src["listing_url"], pages):
                html_text=get_listing_html(u)
                if not html_text: continue
                found += parse_listing(html_text, u, src["link_selectors"], restrict_paths=None)
            for u in found[:max_trusted]:
                jobs.append((u, {"source":name}, "verdadero", False))
    # verificadores
    for src in FACTCHECK_SOURCES:
        name=src["source"]; use_req=bool(src.get("use_requests", False))
        found=[]
        for base in src["listing_urls"]:
            for u in listing_pages(base, pages):
                html_text=get_listing_html(u)
                if not html_text: continue
                found += parse_listing(html_text, u, src["link_selectors"], restrict_paths=src.get("restrict"))
        found = drop_listing_base(found, src.get("listing_urls", []))
        patterns = src.get("article_patterns")
        found = [x for x in found if looks_like_article(x, patterns)]
        
        # dedup por URL cruda
        seen,clean=set(),[]
        for u in found:
            if u not in seen: seen.add(u); clean.append(u)
            if len(clean)>=max_fact: break
        for u in clean:
            jobs.append((u, {"source":name}, None, use_req))

    # dedup global por URL cruda
    seen,uq=set(),[]
    for u,fb,lab,use_req in jobs:
        if u not in seen: seen.add(u); uq.append((u,fb,lab,use_req))

    print(f"Total URLs a procesar: {len(uq)}")
    results=[]
    limits=httpx.Limits(max_connections=CONCURRENCY, max_keepalive_connections=CONCURRENCY)
    async with httpx.AsyncClient(limits=limits) as client:
        sem=asyncio.Semaphore(CONCURRENCY)
        async def bounded(j):
            u,fb,lab,use_req=j
            async with sem:
                return await scrape_article(client,u,fb,lab,use_req,html_dir)
        BATCH=50
        for i in range(0,len(uq),BATCH):
            chunk=uq[i:i+BATCH]
            rows=await asyncio.gather(*[bounded(j) for j in chunk])
            results.extend(rows)

    df=pd.DataFrame(results, columns=["fuente","fecha","titulo","texto","estado","url","url_canonica","url_hash","html_raw_path","fecha_crawl"])
    df=df[df["titulo"].str.len()>0].copy()
    # dedup del día antes del balance
    key = "url_hash" if ("url_hash" in df.columns and df["url_hash"].notna().any()) else ("url_canonica" if "url_canonica" in df.columns else "url")
    df = df.sort_values(by=["fecha"], ascending=[False]).drop_duplicates(subset=[key], keep="first").copy()


    # completa 'verdadero' si viene de confiables
    trusted_names={s["source"] for s in TRUSTED_SOURCES}
    df.loc[(df["fuente"].isin(trusted_names)) & (df["estado"].isna()), "estado"]="verdadero"

    # normaliza
    def collapse(lbl):
        if not isinstance(lbl,str): return None
        t=lbl.lower()
        if "falso" in t: return "falso"
        if "verdadero" in t or "cierto" in t or "real" in t: return "verdadero"
        return "dudoso"
    df["estado"]=df["estado"].map(collapse)

# ------------------ Balanceo por proporciones (p.ej., 40/40/20) ------------------
import math
from collections import defaultdict

def balanced_sample_by_ratio(df_in, ratios, target_total, seed=42):
    ratios = {k: float(v) for k, v in ratios.items()}
    # normaliza ratios por si no suman exactamente 1
    s = sum(ratios.values()) or 1.0
    ratios = {k: v/s for k, v in ratios.items()}

    # conteos disponibles por clase
    avail = df_in["estado"].value_counts().to_dict()
    classes = list(ratios.keys())

    # primer pase: redondeo al entero más cercano
    desired = {c: int(round(target_total * ratios.get(c, 0.0))) for c in classes}

    # si faltan por el redondeo, ajusta
    diff = target_total - sum(desired.values())
    if diff != 0:
        # reparte el exceso/defecto según la mayor fracción
        order = sorted(classes, key=lambda c: (target_total * ratios.get(c, 0.0)) - desired[c], reverse=(diff>0))
        i = 0
        while diff != 0 and i < len(order):
            desired[order[i]] += 1 if diff>0 else -1 if desired[order[i]]>0 else 0
            diff += -1 if diff>0 else 1
            i = (i+1) % len(order)

    # clip por disponibilidad y calcula déficit
    take = {c: min(desired.get(c, 0), avail.get(c, 0)) for c in classes}
    deficit = target_total - sum(take.values())

    # si hay déficit, redistribuye a clases con remanente
    if deficit > 0:
        # remanente = disponibles - ya tomados
        rem = {c: max(avail.get(c, 0) - take.get(c, 0), 0) for c in classes}
        # ordena por mayor remanente
        pool = sorted(classes, key=lambda c: rem[c], reverse=True)
        j = 0
        while deficit > 0 and any(rem[c] > 0 for c in pool):
            c = pool[j % len(pool)]
            if rem[c] > 0:
                take[c] += 1
                rem[c] -= 1
                deficit -= 1
            j += 1

    # si aún queda déficit (no alcanza el total), tomamos todo lo disponible
    # y seguimos adelante sin drama
    parts = []
    for c, n in take.items():
        if n <= 0: 
            continue
        sub = df_in[df_in["estado"] == c]
        if len(sub) <= n:
            parts.append(sub)
        else:
            parts.append(sub.sample(n=n, random_state=seed))
    if parts:
        out = pd.concat(parts, ignore_index=True)
    else:
        out = df_in.copy()
    return out

    # tamaño objetivo y ratios (ej. 40/40/20)
    df_bal = balanced_sample_by_ratio(df, ratios, target_total)
# -------------------------------------------------------------------------------
    # --- Guardados ---
    # CSV (coma, UTF-8)
    df_bal.to_csv(csv_path, index=False, encoding="utf-8")

    # CSV excel-friendly
    df_x = df_bal.copy()
    for c in df_x.columns:
        df_x[c] = df_x[c].map(sanitize_text)
    df_x.to_csv(csv_excel_path, index=False, sep=";", encoding="utf-8-sig")

    # JSONL
    with open(jsonl_path, "a", encoding="utf-8") as f:
        for _,row in df_bal.iterrows():
            f.write(json.dumps(row.to_dict(), ensure_ascii=False) + "\n")

    # Parquet
    try:
        df_bal.to_parquet(parquet_path, index=False)
    except Exception as e:
        print("[WARN parquet]", e)

    pct_fecha = round((df_bal["fecha"].fillna("").str.len()>0).mean()*100,2)
    print(f"% filas con fecha: {pct_fecha}%")
    print("Salidas:")
    print("-", csv_path)
    print("-", csv_excel_path)
    print("-", jsonl_path)
    print("-", parquet_path)

if __name__=="__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default="data", help="Carpeta de salida")
    ap.add_argument("--target-per-class", type=int, default=120)
    ap.add_argument("--max-per-factcheck", type=int, default=120)
    ap.add_argument("--max-per-trusted", type=int, default=60)
    ap.add_argument("--pages", type=int, default=3)
    args = ap.parse_args()
    ap.add_argument("--target-total", type=int, default=360,
                help="Tamaño objetivo del muestreo balanceado final")
    ap.add_argument("--ratio", type=str, default="falso=0.4,verdadero=0.4,dudoso=0.2",
                help="Proporciones por clase, ej: 'falso=0.4,verdadero=0.4,dudoso=0.2'")


    asyncio.get_event_loop().run_until_complete(
        run(args.outdir, args.target_per_class, args.max_per_factcheck, args.max_per_trusted, args.pages)
    )
