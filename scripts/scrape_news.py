#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, asyncio, re, html, json, os, hashlib, unicodedata
from datetime import timezone, datetime, date
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

import requests, httpx, chardet, feedparser, pandas as pd, tldextract
from dateutil import parser as dateparser
from selectolax.parser import HTMLParser
from ftfy import fix_text

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
TIMEOUT, RETRIES, CONCURRENCY = 35, 2, 10

# ---------------- utils ----------------
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

def sanitize_text(s: Optional[str]) -> str:
    """Corrige mojibake, normaliza Unicode y limpia caracteres de control."""
    if s is None: return ""
    if isinstance(s, bytes):
        try: s = s.decode("utf-8", errors="ignore")
        except Exception: s = s.decode(errors="ignore")
    s = html.unescape(str(s))
    try: s = fix_text(s)
    except Exception: pass
    s = unicodedata.normalize("NFC", s)
    s = "".join(ch for ch in s if ch == "\n" or ch == "\t" or ord(ch) >= 32)
    s = re.sub(r"[ \t]+\n", "\n", s)
    s = re.sub(r"[ \t]{2,}", " ", s).strip()
    return s

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

def head_requests(url: str):
    try:
        return requests.head(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT, allow_redirects=True)
    except Exception:
        return None

# --------------- HTTP ----------------
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

# --------------- parsing ----------------
def extract_title(doc: HTMLParser)->str:
    art = doc.css_first("article")
    if art:
        h = art.css_first("h1")
        if h:
            t = norm_ws(h.text())
            if t: return t
    el=doc.css_first('meta[property="og:title"]')
    if el and el.attributes.get("content"): return norm_ws(el.attributes["content"])
    if doc.css_first("title"): return norm_ws(doc.css_first("title").text())
    if doc.css_first("h1"): return norm_ws(doc.css_first("h1").text())
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
    (r"\b(falso|bulo|fake|mentira|completamente\s+falso)\b","falso"),
    (r"\b(verdadero|cierto|real)\b","verdadero"),
    (r"\b(engaños[oa]s?|cuestionable|inexact[oa]|imprecis[oa]|inchequeable|no[ -]?verificable|parcialmente\s+verdadero|verdadero\s+pero)\b","dudoso"),
]

def detect_verdict(doc: HTMLParser)->Optional[str]:
    for sel in ["[class*='calificac']","[class*='veredicto']","[class*='rating']","h1","h2","h3","strong","em","span","p"]:
        for el in doc.css(sel):
            txt=norm_ws(el.text()).lower()
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

# --------------- fuentes ----------------
TRUSTED_SOURCES = [
    {"source":"LaRepublica","rss":"https://www.larepublica.co/rss/"},
    {"source":"Portafolio","rss":"https://www.portafolio.co/rss.xml"},
    {"source":"RCNRadio","rss":"https://www.rcnradio.com/rss.xml"},
    {"source":"ElTiempo","listing_url":"https://www.eltiempo.com/ultimas-noticias","link_selectors":["a[href^='https://www.eltiempo.com/']"]},
    {"source":"ElEspectador","listing_url":"https://www.elespectador.com/ultimas-noticias-colombia/","link_selectors":["a[href^='https://www.elespectador.com/']"]},
    {"source":"BloombergLinea","listing_url":"https://www.bloomberglinea.com/tags/las-ultimas/","link_selectors":["a[href^='https://www.bloomberglinea.com/']"]},
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

# --------------- listados ----------------
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

def looks_like_article(url: str, patterns: Optional[List[str]]) -> bool:
    if not patterns: return True
    return any(re.search(p, url) for p in patterns)

def drop_listing_base(urls: List[str], listing_bases: List[str]) -> List[str]:
    bases = {b.rstrip("/") for b in (listing_bases or [])}
    return [u for u in urls if u.rstrip("/") not in bases]

# --------------- scraping artículo ----------------
async def scrape_article(client: httpx.AsyncClient, url:str, fb:Dict[str,Any], default_label:Optional[str], use_requests:bool, html_dir:str) -> Dict[str,Any]:
    out={"fuente":fb.get("source",""),"fecha":"", "titulo":"", "texto":"", "estado": default_label or "", "url": url,
         "url_canonica":"", "url_hash":"", "html_raw_path":"", "fecha_crawl": datetime.utcnow().isoformat()}

    raw = fetch_requests(url) if use_requests else await fetch_httpx(client, url)
    if not raw: return out

    can = canonicalize_url(url)
    out["url_canonica"] = can
    out["url_hash"] = url_sha256(can)

    os.makedirs(html_dir, exist_ok=True)
    html_path = os.path.join(html_dir, f"{out['url_hash']}.html")
    try:
        with open(html_path, "wb") as f: f.write(raw)
        out["html_raw_path"] = html_path.replace("\\","/")
    except Exception as e:
        print(f"[WARN save HTML] {url} -> {e}")

    doc=HTMLParser(decode_guess(raw))
    out["titulo"]=sanitize_text(to_utf8(extract_title(doc) or fb.get("title","") or ""))
    out["texto"]=sanitize_text(to_utf8(extract_text(doc)))

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

# --------------- main ----------------
async def run(outdir:str, target_per_class:int, max_fact:int, max_trusted:int, pages:int, target_total:int, ratios:Dict[str,float]):
    os.makedirs(outdir, exist_ok=True)
    today = str(date.today())
    html_dir = os.path.join(outdir, "html", today)
    jsonl_path = os.path.join(outdir, "jsonl", f"dataset_{today}.jsonl")
    parquet_path = os.path.join(outdir, "parquet", f"dataset_{today}.parquet")
    csv_path = os.path.join(outdir, f"dataset_{today}.csv")
    csv_excel_path = os.path.join(outdir, f"dataset_{today}_excel.csv")
    for d in [os.path.dirname(jsonl_path), os.path.dirname(parquet_path), os.path.join(outdir, "csv"), html_dir]:
        os.makedirs(d, exist_ok=True)

    jobs=[]

    # Confiables
    for src in TRUSTED_SOURCES:
        name=src["source"]
        if "rss" in src:
            items=[]
            try:
                fp=feedparser.parse(src["rss"])
                for e in fp.entries[:max_trusted]:
                    items.append((e.get("link"), e.get("title"), e.get("summary"), parse_date_any(e.get("published") or e.get("updated"))))
            except Exception as e:
                print(f"[WARN RSS] {src['rss']}: {e}")
            for (u,tt,sm,dt) in items:
                if not u: continue
                jobs.append((u, {"source":name, "title":tt, "summary":sm, "published_at":dt}, "verdadero", False))
        else:
            found=[]
            for u in listing_pages(src["listing_url"], pages):
                html_text=get_listing_html(u)
                if not html_text: continue
                found += parse_listing(html_text, u, src["link_selectors"], restrict_paths=None)
            seen=set(); clean=[]
            for u in found:
                if u not in seen: seen.add(u); clean.append(u)
            for u in clean[:max_trusted]:
                jobs.append((u, {"source":name}, "verdadero", False))

    # Verificadores
    for src in FACTCHECK_SOURCES:
        name=src["source"]; use_req=bool(src.get("use_requests", False))
        found=[]
        for base in src["listing_urls"]:
            for u in listing_pages(base, pages):
                html_text=get_listing_html(u)
                if not html_text: continue
                links = parse_listing(html_text, u, src["link_selectors"], restrict_paths=src.get("restrict"))
                links = drop_listing_base(links, src.get("listing_urls", []))
                links = [x for x in links if looks_like_article(x, src.get("article_patterns"))]
                found += links
        seen=set(); clean=[]
        for u in found:
            if u not in seen: seen.add(u); clean.append(u)
            if len(clean)>=max_fact: break
        for u in clean:
            jobs.append((u, {"source":name}, None, use_req))

    # dedup por URL cruda
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

    # filtros de calidad mínimos
    df = df[df["titulo"].fillna("").str.len() >= 12].copy()
    df = df[df["texto"].fillna("").str.len() >= 200].copy()

    # completa 'verdadero' si viene de confiables
    trusted_names={s["source"] for s in TRUSTED_SOURCES}
    df.loc[(df["fuente"].isin(trusted_names)) & (df["estado"].isna()), "estado"]="verdadero"

    # normaliza labels
    def collapse(lbl):
        if not isinstance(lbl,str): return None
        t=lbl.lower()
        if "falso" in t or "bulo" in t or "fake" in t: return "falso"
        if "verdadero" in t or "cierto" in t or "real" in t: return "verdadero"
        return "dudoso"
    df["estado"]=df["estado"].map(collapse)

    # dedup del día (hash > canónica > url)
    key = "url_hash" if ("url_hash" in df.columns and df["url_hash"].notna().any()) else ("url_canonica" if "url_canonica" in df.columns else "url")
    df = df.sort_values(by=["fecha"], ascending=[False]).drop_duplicates(subset=[key], keep="first").copy()

    print("==== STATS ====")
    print("Filas post-filtros/pre-balance:", len(df))
    if not df.empty:
        print(df["estado"].value_counts(dropna=False))
    else:
        print("⚠️ df vacío")

    # ---------- Balance 40/40/20 (configurable) ----------
    def balanced_sample_by_ratio(df_in, ratios, target_total, seed=42):
        ratios = {k: float(v) for k, v in ratios.items()}
        s = sum(ratios.values()) or 1.0
        ratios = {k: v/s for k, v in ratios.items()}
        avail = df_in["estado"].value_counts().to_dict()
        classes = list(ratios.keys())
        desired = {c: int(round(target_total * ratios.get(c, 0.0))) for c in classes}
        diff = target_total - sum(desired.values())
        if diff != 0:
            order = sorted(classes, key=lambda c: (target_total * ratios.get(c, 0.0)) - desired[c], reverse=(diff>0))
            i = 0
            while diff != 0 and i < len(order):
                if diff>0: desired[order[i]] += 1; diff -= 1
                elif desired[order[i]]>0: desired[order[i]] -= 1; diff += 1
                i = (i+1) % len(order)
        take = {c: min(desired.get(c, 0), avail.get(c, 0)) for c in classes}
        deficit = target_total - sum(take.values())
        if deficit > 0:
            rem = {c: max(avail.get(c, 0) - take.get(c, 0), 0) for c in classes}
            pool = sorted(classes, key=lambda c: rem[c], reverse=True)
            j = 0
            while deficit > 0 and any(rem[c] > 0 for c in pool):
                c = pool[j % len(pool)]
                if rem[c] > 0:
                    take[c] += 1; rem[c] -= 1; deficit -= 1
                j += 1
        parts=[]
        for c,n in take.items():
            if n<=0: continue
            sub=df_in[df_in["estado"]==c]
            parts.append(sub if len(sub)<=n else sub.sample(n=n, random_state=seed))
        return pd.concat(parts, ignore_index=True) if parts else df_in.copy()

    df_bal = balanced_sample_by_ratio(df, ratios, target_total)
    # ------------------------------------------------------

    # ---- Guardados (siempre guardamos, aunque esté vacío) ----
    os.makedirs(os.path.join(outdir, "jsonl"), exist_ok=True)
    os.makedirs(os.path.join(outdir, "parquet"), exist_ok=True)

    df_bal.to_csv(csv_path, index=False, encoding="utf-8")

    df_x = df_bal.copy()
    for c in df_x.columns:
        df_x[c] = df_x[c].map(sanitize_text)
    df_x.to_csv(csv_excel_path, index=False, sep=";", encoding="utf-8-sig")

    with open(jsonl_path, "a", encoding="utf-8") as f:
        for _,row in df_bal.iterrows():
            f.write(json.dumps(row.to_dict(), ensure_ascii=False) + "\n")

    try:
        df_bal.to_parquet(parquet_path, index=False)
    except Exception as e:
        print("[WARN parquet]", e)

    pct_fecha = round((df_bal["fecha"].fillna("").str.len()>0).mean()*100,2) if not df_bal.empty else 0.0
    print(f"% filas con fecha: {pct_fecha}%")
    print("Salidas:")
    print("-", csv_path)
    print("-", csv_excel_path)
    print("-", jsonl_path)
    print("-", parquet_path)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default="data", help="Carpeta de salida")
    ap.add_argument("--target-per-class", type=int, default=120)
    ap.add_argument("--max-per-factcheck", type=int, default=120)
    ap.add_argument("--max-per-trusted", type=int, default=60)
    ap.add_argument("--pages", type=int, default=3)
    ap.add_argument("--target-total", type=int, default=360, help="Número total balanceado")
    ap.add_argument("--ratio", type=str, default="falso=0.4,verdadero=0.4,dudoso=0.2", help="Proporción por clase")

    args = ap.parse_args()
    ratios = {k.strip(): float(v) for k,v in (item.split("=") for item in args.ratio.split(","))}

    coro = run(args.outdir, args.target_per_class, args.max_per_factcheck,
               args.max_per_trusted, args.pages, args.target_total, ratios)

    # launcher asyncio robusto
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = None
    if loop is None or loop.is_closed():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    if loop.is_running():
        new_loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(new_loop)
            new_loop.run_until_complete(coro)
        finally:
            new_loop.close()
            asyncio.set_event_loop(loop)
    else:
        loop.run_until_complete(coro)
