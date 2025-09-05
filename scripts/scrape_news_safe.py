#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, asyncio, os, re, html, json, hashlib, unicodedata
from datetime import date, datetime, timezone
from typing import List, Dict, Any, Optional
import httpx, requests, chardet, pandas as pd
from selectolax.parser import HTMLParser
from ftfy import fix_text
from dateutil import parser as dateparser

USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
TIMEOUT, RETRIES, CONCURRENCY = 35, 2, 10

# -------------------- helpers --------------------
def to_utf8(s):
    if s is None: return ""
    if isinstance(s, bytes):
        try: return s.decode("utf-8", errors="ignore")
        except Exception: return s.decode(errors="ignore")
    return s

def decode_guess(raw: bytes) -> str:
    if not raw: return ""
    enc = (chardet.detect(raw).get("encoding") or "utf-8").strip()
    try: return raw.decode(enc, errors="replace")
    except Exception: return raw.decode("utf-8", errors="ignore")

def norm_ws(s:str)->str:
    if not s: return ""
    s = html.unescape(s)
    return re.sub(r"\s+", " ", s).strip()

def sanitize_text(s: Optional[str]) -> str:
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

def url_sha256(u: str) -> str:
    return hashlib.sha256(u.encode("utf-8")).hexdigest()

async def fetch(client: httpx.AsyncClient, url: str) -> Optional[bytes]:
    last=None
    for _ in range(RETRIES+1):
        try:
            r = await client.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT, follow_redirects=True)
            if 200 <= r.status_code < 400 and r.content:
                return r.content
        except Exception as e:
            last=e
    if last: print(f"[WARN] {url} -> {last}")
    return None

def extract_title(doc: HTMLParser) -> str:
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

def extract_text(doc: HTMLParser) -> str:
    art = doc.css_first("article")
    if art:
        ps = [norm_ws(p.text()) for p in art.css("p")]
        ps = [p for p in ps if p]
        if ps: return to_utf8("\n".join(ps))
    cands = doc.css('[class*="content"],[class*="post"],[class*="article"],[id*="content"],[id*="post"],[id*="article"]')
    best,score="",0
    for c in cands:
        ps=[norm_ws(p.text()) for p in c.css("p")]; ps=[p for p in ps if p]
        s=sum(len(p) for p in ps)
        if s>score and s>400: best,score="\n".join(ps),s
    return to_utf8(best)

def extract_date(doc: HTMLParser) -> Optional[str]:
    metas=[('meta[property="article:published_time"]',"content"),
           ('meta[itemprop="datePublished"]',"content"),
           ('meta[name="date"]',"content"),
           ('meta[name="publishdate"]',"content"),
           ('time[datetime]',"datetime")]
    for sel,attr in metas:
        el=doc.css_first(sel)
        if el:
            raw=(el.attributes.get(attr) or "").strip()
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

FACTCHECK_SOURCES = [
    # Colombia / región + ES
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

def parse_listing(html_text:str, base_url:str, selectors:List[str], restrict_paths:List[str]|None)->List[str]:
    doc=HTMLParser(html_text); urls=[]
    for selector in selectors:
        for a in doc.css(selector):
            href=(a.attributes.get("href") or "").strip()
            if not href: continue
            if href.startswith("//"): href="https:"+href
            if href.startswith("/"):
                origin = base_url.split("/",3)
                origin = origin[0]+"//"+origin[2] if len(origin)>=3 else base_url
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

async def scrape_article(client:httpx.AsyncClient, url:str, src_name:str, use_requests:bool, html_dir:str)->Dict[str,Any]:
    out={"fuente":src_name,"fecha":"", "titulo":"", "texto":"", "estado": None, "url": url,
         "url_canonica":"", "url_hash":"", "html_raw_path":"", "fecha_crawl": datetime.utcnow().isoformat()}

    # requests para sitios que rompen httpx (p. ej. colombiacheck)
    if use_requests:
        try:
            r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT)
            if not (200 <= r.status_code < 400): return out
            raw = r.content
        except Exception as e:
            print(f"[WARN requests] {url} -> {e}"); return out
    else:
        raw = await fetch(client, url)
        if not raw: return out

    can = url
    out["url_canonica"]=can
    out["url_hash"]=url_sha256(can)

    os.makedirs(html_dir, exist_ok=True)
    html_path=os.path.join(html_dir, f"{out['url_hash']}.html")
    try:
        with open(html_path, "wb") as f: f.write(raw)
        out["html_raw_path"]=html_path.replace("\\","/")
    except Exception as e:
        print(f"[WARN save HTML] {url} -> {e}")

    doc=HTMLParser(decode_guess(raw))
    out["titulo"]=sanitize_text(to_utf8(extract_title(doc)))
    out["texto"]=sanitize_text(to_utf8(extract_text(doc)))
    out["fecha"]=extract_date(doc) or ""

    v = detect_verdict(doc)
    out["estado"] = v if v else "dudoso"
    return out

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

async def run(outdir:str, pages:int, max_per_fact:int, target_total:int, ratios:Dict[str,float], min_title:int, min_text:int, require_date:int):
    os.makedirs(outdir, exist_ok=True)
    today=str(date.today())
    html_dir=os.path.join(outdir,"html",today)
    jsonl_path=os.path.join(outdir,"jsonl",f"dataset_{today}.jsonl")
    parquet_path=os.path.join(outdir,"parquet",f"dataset_{today}.parquet")
    csv_path=os.path.join(outdir,f"dataset_{today}.csv")
    csv_excel_path=os.path.join(outdir,f"dataset_{today}_excel.csv")
    for d in [os.path.dirname(jsonl_path), os.path.dirname(parquet_path), html_dir]:
        os.makedirs(d, exist_ok=True)

    jobs=[]
    # *** SOLO FACT-CHECKERS ***
    for src in FACTCHECK_SOURCES:
        name=src["source"]; use_req=bool(src.get("use_requests", False))
        found=[]
        for base in src["listing_urls"]:
            for p in range(1, pages+1):
                u = base if p==1 else f"{base.rstrip('/')}/page/{p}"
                try:
                    r = requests.get(u, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT)
                    if not (200<=r.status_code<400): continue
                    html_text=decode_guess(r.content)
                except Exception:
                    continue
                links = parse_listing(html_text, u, src["link_selectors"], restrict_paths=src.get("restrict"))
                # filtra portada/listado y deja solo artículos
                bases = {b.rstrip("/") for b in src.get("listing_urls", [])}
                links = [x for x in links if x.rstrip("/") not in bases]
                pats = src.get("article_patterns")
                if pats:
                    links = [x for x in links if any(re.search(p, x) for p in pats)]
                found += links
        # únicos y tope
        uniq=[]
        seen=set()
        for u in found:
            if u not in seen:
                seen.add(u); uniq.append(u)
            if len(uniq) >= max_per_fact: break
        for u in uniq:
            jobs.append((u, name, use_req))

    print("URLs a procesar:", len(jobs))
    results=[]
    limits=httpx.Limits(max_connections=CONCURRENCY, max_keepalive_connections=CONCURRENCY)
    async with httpx.AsyncClient(limits=limits) as client:
        sem=asyncio.Semaphore(CONCURRENCY)
        async def bounded(j):
            u,name,use_req=j
            async with sem:
                return await scrape_article(client, u, name, use_req, html_dir)
        B=40
        for i in range(0,len(jobs),B):
            chunk=jobs[i:i+B]
            rows=await asyncio.gather(*[bounded(j) for j in chunk])
            results.extend(rows)

    df=pd.DataFrame(results, columns=["fuente","fecha","titulo","texto","estado","url","url_canonica","url_hash","html_raw_path","fecha_crawl"])
    if df.empty:
        print("⚠️ df vacío tras scraper"); 
    # filtros de calidad
    df = df[df["titulo"].fillna("").str.len() >= min_title].copy()
    df = df[df["texto"].fillna("").str.len() >= min_text].copy()
    if require_date:
        df = df[df["fecha"].fillna("").str.len()>0].copy()

    # normaliza labels
    def collapse(lbl):
        if not isinstance(lbl,str): return None
        t=lbl.lower()
        if "falso" in t or "bulo" in t or "fake" in t: return "falso"
        if "verdadero" in t or "cierto" in t or "real" in t: return "verdadero"
        return "dudoso"
    df["estado"]=df["estado"].map(collapse)

    # dedup del día
    key = "url_hash" if ("url_hash" in df.columns and df["url_hash"].notna().any()) else ("url_canonica" if "url_canonica" in df.columns else "url")
    df = df.sort_values(by=["fecha"], ascending=[False]).drop_duplicates(subset=[key], keep="first").copy()

    print("==== STATS ====")
    print("Filas post-filtros/pre-balance:", len(df))
    if not df.empty: print(df["estado"].value_counts(dropna=False))

    # balance 40/40/20
    df_bal = balanced_sample_by_ratio(df, ratios, target_total)

    # guardados SIEMPRE
    os.makedirs(os.path.join(outdir,"jsonl"), exist_ok=True)
    os.makedirs(os.path.join(outdir,"parquet"), exist_ok=True)

    df_bal.to_csv(csv_path, index=False, encoding="utf-8")

    df_x = df_bal.copy()
    for c in df_x.columns: df_x[c] = df_x[c].map(sanitize_text)
    df_x.to_csv(csv_excel_path, index=False, sep=";", encoding="utf-8-sig")

    with open(jsonl_path,"a",encoding="utf-8") as f:
        for _,row in df_bal.iterrows():
            f.write(json.dumps(row.to_dict(), ensure_ascii=False)+"\n")
    try:
        df_bal.to_parquet(parquet_path, index=False)
    except Exception as e:
        print("[WARN parquet]", e)

    pct_fecha = round((df_bal["fecha"].fillna("").str.len()>0).mean()*100,2) if not df_bal.empty else 0.0
    print(f"% filas con fecha: {pct_fecha}%")
    print("Salidas:", csv_path, csv_excel_path, jsonl_path, parquet_path)

if __name__=="__main__":
    ap=argparse.ArgumentParser()
    ap.add_argument("--outdir", default="data")
    ap.add_argument("--pages", type=int, default=3)
    ap.add_argument("--max-per-factcheck", type=int, default=300)
    ap.add_argument("--target-total", type=int, default=300)
    ap.add_argument("--ratio", type=str, default="falso=0.4,verdadero=0.4,dudoso=0.2")
    ap.add_argument("--min-title", type=int, default=12)
    ap.add_argument("--min-text", type=int, default=300)
    ap.add_argument("--require-date", type=int, default=0)
    args=ap.parse_args()
    ratios = {k.strip(): float(v) for k,v in (item.split("=") for item in args.ratio.split(","))}
    coro = run(args.outdir, args.pages, args.max_per_factcheck, args.target_total, ratios, args.min_title, args.min_text, args.require_date)

    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = None
    if loop is None or loop.is_closed():
        loop = asyncio.new_event_loop(); asyncio.set_event_loop(loop)
    if loop.is_running():
        new_loop=asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(new_loop); new_loop.run_until_complete(coro)
        finally:
            new_loop.close(); asyncio.set_event_loop(loop)
    else:
        loop.run_until_complete(coro)
