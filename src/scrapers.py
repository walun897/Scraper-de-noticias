from __future__ import annotations
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from dateutil import parser as dtparser
from tenacity import retry, stop_after_attempt, wait_random_exponential
from typing import Dict, List
from .utils import detect_decode, logger
from .config import DEFAULT_LOOKBACK_DAYS, TIMEOUT_SECS

def parse_date_guess(text:str):
    try: return dtparser.parse(text, fuzzy=True)
    except Exception: return None

@retry(stop=stop_after_attempt(3), wait=wait_random_exponential(multiplier=1, max=10))
def _http_get(url:str):
    return requests.get(url, timeout=TIMEOUT_SECS,
        headers={"User-Agent":"Mozilla/5.0 (compatible; NewsScraper/1.1)"} )

def fetch_html(url:str)->BeautifulSoup|None:
    try: resp=_http_get(url)
    except Exception as e:
        logger.warning(f"HTTP error {url}: {e}"); return None
    html = detect_decode(resp.content)
    return BeautifulSoup(html,"lxml") if html else None

def scrape_html_cards(url:str, selectors:Dict[str,str], limit_days:int=DEFAULT_LOOKBACK_DAYS)->List[Dict]:
    soup=fetch_html(url); 
    if soup is None: return []
    out=[]
    for card in soup.select(selectors["item"]):
        def sel(css): 
            if not css: return None
            el=card.select_one(css); return el.get_text(strip=True) if el else None
        def href(css):
            if not css: return None
            el=card.select_one(css); return el.get("href") if el else None
        title=sel(selectors.get("title")); link=href(selectors.get("url"))
        if not title or not link: continue
        summary=sel(selectors.get("summary")) or ""
        raw_date=sel(selectors.get("date")); raw_label=sel(selectors.get("label")) or ""
        dt = parse_date_guess(raw_date) if raw_date else None
        if dt and dt < datetime.utcnow() - timedelta(days=limit_days): continue
        out.append({"title":title,"url":link,"summary":summary,
                    "label_raw":raw_label,"published_at":dt.isoformat() if dt else ""})
    return out

def scrape_rss(url:str, label_field_candidates=("category","title"), limit_days:int=DEFAULT_LOOKBACK_DAYS)->List[Dict]:
    soup=fetch_html(url); 
    if soup is None: return []
    out=[]
    for item in soup.select("item"):
        title=item.title.get_text(strip=True) if item.title else None
        link=item.link.get_text(strip=True) if item.link else None
        if not title or not link: continue
        desc=item.description.get_text(strip=True) if item.description else ""
        pub=None
        if item.pubdate: pub=parse_date_guess(item.pubdate.get_text(strip=True))
        elif item.find("dc:date"): pub=parse_date_guess(item.find("dc:date").get_text(strip=True))
        if pub and pub < datetime.utcnow() - timedelta(days=limit_days): continue
        label_raw=""
        for f in label_field_candidates:
            n=item.find(f)
            if n and n.get_text(strip=True): label_raw=n.get_text(strip=True); break
        out.append({"title":title,"url":link,"summary":desc,"label_raw":label_raw,
                    "published_at":pub.isoformat() if pub else ""})
    return out
