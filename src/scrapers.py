from __future__ import annotations
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from dateutil import parser as dtparser
from typing import Dict, List

from .utils import detect_decode, logger
from .config import TIMEOUT_SECS, DEFAULT_LOOKBACK_DAYS

def parse_date_guess(text:str):
    try: return dtparser.parse(text, fuzzy=True)
    except Exception: return None

def fetch_html(url:str)->BeautifulSoup|None:
    try:
        r = requests.get(url, timeout=TIMEOUT_SECS,
            headers={"User-Agent":"Mozilla/5.0 (compatible; NewsScraper/1.0)"})
        html = detect_decode(r.content)
        return BeautifulSoup(html, "lxml") if html else None
    except Exception as e:
        logger.warning(f"HTTP {url}: {e}")
        return None

def scrape_rss(url:str, label_field_candidates=("category","title"), lookback:int=DEFAULT_LOOKBACK_DAYS)->List[Dict]:
    soup = fetch_html(url)
    if soup is None: return []
    out = []
    min_dt = datetime.utcnow() - timedelta(days=lookback)
    for item in soup.select("item"):
        title = item.title.get_text(strip=True) if item.title else None
        link  = item.link.get_text(strip=True)  if item.link  else None
        if not title or not link: continue
        desc  = item.description.get_text(strip=True) if item.description else ""
        pub   = None
        if item.pubdate:         pub = parse_date_guess(item.pubdate.get_text(strip=True))
        elif item.find("dc:date"): pub = parse_date_guess(item.find("dc:date").get_text(strip=True))
        if pub and pub < min_dt: continue
        label_raw = ""
        for f in label_field_candidates:
            n = item.find(f)
            if n and n.get_text(strip=True):
                label_raw = n.get_text(strip=True); break
        out.append({
            "title": title, "url": link, "summary": desc,
            "label_raw": label_raw, "published_at": pub.isoformat() if pub else ""
        })
    return out
