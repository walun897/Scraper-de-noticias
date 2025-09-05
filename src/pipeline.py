from __future__ import annotations
from pathlib import Path
from datetime import datetime
from typing import List, Dict
import pandas as pd

from .config import (
    FACTCHECK_SOURCES, NEWS_SOURCES, STD_FIELDS,
    MAX_TITLE_LEN, MAX_SUMMARY_LEN, MIN_TITLE_CHARS,
    DEFAULT_LOOKBACK_DAYS, FALLBACK_LOOKBACK_DAYS
)
from .labelers import LABELERS
from .scrapers import scrape_rss
from .utils import (
    logger, safe_trim, is_lowinfo_title,
    canonicalize_url, content_fingerprint, replicable_url
)

# Carpetas
DATA_ROOT = Path("data")
DATA_DAILY_DIR = DATA_ROOT / "daily"
DATA_MASTER_DIR = DATA_ROOT / "master"
MASTER = DATA_MASTER_DIR / "dataset_master.csv"

for p in (DATA_DAILY_DIR, DATA_MASTER_DIR):
    if p.exists() and not p.is_dir():
        p.unlink()
    p.mkdir(parents=True, exist_ok=True)

def _normalize_row(source_name: str, source_type: str, row: Dict, label_source: str | None) -> Dict:
    raw_url = (row.get("url") or "").strip()
    title = safe_trim(row.get("title",""), MAX_TITLE_LEN)
    summary = safe_trim(row.get("summary",""), MAX_SUMMARY_LEN)
    if not raw_url or not title or is_lowinfo_title(title) or len(title) < MIN_TITLE_CHARS:
        return {}

    # Etiqueta
    if source_type == "factcheck":
        label_norm = ""
        if label_source:
            lab_fn = LABELERS.get(label_source)
            if lab_fn:
                label_norm = lab_fn(row.get("label_raw","") or "") or ""
        label_origin = "factcheck"
    else:
        # Medios confiables → true
        label_norm = "true"
        label_origin = "trusted_news"

    url_canon = canonicalize_url(row.get("base_url") or raw_url, raw_url)
    h = content_fingerprint(url_canon, row.get("published_at","") or "", title, summary)
    url_rep = replicable_url(url_canon, h)

    return {
        "source": source_name,
        "source_type": source_type,
        "title": title,
        "summary": summary,
        "url": raw_url,
        "url_canonical": url_canon,
        "url_replicable": url_rep,
        "published_at": row.get("published_at","") or "",
        "label_raw": row.get("label_raw","") or "",
        "label": label_norm,
        "label_origin": label_origin,
        "content_hash": h,
    }

def _run_block(sources:list[dict], source_type:str, lookback:int)->pd.DataFrame:
    rows: List[Dict] = []
    for s in sources:
        name,t,url = s["name"], s["type"], s["url"]
        label_src = s.get("label_source")
        logger.info(f"[{source_type.upper()}] {name} → {url}")
        if t!="rss":
            logger.warning(f"[{name}] tipo {t} no RSS (usaremos RSS para estabilidad si está disponible).")
        data = scrape_rss(url, limit_days=lookback)
        logger.info(f"[{source_type.upper()}] {name}: {len(data)} items crudos")
        for r in data:
            norm=_normalize_row(name, source_type, r, label_src)
            if norm: rows.append(norm)
    if not rows: return pd.DataFrame(columns=STD_FIELDS)
    df=pd.DataFrame(rows)[STD_FIELDS]
    # Deduplicación fuerte
    df=df.drop_duplicates(subset=["url_canonical"]).reset_index(drop=True)
    df=df.drop_duplicates(subset=["content_hash"]).reset_index(drop=True)
    return df

def run_all()->pd.DataFrame:
    # Intento normal
    df_fc=_run_block(FACTCHECK_SOURCES,"factcheck",DEFAULT_LOOKBACK_DAYS)
    df_fc=df_fc[df_fc["label"].isin(["true","false","doubtful"])].reset_index(drop=True)
    df_nw=_run_block(NEWS_SOURCES,"news",DEFAULT_LOOKBACK_DAYS)
    # Si muy flaco, ampliamos ventana
    if len(df_fc)<10: 
        logger.warning(f"[FACTCHECK] pocas filas ({len(df_fc)}). Reintento con {FALLBACK_LOOKBACK_DAYS} días.")
        df_fc=_run_block(FACTCHECK_SOURCES,"factcheck",FALLBACK_LOOKBACK_DAYS)
        df_fc=df_fc[df_fc["label"].isin(["true","false","doubtful"])].reset_index(drop=True)
    if len(df_nw)<20:
        logger.warning(f"[NEWS] pocas filas ({len(df_nw)}). Reintento con {FALLBACK_LOOKBACK_DAYS} días.")
        df_nw=_run_block(NEWS_SOURCES,"news",FALLBACK_LOOKBACK_DAYS)
    # Mezcla final en un solo dataset
    df = pd.concat([df_fc, df_nw], ignore_index=True)
    df = df.sample(frac=1.0, random_state=42).reset_index(drop=True)
    logger.info(f"[MERGED] total filas: {len(df)} (factcheck={len(df_fc)} news={len(df_nw)})")
    return df

def save_outputs(df: pd.DataFrame):
    today=datetime.now().date().isoformat()
    daily_path = DATA_DAILY_DIR / f"dataset_{today}.csv"
    df.to_csv(daily_path, index=False, encoding="utf-8")
    if MASTER.exists():
        pd.concat([pd.read_csv(MASTER), df], ignore_index=True)\
          .drop_duplicates(["content_hash","url_canonical"]).to_csv(MASTER, index=False, encoding="utf-8")
    else:
        df.to_csv(MASTER, index=False, encoding="utf-8")
    logger.info(f"Saved daily: {daily_path}  |  Updated master: {MASTER}")

if __name__=="__main__":
    df = run_all()
    save_outputs(df)
