from __future__ import annotations
from pathlib import Path
from datetime import datetime
from typing import List, Dict
import os
import pandas as pd

from .config import (
    FACTCHECK_SOURCES, NEWS_SOURCES, STD_FIELDS,
    MAX_TITLE_LEN, MAX_SUMMARY_LEN, MIN_TITLE_CHARS,
    DEFAULT_LOOKBACK_DAYS, FALLBACK_LOOKBACK_DAYS,
    MIN_ROWS_FACTCHECK, MIN_ROWS_NEWS, MAX_PAGES_HTML,
    ENABLE_CONTENT_EXTRACTION
)
from .labelers import LABELERS
from .scrapers import scrape_html_cards, scrape_rss
from .utils import (
    logger, safe_trim, is_lowinfo_title,
    canonicalize_url, content_fingerprint, replicable_url
)

# Carpetas robustas
DATA_ROOT = Path("data")
DATA_DAILY_DIR = DATA_ROOT / "daily"
DATA_MASTER_DIR = DATA_ROOT / "master"
FC_MASTER = DATA_MASTER_DIR / "factcheck_master.csv"
NW_MASTER = DATA_MASTER_DIR / "news_master.csv"

for p in (DATA_DAILY_DIR, DATA_MASTER_DIR):
    if p.exists() and not p.is_dir():
        p.unlink()
    p.mkdir(parents=True, exist_ok=True)

# Selectores HTML (ajustables por fuente)
HTML_SELECTORS = {
    # FACT-CHECKERS (algunos ejemplos; el resto entran por RSS o páginas simples)
    "Colombiacheck": {"item":"div.views-row","title":"h2 a","url":"h2 a","summary":".field-content .text p","date":"time","label":".field-name-field-calificacion .field-item"},
    "Maldita.es": {"item":"article","title":"h2 a, h3 a","url":"h2 a, h3 a","summary":"p","date":"time, .date","label":".badge, .tag, .c-label"},
    "Newtral": {"item":"article","title":"h2 a, h3 a","url":"h2 a, h3 a","summary":"p","date":"time","label":".etiqueta, .tag"},
    "Chequeado": {"item":"article","title":"h2 a, h3 a","url":"h2 a, h3 a","summary":"p","date":"time","label":".badge, .etiqueta"},
    "EFE Verifica": {"item":"article","title":"h2 a, h3 a","url":"h2 a, h3 a","summary":"p","date":"time","label":".etiqueta, .cat"},
    "Verificat": {"item":"article","title":"h2 a, h3 a","url":"h2 a, h3 a","summary":"p","date":"time","label":".label, .tag"},
    "El Sabueso (Animal Político)": {"item":"article","title":"h2 a, h3 a","url":"h2 a, h3 a","summary":"p","date":"time","label":".etiqueta, .tag"},
    "VerificadoMX": {"item":"article","title":"h2 a, h3 a","url":"h2 a, h3 a","summary":"p","date":"time","label":".etiqueta, .tag"},
    # NEWS (HTML simples; muchas otras entran por RSS)
    "El Tiempo": {"item":"article, li","title":"a","url":"a","summary":"p, .lead","date":"time"},
    "El Espectador": {"item":"article, li","title":"a","url":"a","summary":"p, .lead","date":"time"},
    "La Silla Vacía": {"item":"article, li","title":"a","url":"a","summary":"p, .lead","date":"time"},
    "El Universal MX": {"item":"article, li","title":"a","url":"a","summary":"p, .lead","date":"time"},
    "El Comercio PE": {"item":"article, li","title":"a","url":"a","summary":"p, .lead","date":"time"},
}

def _normalize_row(source_name: str, row: Dict, label_source: str | None) -> Dict:
    raw_url = (row.get("url") or "").strip()
    title = safe_trim(row.get("title",""), MAX_TITLE_LEN)
    summary = safe_trim(row.get("summary",""), MAX_SUMMARY_LEN)
    if not raw_url or not title or is_lowinfo_title(title) or len(title.strip()) < MIN_TITLE_CHARS:
        return {}

    # Etiqueta normalizada (si la fuente es de fact-check)
    label_norm = ""
    if label_source:
        lab_fn = LABELERS.get(label_source)
        if lab_fn:
            label_norm = lab_fn(row.get("label_raw","") or "") or ""

    base = row.get("base_url") or raw_url
    url_canon = canonicalize_url(base, raw_url)

    # Enriquecimiento opcional (texto completo) para hash más fuerte
    content_len = 0
    if ENABLE_CONTENT_EXTRACTION:
        try:
            from .content_extractor import extract_main_text
            art = extract_main_text(url_canon)
            content = (art.get("content") or "")
            content_len = len(content)
            h = content_fingerprint(url_canon, row.get("published_at","") or "", title, content)
        except Exception:
            h = content_fingerprint(url_canon, row.get("published_at","") or "", title, summary)
    else:
        h = content_fingerprint(url_canon, row.get("published_at","") or "", title, summary)

    url_rep = replicable_url(url_canon, h)

    out = {
        "source": source_name,
        "title": title,
        "summary": summary,
        "url": raw_url,
        "url_canonical": url_canon,
        "url_replicable": url_rep,
        "published_at": row.get("published_at","") or "",
        "label_raw": row.get("label_raw","") or "",
        "label": label_norm,
        "content_hash": h,
        "content_len": content_len,
    }
    return out

def _run_block(sources: list[dict], lookback_days: int) -> pd.DataFrame:
    rows: List[Dict] = []
    for s in sources:
        name, t, url = s["name"], s["type"], s["url"]
        label_src = s.get("label_source")
        logger.info(f"[SCRAPE] {name} -> {url}")
        if t == "rss":
            data = scrape_rss(url, limit_days=lookback_days)
        elif t == "html":
            selectors = HTML_SELECTORS.get(name)
            if not selectors:
                logger.warning(f"[SCRAPE] Sin selectores para {name}; skip")
                continue
            data = scrape_html_cards(url, selectors, limit_days=lookback_days, max_pages=MAX_PAGES_HTML)
        else:
            logger.warning(f"[SCRAPE] Tipo desconocido {t} en {name}")
            continue

        logger.info(f"[SCRAPE] {name}: {len(data)} items crudos")
        for r in data:
            norm = _normalize_row(name, r, label_src)
            if norm:
                rows.append(norm)

    if not rows:
        return pd.DataFrame(columns=STD_FIELDS)
    df = pd.DataFrame(rows)[STD_FIELDS]
    # Dedupe fuerte: por url_canonical y luego por content_hash
    df = df.drop_duplicates(subset=["url_canonical"]).reset_index(drop=True)
    df = df.drop_duplicates(subset=["content_hash"]).reset_index(drop=True)
    return df

def run_all():
    # Fact-check (ventana por defecto)
    df_fc = _run_block(FACTCHECK_SOURCES, lookback_days=DEFAULT_LOOKBACK_DAYS)
    if len(df_fc) < MIN_ROWS_FACTCHECK:
        logger.warning(f"[FACTCHECK] pocas filas ({len(df_fc)}), ampliando ventana a {FALLBACK_LOOKBACK_DAYS} días")
        df_fc = _run_block(FACTCHECK_SOURCES, lookback_days=FALLBACK_LOOKBACK_DAYS)
    # filtra solo etiquetas válidas
    before = len(df_fc)
    df_fc = df_fc[df_fc["label"].isin(["true","false","doubtful"])].reset_index(drop=True)
    logger.info(f"[FACTCHECK] tras filtro etiquetas: {len(df_fc)} (antes {before})")

    # News (ventana por defecto)
    df_nw = _run_block(NEWS_SOURCES, lookback_days=DEFAULT_LOOKBACK_DAYS)
    if len(df_nw) < MIN_ROWS_NEWS:
        logger.warning(f"[NEWS] pocas filas ({len(df_nw)}), ampliando ventana a {FALLBACK_LOOKBACK_DAYS} días")
        df_nw = _run_block(NEWS_SOURCES, lookback_days=FALLBACK_LOOKBACK_DAYS)

    return df_fc, df_nw

def _save(df: pd.DataFrame, path: Path):
    df.to_csv(path, index=False, encoding="utf-8")
    logger.info(f"Saved {path} ({len(df)} rows)")

def save_outputs(df_fc: pd.DataFrame, df_nw: pd.DataFrame):
    today = datetime.now().date().isoformat()
    fc_path = DATA_DAILY_DIR / f"factcheck_{today}.csv"
    nw_path = DATA_DAILY_DIR / f"news_{today}.csv"
    _save(df_fc, fc_path)
    _save(df_nw, nw_path)

    if FC_MASTER.exists():
        pd.concat([pd.read_csv(FC_MASTER), df_fc], ignore_index=True)\
          .drop_duplicates(["content_hash","url_canonical"]).to_csv(FC_MASTER, index=False, encoding="utf-8")
    else:
        df_fc.to_csv(FC_MASTER, index=False, encoding="utf-8")

    if NW_MASTER.exists():
        pd.concat([pd.read_csv(NW_MASTER), df_nw], ignore_index=True)\
          .drop_duplicates(["content_hash","url_canonical"]).to_csv(NW_MASTER, index=False, encoding="utf-8")
    else:
        df_nw.to_csv(NW_MASTER, index=False, encoding="utf-8")

if __name__ == "__main__":
    from .balance import stratified_balance
    df_fc, df_nw = run_all()
    if not df_fc.empty:
        df_fc = stratified_balance(df_fc)
    save_outputs(df_fc, df_nw)
    logger.info(f"Done. factcheck={len(df_fc)} news={len(df_nw)}")
