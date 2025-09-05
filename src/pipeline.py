# src/pipeline.py
from __future__ import annotations
from pathlib import Path
from datetime import datetime
from typing import List, Dict

import pandas as pd

from .config import STD_FIELDS, MAX_TITLE_LEN, MAX_SUMMARY_LEN
from .labelers import LABELERS
from .scrapers import scrape_html_cards, scrape_rss
from .utils import logger, safe_trim

# ---- Carpetas robustas ----
DATA_ROOT = Path("data")
DATA_DAILY_DIR = DATA_ROOT / "daily"
DATA_MASTER_DIR = DATA_ROOT / "master"
FC_MASTER = DATA_MASTER_DIR / "factcheck_master.csv"
NW_MASTER = DATA_MASTER_DIR / "news_master.csv"
LEGACY_MASTER = DATA_MASTER_DIR / "master.csv"

def ensure_dir(p: Path):
    if p.exists() and not p.is_dir():
        p.unlink()
    p.mkdir(parents=True, exist_ok=True)

for p in (DATA_DAILY_DIR, DATA_MASTER_DIR):
    ensure_dir(p)

# ---- Selectores HTML (añade/ajusta aquí) ----
HTML_SELECTORS = {
    # FACT-CHECKERS
    "Colombiacheck": {"item":"div.views-row","title":"h2 a","url":"h2 a","summary":".field-content .text p","date":"time","label":".field-name-field-calificacion .field-item"},
    "Maldita.es": {"item":"article","title":"h2 a, h3 a","url":"h2 a, h3 a","summary":"p","date":"time, .date","label":".badge, .tag, .c-label"},
    "Newtral": {"item":"article","title":"h2 a, h3 a","url":"h2 a, h3 a","summary":"p","date":"time","label":".etiqueta, .tag"},
    "Chequeado": {"item":"article","title":"h2 a, h3 a","url":"h2 a, h3 a","summary":"p","date":"time","label":".badge, .etiqueta"},
    "EFE Verifica": {"item":"article","title":"h2 a, h3 a","url":"h2 a, h3 a","summary":"p","date":"time","label":".etiqueta, .cat"},
    "Verificat": {"item":"article","title":"h2 a, h3 a","url":"h2 a, h3 a","summary":"p","date":"time","label":".label, .tag"},
    "El Sabueso (Animal Político)": {"item":"article","title":"h2 a, h3 a","url":"h2 a, h3 a","summary":"p","date":"time","label":".etiqueta, .tag"},
    "VerificadoMX": {"item":"article","title":"h2 a, h3 a","url":"h2 a, h3 a","summary":"p","date":"time","label":".etiqueta, .tag"},
    "Bolivia Verifica": {"item":"article","title":"h2 a, h3 a","url":"h2 a, h3 a","summary":"p","date":"time","label":".etiqueta, .tag"},
    "Ecuador Chequea": {"item":"article","title":"h2 a, h3 a","url":"h2 a, h3 a","summary":"p","date":"time","label":".etiqueta, .tag"},
    "Factchequeado": {"item":"article","title":"h2 a, h3 a","url":"h2 a, h3 a","summary":"p","date":"time","label":".etiqueta, .tag"},

    # NEWS (sin etiqueta)
    "El Tiempo": {"item":"article, li","title":"a","url":"a","summary":"p, .lead","date":"time"},
    "El Espectador": {"item":"article, li","title":"a","url":"a","summary":"p, .lead","date":"time"},
    "La Silla Vacía": {"item":"article, li","title":"a","url":"a","summary":"p, .lead","date":"time"},
    "El Universal MX": {"item":"article, li","title":"a","url":"a","summary":"p, .lead","date":"time"},
    "El Comercio PE": {"item":"article, li","title":"a","url":"a","summary":"p, .lead","date":"time"},
}

def _normalize_row(source_name: str, row: Dict, label_source: str | None) -> Dict:
    label_norm = ""
    if label_source:
        lab_fn = LABELERS.get(label_source)
        if lab_fn:
            label_norm = lab_fn(row.get("label_raw","") or "") or ""
    out = {
        "source": source_name,
        "title": safe_trim(row.get("title",""), MAX_TITLE_LEN),
        "summary": safe_trim(row.get("summary",""), MAX_SUMMARY_LEN),
        "url": row.get("url",""),
        "published_at": row.get("published_at",""),
        "label_raw": row.get("label_raw",""),
        "label": label_norm,
    }
    return out if out["title"] and out["url"] else {}

def _run_block(sources: list[dict]) -> pd.DataFrame:
    rows: List[Dict] = []
    for s in sources:
        name, t, url = s["name"], s["type"], s["url"]
        label_src = s.get("label_source")
        logger.info(f"Scraping {name} -> {url}")

        if t == "rss":
            data = scrape_rss(url)
        elif t == "html":
            selectors = HTML_SELECTORS.get(name)
            if not selectors:
                logger.warning(f"No selectors for {name}; skipping")
                continue
            data = scrape_html_cards(url, selectors)
        else:
            logger.warning(f"Unknown type {t} for {name}; skipping")
            continue

        for r in data:
            norm = _normalize_row(name, r, label_src)
            if norm:
                rows.append(norm)

    if not rows:
        return pd.DataFrame(columns=STD_FIELDS)
    df = pd.DataFrame(rows)[STD_FIELDS].drop_duplicates("url").reset_index(drop=True)
    return df

def _has_dual_sets():
    try:
        from .config import FACTCHECK_SOURCES, NEWS_SOURCES  # type: ignore
        return bool(FACTCHECK_SOURCES) and bool(NEWS_SOURCES)
    except Exception:
        return False

def run_all():
    if _has_dual_sets():
        from .config import FACTCHECK_SOURCES, NEWS_SOURCES  # type: ignore
        df_fc = _run_block(FACTCHECK_SOURCES)
        df_fc = df_fc[df_fc["label"].isin(["true","false","doubtful"])].reset_index(drop=True)
        df_nw = _run_block(NEWS_SOURCES)
        return df_fc, df_nw
    else:
        # Modo legado (solo SOURCES)
        from .config import SOURCES  # type: ignore
        df = _run_block(SOURCES)
        df = df[df["label"].isin(["true","false","doubtful"])].reset_index(drop=True)
        return df, pd.DataFrame(columns=STD_FIELDS)

def _save(df: pd.DataFrame, path: Path):
    df.to_csv(path, index=False, encoding="utf-8")
    logger.info(f"Saved {path} ({len(df)} rows)")

def save_outputs(df_fc: pd.DataFrame, df_nw: pd.DataFrame):
    today = datetime.now().date().isoformat()

    if not df_nw.empty or not df_fc.empty:
        # dual mode
        fc_path = DATA_DAILY_DIR / f"factcheck_{today}.csv"
        nw_path = DATA_DAILY_DIR / f"news_{today}.csv"
        _save(df_fc, fc_path)
        _save(df_nw, nw_path)

        if FC_MASTER.exists():
            pd.concat([pd.read_csv(FC_MASTER), df_fc], ignore_index=True)\
              .drop_duplicates("url").to_csv(FC_MASTER, index=False, encoding="utf-8")
        else:
            df_fc.to_csv(FC_MASTER, index=False, encoding="utf-8")

        if NW_MASTER.exists():
            pd.concat([pd.read_csv(NW_MASTER), df_nw], ignore_index=True)\
              .drop_duplicates("url").to_csv(NW_MASTER, index=False, encoding="utf-8")
        else:
            df_nw.to_csv(NW_MASTER, index=False, encoding="utf-8")
    else:
        # legacy mode fallback
        path = DATA_DAILY_DIR / f"{today}.csv"
        _save(df_fc, path)
        if LEGACY_MASTER.exists():
            pd.concat([pd.read_csv(LEGACY_MASTER), df_fc], ignore_index=True)\
              .drop_duplicates("url").to_csv(LEGACY_MASTER, index=False, encoding="utf-8")
        else:
            df_fc.to_csv(LEGACY_MASTER, index=False, encoding="utf-8")

if __name__ == "__main__":
    from .balance import stratified_balance
    df_fc, df_nw = run_all()
    if not df_fc.empty:
        df_fc = stratified_balance(df_fc)
    save_outputs(df_fc, df_nw)
    logger.info(f"Done. factcheck={len(df_fc)} news={len(df_nw)}")
