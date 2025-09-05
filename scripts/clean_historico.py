#!/usr/bin/env python3
import pandas as pd, sys, re, unicodedata, html
from ftfy import fix_text

BAD_TITLE_PATTERNS = [
    r"^Maldito Bulo/Maldita\.es.*$",
    r"^Maldita\.es.*$",
    r"^Newtral.*$",
]

def sanitize_text(s):
    if s is None: return ""
    s = html.unescape(str(s))
    try: s = fix_text(s)
    except Exception: pass
    s = unicodedata.normalize("NFC", s)
    s = "".join(ch for ch in s if ch == "\n" or ch == "\t" or ord(ch) >= 32)
    s = re.sub(r"[ \t]+\n", "\n", s)
    s = re.sub(r"[ \t]{2,}", " ", s).strip()
    return s

def main(inp="data/dataset_historico.csv", out="data/dataset_historico_clean.csv"):
    df = pd.read_csv(inp)
    # sanitiza
    for col in ["titulo","texto","fuente","estado","url","url_canonica"]:
        if col in df.columns:
            df[col] = df[col].map(sanitize_text)
    # quita títulos genéricos evidentes y muy cortos
    pat = re.compile("|".join(BAD_TITLE_PATTERNS), flags=re.IGNORECASE)
    df = df[~df["titulo"].fillna("").str.match(pat)]
    df = df[df["titulo"].fillna("").str.len() >= 25].copy()
    # dedup fuerte
    key = "url_hash" if ("url_hash" in df.columns and df["url_hash"].notna().any()) else ("url_canonica" if "url_canonica" in df.columns else "url")
    df["_len_texto"] = df["texto"].fillna("").str.len()
    df["_fecha_ok"] = df["fecha"].fillna("").str.len()>0
    df = df.sort_values(by=["_fecha_ok","_len_texto","fecha"], ascending=[False,False,False]) \
           .drop_duplicates(subset=[key], keep="first") \
           .drop(columns=["_len_texto","_fecha_ok"], errors="ignore")
    df.to_csv(out, index=False, encoding="utf-8")
    print("Histórico limpio guardado en:", out, " | filas:", len(df))

if __name__=="__main__":
    inp = sys.argv[1] if len(sys.argv)>1 else "data/dataset_historico.csv"
    out = sys.argv[2] if len(sys.argv)>2 else "data/dataset_historico_clean.csv"
    main(inp, out)
