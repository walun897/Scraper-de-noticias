#!/usr/bin/env python3
import pandas as pd, glob, sys, os

def main(indir="data", out="data/dataset_historico.csv"):
    files = sorted(glob.glob(os.path.join(indir, "dataset_*.csv")))
    if not files:
        print("No hay archivos dataset_*.csv en", indir); return
    dfs = []
    for f in files:
        try:
            dfs.append(pd.read_csv(f))
        except Exception as e:
            print(f"[WARN] {f}: {e}")
    df = pd.concat(dfs, ignore_index=True)
    # dedup por URL, conservando la fila más reciente con fecha no vacía
    df["_fecha_ok"] = df["fecha"].fillna("").str.len()>0
    df = df.sort_values(by=["_fecha_ok","fecha"], ascending=[False, False]).drop_duplicates(subset=["url"], keep="first")
    df = df.drop(columns=["_fecha_ok"], errors="ignore")
    df.to_csv(out, index=False, encoding="utf-8")
    print("Consolidado:", out, "| filas:", len(df))

if __name__=="__main__":
    indir = sys.argv[1] if len(sys.argv)>1 else "data"
    out   = sys.argv[2] if len(sys.argv)>2 else "data/dataset_historico.csv"
    main(indir, out)
