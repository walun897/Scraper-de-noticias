#!/usr/bin/env python3
import pandas as pd, os, glob, sys
from datetime import date

def main(indir="data", outdir="data/reportes"):
    os.makedirs(outdir, exist_ok=True)
    today = str(date.today())
    daily_files = sorted(glob.glob(os.path.join(indir, f"dataset_{today}.csv")))
    hist_file = os.path.join(indir, "dataset_historico.csv")

    lines = [f"# Reporte {today}\n"]

    def section(title): lines.append(f"\n## {title}\n")

    if daily_files:
        df = pd.read_csv(daily_files[-1])
        section("Diario")
        lines.append(f"- Filas: **{len(df)}**")
        lines.append(f"- % con fecha: **{round((df['fecha'].fillna('').str.len()>0).mean()*100,2)}%**")
        lines.append("\n**Por clase**:\n")
        lines.append(df["estado"].value_counts().to_markdown())
        lines.append("\n**Top fuentes**:\n")
        lines.append(df["fuente"].value_counts().head(10).to_markdown())
    else:
        lines.append("\nNo se encontró CSV diario.\n")

    if os.path.exists(hist_file):
        hf = pd.read_csv(hist_file)
        section("Histórico")
        lines.append(f"- Filas: **{len(hf)}**")
        lines.append(f"- % con fecha: **{round((hf['fecha'].fillna('').str.len()>0).mean()*100,2)}%**")
        lines.append("\n**Por clase**:\n")
        lines.append(hf["estado"].value_counts().to_markdown())
        lines.append("\n**Top fuentes**:\n")
        lines.append(hf["fuente"].value_counts().head(10).to_markdown())

    md = "\n".join(lines)
    out_md = os.path.join(outdir, f"reporte_{today}.md")
    with open(out_md, "w", encoding="utf-8") as f:
        f.write(md)
    print("Reporte generado:", out_md)

if __name__=="__main__":
    indir  = sys.argv[1] if len(sys.argv)>1 else "data"
    outdir = sys.argv[2] if len(sys.argv)>2 else "data/reportes"
    main(indir, outdir)
