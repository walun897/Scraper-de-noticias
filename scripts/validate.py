#!/usr/bin/env python3
import pandas as pd, os, glob, sys
from datetime import date
from tabulate import tabulate

TARGET_RATIO = {"falso":0.4,"verdadero":0.4,"dudoso":0.2}
TOL = 0.01  # ±1%

def main(indir="data", outdir="data/reportes"):
    os.makedirs(outdir, exist_ok=True)
    today = str(date.today())
    daily_files = sorted(glob.glob(os.path.join(indir, f"dataset_{today}.csv")))
    hist_file = os.path.join(indir, "dataset_historico.csv")

    lines = [f"# Reporte {today}\n"]

    def section(t): lines.append(f"\n## {t}\n")
    def ratios_str(vc):
        total = vc.sum() if hasattr(vc, "sum") else 0
        if total==0: return "N/A"
        parts=[]
        for k in ["falso","verdadero","dudoso"]:
            r = (vc.get(k,0)/total) if total else 0
            ok = abs(r - TARGET_RATIO.get(k,0)) <= TOL
            tag = "✅" if ok else "⚠️"
            parts.append(f"{k}: {r:.2%} (objetivo {TARGET_RATIO.get(k,0):.0%}) {tag}")
        return " | ".join(parts)

    if daily_files:
        df = pd.read_csv(daily_files[-1])
        section("Diario")
        lines.append(f"- Filas: **{len(df)}**")
        lines.append(f"- % con fecha: **{round((df['fecha'].fillna('').str.len()>0).mean()*100,2)}%**")
        lines.append("\n**Por clase**\n")
        lines.append("```\n"+tabulate(df["estado"].value_counts().reset_index().values, headers=["estado","conteo"])+"\n```")
        lines.append(f"\n**Ratios vs objetivo (40/40/20)**\n\n{ratios_str(df['estado'].value_counts())}\n")
        lines.append("\n**Top fuentes**\n")
        lines.append("```\n"+tabulate(df["fuente"].value_counts().head(10).reset_index().values, headers=["fuente","conteo"])+"\n```")
    else:
        lines.append("\nNo se encontró CSV diario.\n")

    if os.path.exists(hist_file):
        hf = pd.read_csv(hist_file)
        section("Histórico")
        lines.append(f"- Filas: **{len(hf)}**")
        lines.append(f"- % con fecha: **{round((hf['fecha'].fillna('').str.len()>0).mean()*100,2)}%**")
        lines.append("\n**Por clase**\n")
        lines.append("```\n"+tabulate(hf["estado"].value_counts().reset_index().values, headers=["estado","conteo"])+"\n```")
        lines.append("\n**Top fuentes**\n")
        lines.append("```\n"+tabulate(hf["fuente"].value_counts().head(10).reset_index().values, headers=["fuente","conteo"])+"\n```")

    md = "\n".join(lines)
    out_md = os.path.join(outdir, f"reporte_{today}.md")
    with open(out_md, "w", encoding="utf-8") as f:
        f.write(md)
    print("Reporte generado:", out_md)

if __name__=="__main__":
    indir  = sys.argv[1] if len(sys.argv)>1 else "data"
    outdir = sys.argv[2] if len(sys.argv)>2 else "data/reportes"
    main(indir, outdir)
