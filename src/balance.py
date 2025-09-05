import math, pandas as pd
from .config import TARGET_RATIOS

def stratified_balance(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df[df["label"].isin(["true","false","doubtful"])].reset_index(drop=True)
    if df.empty: return df
    n = len(df)
    targets = {k:int(math.floor(n*r)) for k,r in TARGET_RATIOS.items()}
    slices = {}
    for lab, t in targets.items():
        pool = df[df["label"]==lab]
        slices[lab] = pool if len(pool)<=t else pool.sample(n=t,random_state=42)
    allocated = sum(len(s) for s in slices.values())
    deficit = n - allocated
    if deficit>0:
        used_idx = pd.concat(slices.values()).index if slices else pd.Index([])
        remain = df.drop(index=used_idx)
        add = remain.sample(n=min(deficit,len(remain)), random_state=42) if not remain.empty else pd.DataFrame(columns=df.columns)
        df_final = pd.concat([pd.concat(list(slices.values())), add], ignore_index=True)
    else:
        df_final = pd.concat(list(slices.values()), ignore_index=True)
    return df_final.sample(frac=1.0, random_state=42).reset_index(drop=True)

