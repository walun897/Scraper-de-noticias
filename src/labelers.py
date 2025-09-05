from __future__ import annotations
from typing import Optional

TRUE, FALSE, DOUBT = "true","false","doubtful"

def _pick(txt: str, pos, neg, dub) -> Optional[str]:
    r = (txt or "").lower()
    if any(w in r for w in pos): return TRUE
    if any(w in r for w in neg): return FALSE
    if any(w in r for w in dub): return DOUBT
    return None

# AFP suele meter la “categoría” o pistas en título/desc
def norm_from_afp(x): return _pick(x, ["cierto","verdadero"], ["falso","engañoso","bulo"], ["dudoso","no verificable"])

LABELERS = {
    "afp": norm_from_afp,
}
