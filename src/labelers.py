from __future__ import annotations
from typing import Optional
TRUE,FALSE,DOUBT="true","false","doubtful"

def pick(txt, pos, neg, dub)->Optional[str]:
    r=txt.lower()
    if any(w in r for w in pos): return TRUE
    if any(w in r for w in neg): return FALSE
    if any(w in r for w in dub): return DOUBT
    return None

def norm_from_colombiacheck(x): return pick(x,["verdadero"],["falso","engañoso"],["inchequeable","dudoso"])
def norm_from_afp(x):           return pick(x,["cierto","verdadero"],["falso","engañoso"],["dudoso","no verificable"])
def norm_from_maldita(x):       return pick(x,["es cierto","verdadero"],["bulo","falso"],["sin evidencias","dudoso"])
def norm_from_newtral(x):       return pick(x,["verdadero","verdadera"],["falso","bulo","engañoso"],["no verificable","dudoso"])
def norm_from_chequeado(x):     return pick(x,["verdadero"],["falso","engañoso"],["insustancial","dudoso"])
def norm_from_efe(x):           return pick(x,["verdadero"],["falso","engañoso"],["no verificable","dudoso"])
def norm_from_verificat(x):     return pick(x,["cierto","verdadero"],["falso","enganyos"],["dubtós","dudoso"])
def norm_from_sabueso(x):       return pick(x,["verdadero"],["falso","engañoso"],["dudoso"])
def norm_from_verificadomx(x):  return pick(x,["verdadero"],["falso","bulo"],["dudoso"])
def norm_from_bolivia(x):       return pick(x,["verdadero"],["falso"],["dudoso"])
def norm_from_ecuador(x):       return pick(x,["verdadero"],["falso"],["dudoso"])
def norm_from_factchequeado(x): return pick(x,["verdadero"],["falso"],["dudoso"])

LABELERS = {
 "colombiacheck":norm_from_colombiacheck, "afp":norm_from_afp, "maldita":norm_from_maldita,
 "newtral":norm_from_newtral, "chequeado":norm_from_chequeado, "efe":norm_from_efe,
 "verificat":norm_from_verificat, "sabueso":norm_from_sabueso, "verificadomx":norm_from_verificadomx,
 "bolivia":norm_from_bolivia, "ecuador":norm_from_ecuador, "factchequeado":norm_from_factchequeado
}
