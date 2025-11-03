#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ESKLP updater (ZIP bulk) — v5

Что умеет:
- Если задан ESKLP_BULK_URL: скачивает ZIP, извлекает все файлы в память,
  автоматически классифицирует таблицы на:
    • products (ТН/форма/упаковка/страна/ATC/ссылки)
    • compositions (product_id/INN/доза/ед.)
    • prices (ЖНВЛП: product_id/ТН/цена/дата)
- Поддерживает форматы внутри ZIP: CSV, XLSX, JSON.
- Если BULK не задан: fallback на ESKLP_PRODUCTS_URL / ESKLP_COMPOSITIONS_URL / ESKLP_PRICES_URL (JSON/CSV).
- Нормализация по словарям из dictionaries/.
- Мэппинг цен: product_id → (бренд+форма) → бренд.
- Дедуп цен: самая свежая по product_id.
- Экспорт ATC-иерархии.
- Обновляет data/version.txt.

Выход:
  data/products.csv, data/ingredients.csv, data/prices.csv, data/atc.csv, data/version.txt
"""

import os, re, io, sys, json, zipfile, hashlib
from datetime import datetime
import urllib.request
import pandas as pd

DATA_DIR = "data"
DICT_DIR = "dictionaries"

OUT_PRODUCTS = os.path.join(DATA_DIR, "products.csv")
OUT_INGRS    = os.path.join(DATA_DIR, "ingredients.csv")
OUT_PRICES   = os.path.join(DATA_DIR, "prices.csv")
OUT_ATC      = os.path.join(DATA_DIR, "atc.csv")
OUT_VERSION  = os.path.join(DATA_DIR, "version.txt")

# ENV
U_BULK  = os.getenv("ESKLP_BULK_URL", "").strip()
U_PROD  = os.getenv("ESKLP_PRODUCTS_URL", "").strip()
U_COMP  = os.getenv("ESKLP_COMPOSITIONS_URL", "").strip()
U_PRICE = os.getenv("ESKLP_PRICES_URL", "").strip()

# ---------- utils ----------
def norm(s): return re.sub(r"\s+"," ", (str(s) if s is not None else "").strip())
def low (s): return norm(s).lower()

def http_get(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=300) as r:
        return r.read()

def load_dict_csv(path, to_lower=True):
    if not os.path.exists(path): return {}
    df = pd.read_csv(path)
    out = {}
    for _,r in df.iterrows():
        src = str(r.get("src","")).strip()
        can = str(r.get("canon","")).strip()
        if not src or not can: continue
        out[(src.lower() if to_lower else src)] = can
    return out

def syn(x, mp, to_lower=True):
    if x is None: return x
    k = x.lower() if to_lower else x
    return mp.get(k, x)

def synthetic_id(*parts):
    raw = "||".join(str(p) for p in parts)
    return "pid_" + hashlib.md5(raw.encode("utf-8")).hexdigest()[:16]

def split_atc(atc: str):
    code = (atc or "").strip().upper()
    return {
        "atc_code": code,
        "level1": code[:1] if len(code)>=1 else "",
        "level2": code[:3] if len(code)>=3 else "",
        "level3": code[:4] if len(code)>=4 else "",
        "level4": code[:5] if len(code)>=5 else "",
        "level5": code if len(code)>=7 else "",
    }

# ---------- flexible tabular readers ----------
def read_table_from_bytes(name: str, data: bytes) -> pd.DataFrame:
    n = name.lower()
    # try JSON
    try:
        if n.endswith(".json"):
            obj = json.loads(data.decode("utf-8", errors="ignore"))
            if isinstance(obj, dict) and "data" in obj: obj = obj["data"]
            return pd.json_normalize(obj)
    except Exception:
        pass
    # try CSV (utf-8; fallback cp1251/semicolon)
    try:
        text = data.decode("utf-8", errors="ignore")
        try:
            return pd.read_csv(io.StringIO(text))
        except Exception:
            return pd.read_csv(io.StringIO(text), sep=";")
    except Exception:
        pass
    # try XLSX
    if n.endswith((".xlsx",".xls")):
        return pd.read_excel(io.BytesIO(data))
    return pd.DataFrame()

def load_table_url(url: str) -> pd.DataFrame:
    if not url: return pd.DataFrame()
    raw = http_get(url)
    # try JSON
    try:
        arr = json.loads(raw.decode("utf-8", errors="ignore"))
        if isinstance(arr, dict) and "data" in arr: arr = arr["data"]
        return pd.json_normalize(arr)
    except Exception:
        pass
    # try CSV
    try:
        text = raw.decode("utf-8", errors="ignore")
        try:
            return pd.read_csv(io.StringIO(text))
        except Exception:
            return pd.read_csv(io.StringIO(text), sep=";")
    except Exception:
        pass
    # try XLSX
    try:
        return pd.read_excel(io.BytesIO(raw))
    except Exception:
        return pd.DataFrame()

# ---------- classification heuristics ----------
def classify_table(df: pd.DataFrame, filename: str) -> str | None:
    cols = set([c.lower() for c in df.columns])
    name = filename.lower()

    # compositions
    if {"product_id","inn"}.issubset(cols): return "compositions"
    if ("product_id" in cols and ("substance" in cols or "ingredient" in cols)): return "compositions"

    # prices
    if ("price_rub" in cols or "price" in cols or "znvlp_price" in cols):
        if "trade_name" in cols or "brand" in cols or "product_id" in cols:
            return "prices"
    if any(k in name for k in ["znvlp","price","цена","жнвлп"]): return "prices"

    # products
    prod_keys = {"trade_name","dosage_form","pack","country"} & cols
    if len(prod_keys) >= 2: return "products"
    if any(k in name for k in ["prod","product","tn","trade","brand","реестр","лекарств"]):
        return "products"

    return None

# ---------- load from BULK zip ----------
def load_from_bulk(url: str):
    raw = http_get(url)
    products, comps, prices = [], [], []

    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
        for info in zf.infolist():
            if info.is_dir(): continue
            fname = info.filename
            data = zf.read(info)
            df = read_table_from_bytes(fname, data)
            if df.empty: continue
            kind = classify_table(df, fname)
            if kind == "products":
                products.append(df)
            elif kind == "compositions":
                comps.append(df)
            elif kind == "prices":
                prices.append(df)

    dfp = pd.concat(products, ignore_index=True) if products else pd.DataFrame()
    dfi = pd.concat(comps, ignore_index=True) if comps else pd.DataFrame()
    dfz = pd.concat(prices, ignore_index=True) if prices else pd.DataFrame()
    return dfp, dfi, dfz

# ---------- main ----------
def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    # dictionaries
    d_inn   = load_dict_csv(os.path.join(DICT_DIR,"inn_synonyms.csv"), True)
    d_brand = load_dict_csv(os.path.join(DICT_DIR,"brand_synonyms.csv"), False)
    d_form  = load_dict_csv(os.path.join(DICT_DIR,"form_normalization.csv"), True)
    d_ctry  = load_dict_csv(os.path.join(DICT_DIR,"country_normalization.csv"), True)

    # load raw
    if U_BULK:
        print(f"→ BULK ZIP: {U_BULK}")
        dfp_raw, dfi_raw, dfz_raw = load_from_bulk(U_BULK)
    else:
        dfp_raw = load_table_url(U_PROD)
        dfi_raw = load_table_url(U_COMP)
        dfz_raw = load_table_url(U_PRICE)

    # ---------- PRODUCTS transform ----------
    prows=[]
    for _,r in (dfp_raw if not dfp_raw.empty else pd.DataFrame()).iterrows():
        pid = str(r.get("product_id") or r.get("id") or "").strip()
        tn  = syn(norm(r.get("trade_name") or r.get("brand") or r.get("tn") or ""), d_brand, to_lower=False)
        frm = syn(norm(r.get("dosage_form") or r.get("form") or ""), d_form, True)
        pack= norm(r.get("pack") or r.get("package") or r.get("packaging") or "")
        ctr = syn(low (r.get("country") or r.get("origin_country") or ""), d_ctry, True)
        atc = norm(r.get("atc_code") or r.get("atc") or "").upper()
        reg = norm(r.get("ru_registry_url") or r.get("registry_url") or r.get("grls") or "")
        ins = norm(r.get("instruction_url") or r.get("leaflet_url") or "")
        if not pid:
            pid = synthetic_id(tn, frm, pack)
        prows.append({
            "product_id": pid, "trade_name": tn, "dosage_form": frm, "pack": pack,
            "country": ctr, "is_znvlp": False, "atc_code": atc,
            "ru_registry_url": reg, "instruction_url": ins
        })
    dfP = pd.DataFrame(prows).drop_duplicates() if prows else pd.DataFrame()
    if dfP.empty:
        print("ERROR: products empty (BULK not parsed?)", file=sys.stderr); sys.exit(1)

    # ---------- COMPOSITIONS transform ----------
    irows=[]
    for _,r in (dfi_raw if not dfi_raw.empty else pd.DataFrame()).iterrows():
        pid = str(r.get("product_id") or r.get("id") or "").strip()
        inn = syn(low(r.get("inn") or r.get("ingredient") or r.get("substance") or ""), d_inn, True)
        strength = str(r.get("strength") or r.get("dose") or "").replace(",", ".")
        unit = low(r.get("unit") or r.get("uom") or "")
        if pid and inn:
            irows.append({"product_id":pid,"inn":inn,"strength":strength,"unit":unit})
    dfI = pd.DataFrame(irows).drop_duplicates(subset=["product_id","inn","strength","unit"]) if irows else pd.DataFrame()

    # ---------- PRICES transform ----------
    zrows=[]
    for _,r in (dfz_raw if not dfz_raw.empty else pd.DataFrame()).iterrows():
        price = r.get("price_rub", r.get("price", r.get("znvlp_price", None)))
        if price is None: continue
        try: price = float(str(price).replace(",", ".")); 
        except Exception: continue
        pid = str(r.get("product_id") or r.get("id") or "").strip()
        tn  = low(r.get("trade_name") or r.get("brand") or r.get("tn") or "")
        date= norm(r.get("date") or r.get("price_date") or r.get("updated_at") or datetime.utcnow().date().isoformat())
        zrows.append({"product_id":pid,"trade_name":tn,"price":price,"date":date})
    dfZ_raw = pd.DataFrame(zrows)

    # ---------- match prices to products ----------
    pid_set = set(dfP["product_id"].astype(str))
    idx_brand, idx_bf = {}, {}
    for _,r in dfP.iterrows():
        b = low(r["trade_name"]); f = low(r["dosage_form"]); pid = str(r["product_id"])
        idx_brand.setdefault(b,set()).add(pid)
        idx_bf.setdefault((b,f),set()).add(pid)

    zout=[]
    if not dfZ_raw.empty:
        for _,r in dfZ_raw.iterrows():
            pids=set()
            if r["product_id"] and r["product_id"] in pid_set:
                pids.add(r["product_id"])
            if not pids and r["trade_name"]:
                for (b,f), ids in idx_bf.items():
                    if b == r["trade_name"]:
                        pids |= ids
            if not pids and r["trade_name"]:
                pids |= idx_brand.get(r["trade_name"], set())
            for pid in pids:
                zout.append({"product_id":pid,"znvlp_price_rub":r["price"],"price_date":r["date"]})
    dfZ = pd.DataFrame(zout)
    if not dfZ.empty:
        dfZ["price_date"] = pd.to_datetime(dfZ["price_date"], errors="coerce")
        dfZ = dfZ.sort_values(["product_id","price_date"]).dropna(subset=["price_date"])
        dfZ = dfZ.groupby("product_id", as_index=False).tail(1)

    # флаг ЖНВЛП
    if not dfZ.empty:
        zn = set(dfZ["product_id"].astype(str))
        dfP["is_znvlp"] = dfP["product_id"].astype(str).isin(zn)

    # ATC-иерархия
    atc_rows=[]
    for _,r in dfP.iterrows():
        info = split_atc(r.get("atc_code",""))
        info["product_id"] = r["product_id"]
        atc_rows.append(info)
    dfA = pd.DataFrame(atc_rows).drop_duplicates()

    # финал: сортировки и запись
    dfP = dfP.sort_values(["trade_name","dosage_form","pack"]).reset_index(drop=True)
    if not dfI.empty: dfI = dfI.sort_values(["product_id","inn"]).reset_index(drop=True)
    if not dfZ.empty: dfZ = dfZ.sort_values(["product_id"]).reset_index(drop=True)
    if not dfA.empty: dfA = dfA.sort_values(["product_id"]).reset_index(drop=True)

    dfP.to_csv(OUT_PRODUCTS, index=False)
    dfI.to_csv(OUT_INGRS, index=False)
    dfZ.to_csv(OUT_PRICES, index=False)
    dfA.to_csv(OUT_ATC, index=False)
    with open(OUT_VERSION,"w",encoding="utf-8") as f:
        f.write(datetime.utcnow().isoformat())

    print(f"OK bulk: products={len(dfP)}, ingrs={len(dfI)}, prices={len(dfZ)}, atc_rows={len(dfA)}")

if __name__ == "__main__":
    sys.exit(main())
