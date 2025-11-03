#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ESKLP обновлялка для Help.Drugix (расширенная)
- ONLINE: читает JSON из ENV-URL (ESKLP_*_URL)
- OFFLINE: data/source/esklp_*.json
- Нормализация по словарям из dictionaries/
- Дедупликация, мэппинг ЖНВЛП по product_id и бренду
На выходе: data/products.csv, data/ingredients.csv, data/prices.csv, data/version.txt
"""

import os, re, json, sys, urllib.request, hashlib, time
from datetime import datetime
import pandas as pd

DATA_DIR = "data"
SRC_DIR  = os.path.join(DATA_DIR, "source")
DICT_DIR = "dictionaries"

OUT_PRODUCTS = os.path.join(DATA_DIR, "products.csv")
OUT_INGRS    = os.path.join(DATA_DIR, "ingredients.csv")
OUT_PRICES   = os.path.join(DATA_DIR, "prices.csv")
OUT_VERSION  = os.path.join(DATA_DIR, "version.txt")

ESKLP_PRODUCTS_URL     = os.getenv("ESKLP_PRODUCTS_URL", "").strip()
ESKLP_COMPOSITIONS_URL = os.getenv("ESKLP_COMPOSITIONS_URL", "").strip()
ESKLP_PRICES_URL       = os.getenv("ESKLP_PRICES_URL", "").strip()

def normspace(s:str) -> str:
    return re.sub(r"\s+"," ",(s or "").strip())

def lower(s:str) -> str:
    return normspace(s).lower()

def load_dict_csv(path, to_lower=True):
    if not os.path.exists(path): return {}
    df = pd.read_csv(path)
    m = {}
    for _,r in df.iterrows():
        src = str(r.get("src","")).strip()
        canon = str(r.get("canon","")).strip()
        if not src or not canon: continue
        m[(src.lower() if to_lower else src)] = canon
    return m

def repl_syn(s, mapping, to_lower=True):
    if not s: return s
    key = s.lower() if to_lower else s
    return mapping.get(key, s)

def read_url_json(url):
    if not url: return None
    try:
        with urllib.request.urlopen(url, timeout=120) as resp:
            return json.loads(resp.read().decode("utf-8", errors="ignore"))
    except Exception as e:
        print(f"ONLINE FAIL {url}: {e}", file=sys.stderr)
        return None

def read_file_json(path):
    try:
        with open(path,"r",encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []

def synthetic_id(*parts):
    raw = "||".join(str(p) for p in parts)
    return "pid_" + hashlib.md5(raw.encode("utf-8")).hexdigest()[:16]

def load_products(dict_forms, dict_countries, dict_brand):
    data = read_url_json(ESKLP_PRODUCTS_URL)
    if data is None:
        data = read_file_json(os.path.join(SRC_DIR,"esklp_products.json"))
        print("→ OFFLINE products")
    rows = []
    for i, r in enumerate(data or []):
        pid = r.get("id") or synthetic_id(r.get("trade_name"), r.get("dosage_form"), r.get("pack"))
        tn_raw = normspace(r.get("trade_name"))
        tn = repl_syn(tn_raw, dict_brand, to_lower=False)
        form = repl_syn(normspace(r.get("dosage_form")), dict_forms, to_lower=True)
        country = repl_syn(lower(r.get("country")), dict_countries, to_lower=True)
        rows.append({
            "product_id":      str(pid),
            "trade_name":      tn,
            "dosage_form":     form,
            "pack":            normspace(r.get("pack")),
            "country":         country,
            "is_znvlp":        False,
            "atc_code":        normspace(r.get("atc")).upper(),
            "ru_registry_url": r.get("registry_url") or "",
            "instruction_url": r.get("instruction_url") or "",
        })
    df = pd.DataFrame(rows).drop_duplicates()
    return df

def load_compositions(dict_inn):
    data = read_url_json(ESKLP_COMPOSITIONS_URL)
    if data is None:
        data = read_file_json(os.path.join(SRC_DIR,"esklp_compositions.json"))
        print("→ OFFLINE compositions")
    rows = []
    for r in (data or []):
        pid = str(r.get("product_id") or "")
        inn_raw = lower(r.get("inn"))
        inn = repl_syn(inn_raw, dict_inn, to_lower=True)
        strength = str(r.get("strength") or "").replace(",",".")
        unit = lower(r.get("unit"))
        if pid and inn:
            rows.append({"product_id":pid,"inn":inn,"strength":strength,"unit":unit})
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.drop_duplicates(subset=["product_id","inn","strength","unit"])
    return df

def load_prices(dict_brand):
    data = read_url_json(ESKLP_PRICES_URL)
    if data is None:
        data = read_file_json(os.path.join(SRC_DIR,"esklp_prices.json"))
        print("→ OFFLINE prices")
    rows = []
    for r in (data or []):
        price = r.get("price_rub")
        if price is None: continue
        try:
            price = float(str(price).replace(",", "."))
        except Exception:
            continue
        tn_raw = normspace(r.get("trade_name"))
        tn = repl_syn(tn_raw, dict_brand, to_lower=False)
        rows.append({
            "product_id": str(r.get("product_id") or ""),
            "trade_name": lower(tn),
            "price": price,
            "date": normspace(r.get("date")) or datetime.utcnow().date().isoformat()
        })
    return pd.DataFrame(rows)

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(SRC_DIR, exist_ok=True)

    # словари
    dict_inn       = load_dict_csv(os.path.join(DICT_DIR,"inn_synonyms.csv"), True)
    dict_brand     = load_dict_csv(os.path.join(DICT_DIR,"brand_synonyms.csv"), False)
    dict_forms     = load_dict_csv(os.path.join(DICT_DIR,"form_normalization.csv"), True)
    dict_countries = load_dict_csv(os.path.join(DICT_DIR,"country_normalization.csv"), True)

    # загрузка
    df_p = load_products(dict_forms, dict_countries, dict_brand)
    df_i = load_compositions(dict_inn)
    df_z = load_prices(dict_brand)

    if df_p.empty:
        print("ERROR: products empty", file=sys.stderr)
        sys.exit(1)

    # индексы для мэппинга цен
    tn_index = {}
    for _, r in df_p.iterrows():
        tn_key = lower(r["trade_name"])
        tn_index.setdefault(tn_key, set()).add(str(r["product_id"]))

    prices_out = []
    if not df_z.empty:
        for _, r in df_z.iterrows():
            pids = set()
            if r.get("product_id"):
                pids.add(str(r["product_id"]))
            tn = r.get("trade_name") or ""
            if tn in tn_index:
                pids |= tn_index[tn]
            for pid in pids:
                prices_out.append({
                    "product_id": pid,
                    "znvlp_price_rub": r["price"],
                    "price_date": r["date"]
                })
    df_prices = pd.DataFrame(prices_out).drop_duplicates()

    # проставим флаг ЖНВЛП
    if not df_prices.empty:
        zn_ids = set(df_prices["product_id"].astype(str))
        df_p["is_znvlp"] = df_p["product_id"].astype(str).isin(zn_ids)

    # сортировки
    df_p = df_p.sort_values(["trade_name","dosage_form","pack"]).reset_index(drop=True)
    if not df_i.empty:
        df_i = df_i.sort_values(["product_id","inn"]).reset_index(drop=True)
    df_prices = df_prices.sort_values(["product_id"]).reset_index(drop=True)

    # запись
    df_p.to_csv(OUT_PRODUCTS, index=False)
    df_i.to_csv(OUT_INGRS, index=False)
    df_prices.to_csv(OUT_PRICES, index=False)
    with open(OUT_VERSION,"w",encoding="utf-8") as f:
        f.write(datetime.utcnow().isoformat())

    print(f"OK: products={len(df_p)}, ingrs={len(df_i)}, prices={len(df_prices)}")

if __name__ == "__main__":
    sys.exit(main())
