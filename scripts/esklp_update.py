#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ESKLP обновлялка для Help.Drugix
ONLINE: читает JSON из ENV-URL (ESKLP_*_URL)
OFFLINE: если URL нет — берёт data/source/*.json

На выходе: data/products.csv, data/ingredients.csv, data/prices.csv, data/version.txt
"""

import os, re, json, sys, urllib.request
from datetime import datetime
import pandas as pd

DATA_DIR = "data"
SRC_DIR  = os.path.join(DATA_DIR, "source")
OUT_PRODUCTS = os.path.join(DATA_DIR, "products.csv")
OUT_INGRS    = os.path.join(DATA_DIR, "ingredients.csv")
OUT_PRICES   = os.path.join(DATA_DIR, "prices.csv")
OUT_VERSION  = os.path.join(DATA_DIR, "version.txt")

ESKLP_PRODUCTS_URL     = os.getenv("ESKLP_PRODUCTS_URL", "").strip()
ESKLP_COMPOSITIONS_URL = os.getenv("ESKLP_COMPOSITIONS_URL", "").strip()
ESKLP_PRICES_URL       = os.getenv("ESKLP_PRICES_URL", "").strip()

def _read_url_json(url: str):
    if not url: return None
    try:
        print(f"→ ONLINE: {url}")
        with urllib.request.urlopen(url, timeout=90) as resp:
            return json.loads(resp.read().decode("utf-8", errors="ignore"))
    except Exception as e:
        print(f"ONLINE FAIL: {e}", file=sys.stderr)
        return None

def _read_file_json(path: str):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []

def norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def load_products() -> pd.DataFrame:
    data = _read_url_json(ESKLP_PRODUCTS_URL)
    if data is None:
        data = _read_file_json(os.path.join(SRC_DIR, "esklp_products.json"))
        print("→ OFFLINE: esklp_products.json")
    rows = []
    for i, r in enumerate(data or []):
        pid = r.get("id") or f"esklp_{i+1:07d}"
        rows.append({
            "product_id":      str(pid),
            "trade_name":      norm(r.get("trade_name")),
            "dosage_form":     norm(r.get("dosage_form")),
            "pack":            norm(r.get("pack")),
            "country":         norm(r.get("country")),
            "is_znvlp":        False,
            "atc_code":        norm(r.get("atc")).upper(),
            "ru_registry_url": r.get("registry_url") or "",
            "instruction_url": r.get("instruction_url") or "",
        })
    return pd.DataFrame(rows)

def load_compositions() -> pd.DataFrame:
    data = _read_url_json(ESKLP_COMPOSITIONS_URL)
    if data is None:
        data = _read_file_json(os.path.join(SRC_DIR, "esklp_compositions.json"))
        print("→ OFFLINE: esklp_compositions.json")
    rows = []
    for r in (data or []):
        rows.append({
            "product_id": str(r.get("product_id") or ""),
            "inn":        norm(r.get("inn")).lower(),
            "strength":   str(r.get("strength") or "").replace(",", "."),
            "unit":       norm(r.get("unit")).lower(),
        })
    rows = [x for x in rows if x["product_id"] and x["inn"]]
    return pd.DataFrame(rows)

def load_prices() -> pd.DataFrame:
    data = _read_url_json(ESKLP_PRICES_URL)
    if data is None:
        data = _read_file_json(os.path.join(SRC_DIR, "esklp_prices.json"))
        print("→ OFFLINE: esklp_prices.json")
    rows = []
    for r in (data or []):
        rows.append({
            "product_id":  str(r.get("product_id") or ""),
            "trade_name":  norm(r.get("trade_name")).lower(),
            "price":       float(str(r.get("price_rub") or "0").replace(",", ".") or 0) if r.get("price_rub") is not None else None,
            "date":        norm(r.get("date")) or datetime.utcnow().date().isoformat(),
        })
    rows = [x for x in rows if x["price"] is not None]
    return pd.DataFrame(rows)

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(SRC_DIR, exist_ok=True)

    df_p = load_products()
    df_i = load_compositions()
    df_z = load_prices()

    if df_p.empty:
        print("ERROR: products empty", file=sys.stderr); sys.exit(1)

    # цены: сначала по product_id, затем по trade_name
    prices_out = []
    tn_index = {}
    for _, r in df_p.iterrows():
        tn = (r["trade_name"] or "").lower()
        if tn: tn_index.setdefault(tn, set()).add(r["product_id"])

    for _, r in df_z.iterrows():
        pids = set()
        if r.get("product_id"): pids.add(str(r["product_id"]))
        tn = str(r.get("trade_name") or "")
        if tn and tn in tn_index: pids |= tn_index[tn]
        for pid in pids:
            prices_out.append({
                "product_id": pid,
                "znvlp_price_rub": r["price"],
                "price_date": r["date"],
            })
    df_prices = pd.DataFrame(prices_out)

    if not df_prices.empty:
        zn_ids = set(df_prices["product_id"].astype(str))
        df_p["is_znvlp"] = df_p["product_id"].astype(str).isin(zn_ids)

    # сортировка и запись
    df_p = df_p.sort_values(["trade_name","dosage_form","pack"]).reset_index(drop=True)
    if not df_i.empty:
        df_i = df_i.dropna(subset=["product_id","inn"]).sort_values(["product_id","inn"]).reset_index(drop=True)
    df_prices = df_prices.sort_values(["product_id"]).reset_index(drop=True)

    df_p.to_csv(OUT_PRODUCTS, index=False)
    df_i.to_csv(OUT_INGRS, index=False)
    df_prices.to_csv(OUT_PRICES, index=False)
    with open(OUT_VERSION, "w", encoding="utf-8") as f:
        f.write(datetime.utcnow().isoformat())
    print(f"OK: products={len(df_p)}, ingrs={len(df_i)}, prices={len(df_prices)}")

if __name__ == "__main__":
    sys.exit(main())
