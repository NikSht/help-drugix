#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ESKLP обновлялка для Help.Drugix
--------------------------------
Работает в двух режимах:
- ONLINE: читает JSON из эндпоинтов ESKLP (ENV-переменные: ESKLP_*_URL)
- OFFLINE: если URL нет/недоступны — читает локальные JSON из data/source/

На выходе: data/products.csv, data/ingredients.csv, data/prices.csv, data/version.txt
Форматы ровно те, что ждёт мини-апп.
"""

import os
import io
import re
import json
import sys
import math
import time
import urllib.request
from datetime import datetime
import pandas as pd

DATA_DIR = "data"
SRC_DIR  = os.path.join(DATA_DIR, "source")

OUT_PRODUCTS = os.path.join(DATA_DIR, "products.csv")
OUT_INGRS    = os.path.join(DATA_DIR, "ingredients.csv")
OUT_PRICES   = os.path.join(DATA_DIR, "prices.csv")
OUT_VERSION  = os.path.join(DATA_DIR, "version.txt")

# ONLINE-режим: задай эти переменные в Secrets (Settings → Secrets → Actions)
ESKLP_PRODUCTS_URL    = os.getenv("ESKLP_PRODUCTS_URL", "").strip()
ESKLP_COMPOSITIONS_URL= os.getenv("ESKLP_COMPOSITIONS_URL", "").strip()
ESKLP_PRICES_URL      = os.getenv("ESKLP_PRICES_URL", "").strip()

# Ожидаемые поля входных JSON (можно подправить под реальную схему)
# products.json — запись вида:
# {
#   "id": "uuid/num",
#   "trade_name": "Торговое название",
#   "dosage_form": "Таблетки",
#   "pack": "№10; блистер",
#   "country": "Индия",
#   "atc": "N02BE51",
#   "registry_url": "https://...",
#   "instruction_url": "https://..."
# }
#
# compositions.json — одна строка на компонент:
# {
#   "product_id": "<id из products>",
#   "inn": "ибупрофен",
#   "strength": "200",
#   "unit": "мг"
# }
#
# prices.json — ЖНВЛП:
# {
#   "product_id": "<id из products или иной ключ> (можно пусто)",
#   "trade_name": "ТН",
#   "price_rub": 123.45,
#   "date": "2024-11-01"
# }

def _read_url_json(url: str):
    if not url:
        return None
    try:
        print(f"→ ONLINE: GET {url}")
        with urllib.request.urlopen(url, timeout=90) as resp:
            data = resp.read()
        return json.loads(data.decode("utf-8", errors="ignore"))
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

def yes(x) -> bool:
    s = str(x).strip().lower()
    return s in {"1","true","да","y","yes"}

def load_products():
    data = _read_url_json(ESKLP_PRODUCTS_URL)
    if data is None:
        path = os.path.join(SRC_DIR, "esklp_products.json")
        print(f"→ OFFLINE: {path}")
        data = _read_file_json(path)

    rows = []
    for i, r in enumerate(data or []):
        pid = r.get("id") or f"esklp_{i+1:07d}"
        rows.append({
            "product_id":      str(pid),
            "trade_name":      norm(r.get("trade_name")),
            "dosage_form":     norm(r.get("dosage_form")),
            "pack":            norm(r.get("pack")),
            "country":         norm(r.get("country")),
            "is_znvlp":        False,  # пометим позже, когда соотнесём цены
            "atc_code":        norm(r.get("atc")).upper(),
            "ru_registry_url": r.get("registry_url") or "",
            "instruction_url": r.get("instruction_url") or "",
        })
    return pd.DataFrame(rows)

def load_compositions():
    data = _read_url_json(ESKLP_COMPOSITIONS_URL)
    if data is None:
        path = os.path.join(SRC_DIR, "esklp_compositions.json")
        print(f"→ OFFLINE: {path}")
        data = _read_file_json(path)

    rows = []
    for r in (data or []):
        rows.append({
            "product_id": str(r.get("product_id") or ""),
            "inn":        norm(r.get("inn")).lower(),
            "strength":   str(r.get("strength") or "").replace(",", "."),
            "unit":       norm(r.get("unit")).lower(),
        })
    # фильтруем пустые
    rows = [x for x in rows if x["product_id"] and x["inn"]]
    return pd.DataFrame(rows)

def load_prices():
    data = _read_url_json(ESKLP_PRICES_URL)
    if data is None:
        path = os.path.join(SRC_DIR, "esklp_prices.json")
        print(f"→ OFFLINE: {path}")
        data = _read_file_json(path)

    rows = []
    for r in (data or []):
        # допускаем отсутствие product_id — будем маппить по trade_name
        rows.append({
            "product_id":  str(r.get("product_id") or ""),
            "trade_name":  norm(r.get("trade_name")).lower(),
            "price":       float(str(r.get("price_rub") or "0").replace(",", ".") or 0) or None,
            "date":        norm(r.get("date")) or datetime.utcnow().date().isoformat(),
        })
    # оставляем только где есть цена
    rows = [x for x in rows if x["price"] is not None]
    return pd.DataFrame(rows)

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(SRC_DIR, exist_ok=True)

    df_p = load_products()
    df_i = load_compositions()
    df_z = load_prices()

    if df_p.empty:
        print("ERROR: список продуктов пуст — нет данных ни ONLINE, ни OFFLINE", file=sys.stderr)
        sys.exit(1)

    # маппинг ЖНВЛП: пробуем по product_id, иначе по ТН
    df_prices_out = []
    tn_to_pid = { (str(r.trade_name or "").lower(), r.product_id) for _, r in df_p.iterrows() }
    tn_index = {}
    for tn, pid in tn_to_pid:
        if tn:
            tn_index.setdefault(tn, set()).add(pid)

    for _, r in (df_z if not df_z.empty else pd.DataFrame()).iterrows():
        pids = set()
        if r.get("product_id"):
            pids.add(str(r["product_id"]))
        tn = str(r.get("trade_name") or "").lower()
        if tn and tn in tn_index:
            pids |= tn_index[tn]

        for pid in pids:
            df_prices_out.append({
                "product_id": pid,
                "znvlp_price_rub": r["price"],
                "price_date": r["date"],
            })

    df_prices_out = pd.DataFrame(df_prices_out)

    # пометим ЖНВЛП
    if not df_prices_out.empty:
        znvlp_ids = set(df_prices_out["product_id"].astype(str).tolist())
        df_p["is_znvlp"] = df_p["product_id"].astype(str).isin(znvlp_ids)

    # сортировка/чистка
    if not df_i.empty:
        df_i = df_i.dropna(subset=["product_id","inn"]).copy()
        df_i["inn"] = df_i["inn"].str.strip().str.lower()
        df_i = df_i.sort_values(["product_id","inn"])

    df_p = df_p.sort_values(["trade_name","dosage_form","pack"]).reset_index(drop=True)
    df_prices_out = df_prices_out.sort_values(["product_id"]).reset_index(drop=True)

    # запись
    df_p.to_csv(OUT_PRODUCTS, index=False)
    df_i.to_csv(OUT_INGRS, index=False)
    df_prices_out.to_csv(OUT_PRICES, index=False)

    with open(OUT_VERSION, "w", encoding="utf-8") as f:
        f.write(datetime.utcnow().isoformat())

    print(f"OK: products={len(df_p)}, ingrs={len(df_i)}, prices={len(df_prices_out)}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
