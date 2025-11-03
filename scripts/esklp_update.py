#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ESKLP обновлялка для Help.Drugix (v2)
- ONLINE: читает JSON из ENV-URL (ESKLP_*_URL)
- OFFLINE: data/source/esklp_*.json
- Нормализация по словарям из dictionaries/
- Цена ЖНВЛП: сопоставление по product_id → (бренд+форма) → бренд
- Нормализация упаковок ("№10", "10 табл", "табл. №10" и т.п.)
- Экспорт ATC-иерархии в data/atc.csv
- Дедупликация цен: по каждому product_id берём самую свежую

Выход:
  data/products.csv, data/ingredients.csv, data/prices.csv, data/atc.csv, data/version.txt
"""

import os, re, json, sys, urllib.request, hashlib
from datetime import datetime
import pandas as pd

DATA_DIR = "data"
SRC_DIR  = os.path.join(DATA_DIR, "source")
DICT_DIR = "dictionaries"

OUT_PRODUCTS = os.path.join(DATA_DIR, "products.csv")
OUT_INGRS    = os.path.join(DATA_DIR, "ingredients.csv")
OUT_PRICES   = os.path.join(DATA_DIR, "prices.csv")
OUT_ATC      = os.path.join(DATA_DIR, "atc.csv")
OUT_VERSION  = os.path.join(DATA_DIR, "version.txt")

ESKLP_PRODUCTS_URL     = os.getenv("ESKLP_PRODUCTS_URL", "").strip()
ESKLP_COMPOSITIONS_URL = os.getenv("ESKLP_COMPOSITIONS_URL", "").strip()
ESKLP_PRICES_URL       = os.getenv("ESKLP_PRICES_URL", "").strip()

# ---------------------- helpers ----------------------

def normspace(s:str) -> str:
    return re.sub(r"\s+"," ", (s or "").strip())

def lower(s:str) -> str:
    return normspace(s).lower()

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

def synthetic_id(*parts):
    raw = "||".join(str(p) for p in parts)
    return "pid_" + hashlib.md5(raw.encode("utf-8")).hexdigest()[:16]

def normalize_pack(pack:str) -> dict:
    """
    Извлекаем простые признаки упаковки:
    - количество единиц (№10 / 10 / x10) → units
    - тип единиц (таблетки/капсулы/пакеты...) по ключевым словам
    """
    p = lower(pack)
    units = None
    # номера вида №10
    m = re.search(r"№\s*(\d{1,4})", p)
    if m: units = int(m.group(1))
    # просто число, возможно рядом с типом
    if units is None:
        m = re.search(r"(\d{1,4})\s*(таб|\bтабл\b|капс|пакет|пакетов|пак\b|саше)", p)
        if m: units = int(m.group(1))
    # тип
    form_hint = None
    if re.search(r"\bтаб|\bтабл", p): form_hint = "таблетки"
    elif "капс" in p: form_hint = "капсулы"
    elif "пакет" in p or "саше" in p: form_hint = "пакеты"
    elif "порош" in p: form_hint = "порошок"
    return {"units": units, "form_hint": form_hint}

def split_atc(atc:str) -> dict:
    """
    Делим ATC-код на уровни. Пример N02BE51:
    L1=N, L2=N02, L3=N02B, L4=N02BE, L5=N02BE51
    """
    code = (atc or "").strip().upper()
    good = bool(re.match(r"^[A-Z]\d{2}[A-Z]\w{2}\d{0,2}$", code)) or bool(re.match(r"^[A-Z]\d{2}$", code))
    res = {
        "atc_code": code,
        "level1": code[:1] if len(code)>=1 else "",
        "level2": code[:3] if len(code)>=3 else "",
        "level3": code[:4] if len(code)>=4 else "",
        "level4": code[:5] if len(code)>=5 else "",
        "level5": code if len(code)>=7 else ""
    }
    res["code_valid"] = good
    return res

# ---------------------- loaders ----------------------

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
        pack = normspace(r.get("pack"))
        atc = normspace(r.get("atc")).upper()
        rows.append({
            "product_id":      str(pid),
            "trade_name":      tn,
            "dosage_form":     form,
            "pack":            pack,
            "country":         country,
            "is_znvlp":        False,
            "atc_code":        atc,
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

# ---------------------- main ----------------------

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
    # 1) по product_id
    pid_set = set(df_p["product_id"].astype(str))

    # 2) по бренд+форма (канонизированные)
    bf_index = {}
    for _, r in df_p.iterrows():
        key = (lower(r["trade_name"]), lower(r["dosage_form"]))
        bf_index.setdefault(key, set()).add(str(r["product_id"]))

    # 3) по бренду
    brand_index = {}
    for _, r in df_p.iterrows():
        key = lower(r["trade_name"])
        brand_index.setdefault(key, set()).add(str(r["product_id"]))

    # нормализованная упаковка для возможного дальнейшего усиления
    df_p["_units"] = None
    df_p["_form_hint"] = None
    if "pack" in df_p.columns:
        tmp = df_p["pack"].apply(normalize_pack)
        df_p["_units"] = tmp.apply(lambda x: x["units"])
        df_p["_form_hint"] = tmp.apply(lambda x: x["form_hint"])

    # мэппинг цен по приоритетам
    prices_out = []
    if not df_z.empty:
        for _, r in df_z.iterrows():
            # 1) product_id
            pids = set()
            if r.get("product_id"):
                pid = str(r["product_id"])
                if pid in pid_set:
                    pids.add(pid)

            # 2) бренд+форма
            if not pids:
                # формы у цен нет — используем наиболее вероятную: поиск по бренд+любой форме
                tn = (r.get("trade_name") or "").strip().lower()
                # если найдём нескольким формам — отметим все
                for form_key, ids in bf_index.items():
                    if form_key[0] == tn:
                        pids |= ids

            # 3) бренд
            if not pids:
                tn = (r.get("trade_name") or "").strip().lower()
                pids |= brand_index.get(tn, set())

            for pid in pids:
                prices_out.append({
                    "product_id": pid,
                    "znvlp_price_rub": r["price"],
                    "price_date": r["date"]
                })

    df_prices = pd.DataFrame(prices_out)
    if not df_prices.empty:
        # берём по каждому product_id самую свежую запись
        df_prices["price_date"] = pd.to_datetime(df_prices["price_date"], errors="coerce")
        df_prices = df_prices.sort_values(["product_id","price_date"]).dropna(subset=["price_date"])
        df_prices = df_prices.groupby("product_id", as_index=False).tail(1)

    # проставим флаг ЖНВЛП
    if not df_prices.empty:
        zn_ids = set(df_prices["product_id"].astype(str))
        df_p["is_znvlp"] = df_p["product_id"].astype(str).isin(zn_ids)

    # ATC-иерархия
    atc_rows = []
    for _, r in df_p.iterrows():
        if pd.isna(r.get("atc_code")): continue
        atc_info = split_atc(str(r["atc_code"]))
        atc_info["product_id"] = r["product_id"]
        atc_rows.append(atc_info)
    df_atc = pd.DataFrame(atc_rows).drop_duplicates()

    # финальные сортировки
    df_p = df_p.drop(columns=[c for c in ["_units","_form_hint"] if c in df_p.columns])
    df_p = df_p.sort_values(["trade_name","dosage_form","pack"]).reset_index(drop=True)
    if not df_i.empty:
        df_i = df_i.sort_values(["product_id","inn"]).reset_index(drop=True)
    if not df_prices.empty:
        df_prices = df_prices.sort_values(["product_id"]).reset_index(drop=True)
    if not df_atc.empty:
        df_atc = df_atc.sort_values(["product_id"]).reset_index(drop=True)

    # запись
    df_p.to_csv(OUT_PRODUCTS, index=False)
    df_i.to_csv(OUT_INGRS, index=False)
    df_prices.to_csv(OUT_PRICES, index=False)
    df_atc.to_csv(OUT_ATC, index=False)
    with open(OUT_VERSION,"w",encoding="utf-8") as f:
        f.write(datetime.utcnow().isoformat())

    print(f"OK: products={len(df_p)}, ingrs={len(df_i)}, prices={len(df_prices)}, atc_rows={len(df_atc)}")

if __name__ == "__main__":
    sys.exit(main())
