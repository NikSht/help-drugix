#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ESKLP updater (ZIP bulk) — PRO v6

Что делает:
  • Скачивает ZIP-архив ЕСКЛП (ESKLP_BULK_URL) и парсит все таблицы (XLSX/CSV/JSON)
  • Автоклассификация таблиц: products / compositions / prices / ATC / manufacturers / registers
  • Распознаёт русские/английские названия колонок по словарям-синонимам
  • Нормализует ТН/формы/страны по dictionaries/*
  • Строит итоговые CSV:
        data/products.csv      (расширенный состав полей)
        data/ingredients.csv
        data/prices.csv        (ЖНВЛП, с дедупликацией по product_id и дате)
        data/atc.csv           (иерархия 1–5)
        data/version.txt
  • При отсутствии BULK — fallback на ESKLP_PRODUCTS_URL / ESKLP_COMPOSITIONS_URL / ESKLP_PRICES_URL

Логика мэппинга цен:
  price -> product_id (если есть) → (brand+form) → brand

Примечание:
  Мы добавляем в products.csv дополнительные столбцы (ok для твоего фронта — лишние игнорируются):
    manufacturer, holder, reg_number, reg_status, country, atc_code, ru_registry_url, instruction_url, etc.
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

# ====== helpers ======
def norm(s): return re.sub(r"\s+"," ", (str(s) if s is not None else "").strip())
def low (s): return norm(s).lower()

def http_get(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=360) as r:
        return r.read()

def load_dict_csv(path, to_lower=True):
    if not os.path.exists(path): return {}
    try:
        df = pd.read_csv(path)
    except Exception:
        return {}
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

# ====== readers ======
def read_table_from_bytes(name: str, data: bytes) -> pd.DataFrame:
    n = name.lower()

    # JSON
    try:
        if n.endswith(".json"):
            obj = json.loads(data.decode("utf-8", errors="ignore"))
            if isinstance(obj, dict) and "data" in obj: obj = obj["data"]
            return pd.json_normalize(obj)
    except Exception:
        pass

    # CSV (utf-8/; fallback ;)
    try:
        text = data.decode("utf-8", errors="ignore")
        try:
            return pd.read_csv(io.StringIO(text))
        except Exception:
            return pd.read_csv(io.StringIO(text), sep=";")
    except Exception:
        pass

    # XLSX
    if n.endswith((".xlsx",".xls")):
        return pd.read_excel(io.BytesIO(data))

    return pd.DataFrame()

def load_table_url(url: str) -> pd.DataFrame:
    if not url: return pd.DataFrame()
    raw = http_get(url)
    # JSON
    try:
        arr = json.loads(raw.decode("utf-8", errors="ignore"))
        if isinstance(arr, dict) and "data" in arr: arr = arr["data"]
        return pd.json_normalize(arr)
    except Exception:
        pass
    # CSV
    try:
        text = raw.decode("utf-8", errors="ignore")
        try:
            return pd.read_csv(io.StringIO(text))
        except Exception:
            return pd.read_csv(io.StringIO(text), sep=";")
    except Exception:
        pass
    # XLSX
    try:
        return pd.read_excel(io.BytesIO(raw))
    except Exception:
        return pd.DataFrame()

# ====== schema / synonyms ======
# ключи — множества синонимов для русских/английских названий
COLS = {
    "product_id": {"product_id","id","ид","код","код_изделия","идентификатор","ид_продукта"},
    "trade_name": {"trade_name","brand","tn","тн","торговое_наименование","наименование_торговое","лекарственный_препарат"},
    "dosage_form":{"dosage_form","form","форма","форма_выпуска","лекарственная_форма"},
    "pack":       {"pack","package","упаковка","упак","доза_упаковки","количество_в_упаковке"},
    "country":    {"country","origin_country","страна","страна_производства","страна_происхождения"},
    "atc_code":   {"atc_code","atc","атс","атх","код_атс","код_атх"},
    "registry":   {"ru_registry_url","registry_url","grls","грлс","ссылка_грлс"},
    "leaflet":    {"instruction_url","leaflet_url","инструкция","ссылка_инструкция"},

    "inn":        {"inn","ingredient","substance","мнг","мнн","действующее_вещество","ингредиент","вещество"},
    "strength":   {"strength","dose","доза","количество","содержание"},
    "unit":       {"unit","uom","ед","единица","единица_изм","ед_изм"},

    "price":      {"price_rub","price","znvlp_price","предельная_цена","цена","макс_цена"},
    "price_date": {"date","price_date","updated_at","дата","дата_установления"},

    "manufacturer":{"manufacturer","producer","изготовитель","производитель","организация_производитель"},
    "holder":     {"holder","владелец_рег_удостоверения","держатель_регистрации","держатель_ру"},
    "reg_number": {"reg_number","регистрационный_номер","рег_номер","номер_ру"},
    "reg_status": {"status","reg_status","статус","статус_регистрации"},
}

def pick(row, key, default=""):
    # выбираем значение по набору синонимов
    synonyms = COLS.get(key, set())
    for c in row.index:
        if c.lower() in synonyms:
            v = row[c]
            if pd.notna(v): return v
    return default

def pick_from_df(df, key):
    syns = COLS.get(key, set())
    for c in df.columns:
        if c.lower() in syns:
            return c
    return None

# ====== classification ======
def guess_kind(df: pd.DataFrame, filename: str) -> str | None:
    cols = {c.lower() for c in df.columns}
    name = filename.lower()

    # compositions
    if {"inn"} & cols and ("product_id" in cols or "id" in cols or "код" in cols):
        return "compositions"
    if ("substance" in cols or "ingredient" in cols) and ("product_id" in cols or "id" in cols):
        return "compositions"
    if any(k in name for k in ["состав","ингредиент","composition","ingredients"]):
        return "compositions"

    # prices
    if ({"price_rub","price","znvlp_price","цена","предельная_цена"} & cols):
        if {"product_id","trade_name","brand","тн","торговое_наименование"} & cols:
            return "prices"
    if any(k in name for k in ["жнвлп","price","цена","предельн"]):
        return "prices"

    # products
    prod_keys = {"trade_name","brand","торговое_наименование","dosage_form","форма_выпуска","pack","упаковка","country","страна"} & cols
    if len(prod_keys) >= 2:
        return "products"
    if any(k in name for k in ["prod","product","лекарств","реестр","препарат","tn","brand"]):
        return "products"

    # atc / атх — отдельная таблица бывает не всегда
    if {"atc","atc_code","атс","атх"} & cols:
        return "products"  # полезнее слить с продуктами

    return None

# ====== load from BULK zip ======
def load_from_bulk(url: str):
    raw = http_get(url)
    products, comps, prices = [], [], []
    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
        for info in zf.infolist():
            if info.is_dir(): continue
            fname = info.filename
            data = zf.read(info)
            df = read_table_from_bytes(fname, data)
            if df.empty: 
                continue
            kind = guess_kind(df, fname)
            if kind == "products":     products.append(df)
            elif kind == "compositions": comps.append(df)
            elif kind == "prices":      prices.append(df)
    dfp = pd.concat(products, ignore_index=True) if products else pd.DataFrame()
    dfi = pd.concat(comps,     ignore_index=True) if comps     else pd.DataFrame()
    dfz = pd.concat(prices,    ignore_index=True) if prices    else pd.DataFrame()
    return dfp, dfi, dfz

# ====== main ======
def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    # словари нормализаций
    d_inn   = load_dict_csv(os.path.join(DICT_DIR,"inn_synonyms.csv"), True)
    d_brand = load_dict_csv(os.path.join(DICT_DIR,"brand_synonyms.csv"), False)
    d_form  = load_dict_csv(os.path.join(DICT_DIR,"form_normalization.csv"), True)
    d_ctry  = load_dict_csv(os.path.join(DICT_DIR,"country_normalization.csv"), True)

    # загрузка сырых таблиц
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
        pid = str(pick(r, "product_id", "")).strip()
        tn  = syn(norm(pick(r, "trade_name", "")), d_brand, to_lower=False)
        frm = syn(norm(pick(r, "dosage_form", "")), d_form, True)
        pack= norm(pick(r, "pack", ""))
        ctr = syn(low (pick(r, "country", "")), d_ctry, True)
        atc = norm(pick(r, "atc_code", "")).upper()
        reg = norm(pick(r, "registry", ""))
        ins = norm(pick(r, "leaflet", ""))

        manufacturer = norm(pick(r, "manufacturer", ""))
        holder       = norm(pick(r, "holder", ""))
        reg_number   = norm(pick(r, "reg_number", ""))
        reg_status   = norm(pick(r, "reg_status", ""))

        if not pid:
            pid = synthetic_id(tn, frm, pack)

        prows.append({
            "product_id": pid,
            "trade_name": tn,
            "dosage_form": frm,
            "pack": pack,
            "country": ctr,
            "is_znvlp": False,
            "atc_code": atc,
            "ru_registry_url": reg,
            "instruction_url": ins,
            "manufacturer": manufacturer,
            "holder": holder,
            "reg_number": reg_number,
            "reg_status": reg_status
        })
    dfP = pd.DataFrame(prows).drop_duplicates() if prows else pd.DataFrame()
    if dfP.empty:
        print("ERROR: products empty (BULK not parsed?)", file=sys.stderr)
        return 1

    # ---------- COMPOSITIONS transform ----------
    irows=[]
    for _,r in (dfi_raw if not dfi_raw.empty else pd.DataFrame()).iterrows():
        pid = str(pick(r, "product_id", "")).strip() or str(pick(r,"trade_name",""))
        inn = syn(low(pick(r, "inn", "")), d_inn, True)
        strength = str(pick(r, "strength", "")).replace(",", ".")
        unit = low(pick(r, "unit", ""))
        if pid and inn:
            irows.append({"product_id":pid,"inn":inn,"strength":strength,"unit":unit})
    dfI = pd.DataFrame(irows).drop_duplicates(subset=["product_id","inn","strength","unit"]) if irows else pd.DataFrame()

    # ---------- PRICES (ЖНВЛП) ----------
    zrows=[]
    for _,r in (dfz_raw if not dfz_raw.empty else pd.DataFrame()).iterrows():
        price = pick(r, "price", None)
        if price in (None, ""): continue
        try: price = float(str(price).replace(",", ".")); 
        except Exception: continue
        pid = str(pick(r,"product_id","")).strip()
        tn  = low(pick(r,"trade_name",""))
        date= norm(pick(r,"price_date","")) or datetime.utcnow().date().isoformat()
        zrows.append({"product_id":pid,"trade_name":tn,"price":price,"date":date})
    dfZ_raw = pd.DataFrame(zrows)

    # индексы для мэппинга цен
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

    # ---------- ATC иерархия ----------
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
    return 0

if __name__ == "__main__":
    sys.exit(main())
