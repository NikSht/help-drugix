#!/usr/bin/env python3
import os, re, csv, sys, json, hashlib, datetime, pathlib, tempfile
from urllib.request import urlopen, Request
import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DICT = ROOT / "dictionaries"
DATA.mkdir(parents=True, exist_ok=True)

GRLS_URL = os.environ.get("GRLS_URL", "https://example.invalid/grls_export.csv")
ZNVLP_URL = os.environ.get("ZNVLP_URL", "https://example.invalid/znvlp.csv")

def sha8(s): import hashlib; return hashlib.sha1(s.encode("utf-8")).hexdigest()[:8]

def dl(url, dest):
    req = Request(url, headers={"User-Agent":"Mozilla/5.0"})
    with urlopen(req, timeout=120) as r, open(dest, "wb") as f:
        f.write(r.read())

def load_dict(path):
    if not path.exists(): return {}
    df = pd.read_csv(path)
    return {str(r["from"]).strip().lower(): str(r["to"]).strip().lower() for _,r in df.iterrows()}

INN_MAP = {}
FORM_MAP = {}

def norm_inn(s):
    s = (s or "").strip().lower()
    return INN_MAP.get(s, s)

def norm_form(s):
    s = (s or "").strip().lower()
    return FORM_MAP.get(s, s.capitalize())

dose_rx = re.compile(r"(?P<val>\d+[\d\.,\/]*)\s*(?P<unit>мг|мкг|г|мл|ме|%)", re.IGNORECASE)

def split_ingredients(s):
    if not s: return []
    raw_parts = re.split(r"\s*(\+|;|/|плюс)\s*", s, flags=re.IGNORECASE)
    parts = [p.strip() for p in raw_parts if p and p not in ['+',';','/','плюс']]
    out = []
    for p in parts:
        m = dose_rx.search(p)
        val, unit = None, None
        if m:
            val = float(str(m.group('val')).replace(',', '.'))
            unit = m.group('unit').lower()
        name = dose_rx.sub('', p).strip(' ,')
        out.append({"inn_raw": p, "inn": norm_inn(name), "strength": val, "unit": unit, "per_unit": ""})
    return out

def run():
    global INN_MAP, FORM_MAP
    INN_MAP = load_dict(DICT / "inn_synonyms.csv")
    FORM_MAP = load_dict(DICT / "form_normalization.csv")

    tmp = pathlib.Path(tempfile.mkdtemp())
    grls_path = tmp / "grls.csv"
    znvlp_path = tmp / "znvlp.csv"
    dl(GRLS_URL, grls_path)
    dl(ZNVLP_URL, znvlp_path)

    grls = pd.read_csv(grls_path)
    today = datetime.date.today().isoformat()

    products, ingredients, atc_rows = [], [], []
    for _, r in grls.iterrows():
        trade = str(r.get("trade_name","")).strip()
        reg = str(r.get("reg_number","")).strip()
        pack = str(r.get("pack","")).strip()
        pid = sha8(f"{reg}|{trade}|{pack}")
        form_raw = str(r.get("dosage_form","")).strip()
        products.append({
            "product_id": pid,
            "trade_name": trade,
            "reg_number": reg,
            "reg_status": str(r.get("reg_status","")).strip(),
            "dosage_form": norm_form(form_raw),
            "form_raw": form_raw,
            "atc_code": str(r.get("atc_code","")).strip(),
            "pack": pack,
            "country": str(r.get("country","")).strip(),
            "holder": str(r.get("holder","")).strip(),
            "manufacturer": str(r.get("manufacturer","")).strip(),
            "instruction_url": str(r.get("instruction_url","")).strip(),
            "ru_registry_url": str(r.get("ru_registry_url","")).strip(),
            "is_znvlp": False,
            "updated_at": today,
        })
        comp = str(r.get("composition","")).strip()
        for ing in split_ingredients(comp):
            ing["product_id"] = pid
            ing["updated_at"] = today
            ingredients.append(ing)
        if r.get("atc_code"):
            atc_rows.append({"product_id": pid, "atc_code": str(r.get("atc_code")), "source": "GRLS", "updated_at": today})

    zn = pd.read_csv(znvlp_path)
    prices = []
    for _, r in zn.iterrows():
        reg = str(r.get("reg_number","")).strip()
        trade = str(r.get("trade_name","")).strip()
        pack = str(r.get("pack","")).strip()
        pid = sha8(f"{reg}|{trade}|{pack}")
        price = str(r.get("price","0")).replace(',', '.')
        prices.append({"product_id": pid, "pack": pack, "znvlp_price_rub": float(price), "price_date": str(r.get("date","")), "updated_at": today})

    zn_pids = set(p["product_id"] for p in prices)
    for p in products:
        p["is_znvlp"] = p["product_id"] in zn_pids

    pd.DataFrame(products).to_csv(DATA / "products.csv", index=False)
    pd.DataFrame(ingredients).to_csv(DATA / "ingredients.csv", index=False)
    pd.DataFrame(prices).to_csv(DATA / "prices.csv", index=False)
    pd.DataFrame(atc_rows).to_csv(DATA / "atc.csv", index=False)

if __name__ == "__main__":
    run()
