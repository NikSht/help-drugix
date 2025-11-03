#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Собираем комбинированные препараты из ESKLP ZIP (xlsx-выпуск):
- читаем все файлы вида esklp_klp_*.xlsx из архива
- берём только КЛП с несколькими МНН (признак: "Нормализованное МНН" содержит '+')
- на выходе: data/products.csv и data/ingredients.csv
- data/prices.csv — создаём пустой заголовок (заглушка для фронта)
"""

import os
import io
import sys
import csv
import glob
import zipfile
import urllib.request
from datetime import datetime

import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data")
SRC_DIR  = os.path.join(DATA_DIR, "source")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(SRC_DIR, exist_ok=True)

# ----------------------------
# Настройки входа/выхода
# ----------------------------
OUT_PRODUCTS    = os.path.join(DATA_DIR, "products.csv")
OUT_INGREDIENTS = os.path.join(DATA_DIR, "ingredients.csv")
OUT_PRICES      = os.path.join(DATA_DIR, "prices.csv")  # заглушка

# Колонки на выходе (под фронт)
PRODUCTS_COLUMNS = [
    "product_id",        # КЛП (код)
    "trade_name",        # Торговое наименование
    "dosage_form",       # Нормализованная лекарственная форма
    "pack",              # Упаковка/дозировка (соберём скромно)
    "country",           # нет в ESKLP -> пусто
    "is_znvlp",          # нет данных -> False
    "atc_code",          # нет в выгрузке -> пусто
    "ru_registry_url",   # ссылка на ГРЛС (если бы была) -> пусто
    "instruction_url",   # ссылка на инструкцию -> пусто
]

ING_COLUMNS = [
    "product_id",  # КЛП
    "inn",         # МНН (нормализованное)
    "strength",    # пусто (выпилим, чтобы не раздувать)
    "unit",        # пусто
]

# Названия столбцов в excel (русские имена из вкладки с данными)
COL_KLP_CODE   = "Код КЛП"
COL_TRADE_NAME = "Торговое наименование"
COL_MNN_NORM   = "Нормализованное МНН"
COL_FORM_NORM  = "Нормализованная лекарственная форма"
COL_DOSE_NORM  = "Нормализованная дозировка"

def http_get_bytes(url: str, timeout: int = 360) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()

def find_local_zip() -> str:
    # Берём самый большой .zip из data/source — он обычно и есть ESKLP
    zips = glob.glob(os.path.join(SRC_DIR, "*.zip"))
    if not zips:
        return ""
    zips.sort(key=lambda p: os.path.getsize(p), reverse=True)
    return zips[0]

def load_zip_bytes() -> bytes:
    url = os.getenv("ESKLP_BULK_URL", "").strip()
    if url:
        print(f"BULK ZIP (from URL): {url}")
        return http_get_bytes(url)
    local = find_local_zip()
    if not local:
        print("ERROR: no ESKLP zip source found (no ESKLP_BULK_URL and no data/source/*.zip)")
        sys.exit(1)
    print(f"BULK ZIP (local): {local}")
    with open(local, "rb") as f:
        return f.read()

def read_klp_tables_from_zip(zip_bytes: bytes) -> list[pd.DataFrame]:
    """Возвращаем список датафреймов из листов esklp_klp_*.xlsx (лист с данными — обычно второй)."""
    dfs = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        names = [n for n in zf.namelist() if n.lower().endswith(".xlsx") and "esklp_klp_" in n.lower()]
        names.sort()
        print("Found files:")
        for n in names:
            print(" -", n)
        for n in names:
            with zf.open(n) as fh:
                # Лист с данными — как правило, второй (index=1). Если не получится — попробуем 0.
                try_order = [1, 0]
                read_ok = False
                for idx in try_order:
                    try:
                        df = pd.read_excel(fh, sheet_name=idx, engine="openpyxl")
                        # нормализуем заголовки: убираем \n и пробелы по краям
                        df.columns = [str(c).strip().replace("\n", " ").replace("\r", " ") for c in df.columns]
                        # минимум столбцов должен присутствовать:
                        if all(c in df.columns for c in [COL_KLP_CODE, COL_TRADE_NAME, COL_MNN_NORM, COL_FORM_NORM, COL_DOSE_NORM]):
                            dfs.append(df)
                            read_ok = True
                            break
                    except Exception as e:
                        continue
                if not read_ok:
                    print(f"WARN: {n} — не удалось прочитать нужный лист/колонки, пропускаю.")
    return dfs

def split_inns(mnn: str) -> list[str]:
    if not isinstance(mnn, str):
        return []
    # Разбиваем по " + " и запятым/точкам с запятыми
    raw = mnn.replace(";", "+").replace(",", "+")
    parts = [p.strip() for p in raw.split("+") if p.strip()]
    # Нормализуем регистр
    return [p.lower() for p in parts]

def main():
    raw = load_zip_bytes()
    dfs = read_klp_tables_from_zip(raw)
    if not dfs:
        print("ERROR: no tables read from ZIP")
        sys.exit(1)

    prows = []  # для products.csv
    irows = []  # для ingredients.csv

    seen_products = set()  # (klp_code) чтобы уникализировать
    seen_ing = set()       # (klp_code, inn)

    total_rows = 0
    for df in dfs:
        total_rows += len(df)

        # Оставляем только комбинированные позиции (есть '+')
        mask_combo = df[COL_MNN_NORM].astype(str).str.contains(r"\+", regex=True, na=False)
        sub = df.loc[mask_combo, [COL_KLP_CODE, COL_TRADE_NAME, COL_MNN_NORM, COL_FORM_NORM, COL_DOSE_NORM]].copy()

        for _, r in sub.iterrows():
            klp = str(r[COL_KLP_CODE]).strip()
            if not klp or klp == "nan":
                continue

            trade_name = str(r[COL_TRADE_NAME]).strip()
            form = str(r[COL_FORM_NORM]).strip()
            dose = str(r[COL_DOSE_NORM]).strip()
            mnn_norm = str(r[COL_MNN_NORM]).strip()

            # pack соберём из нормализованной дозировки (без излишков)
            pack = dose

            if klp not in seen_products:
                prows.append([
                    klp,             # product_id
                    trade_name,      # trade_name
                    form,            # dosage_form
                    pack,            # pack
                    "",              # country
                    False,           # is_znvlp
                    "",              # atc_code
                    "",              # ru_registry_url
                    "",              # instruction_url
                ])
                seen_products.add(klp)

            inns = split_inns(mnn_norm)
            for inn in inns:
                key = (klp, inn)
                if inn and key not in seen_ing:
                    irows.append([klp, inn, "", ""])
                    seen_ing.add(key)

    print(f"OK: scanned_rows={total_rows}, products={len(prows)}, ingrs={len(irows)}")

    # Пишем products.csv (компактно)
    with open(OUT_PRODUCTS, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(PRODUCTS_COLUMNS)
        w.writerows(prows)

    # Пишем ingredients.csv
    with open(OUT_INGREDIENTS, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(ING_COLUMNS)
        w.writerows(irows)

    # Пишем пустышку цен, чтобы фронт не ломался
    if not os.path.exists(OUT_PRICES):
        with open(OUT_PRICES, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["product_id","znvlp_price_rub","price_date"])

    print("DONE")

if __name__ == "__main__":
    main()
