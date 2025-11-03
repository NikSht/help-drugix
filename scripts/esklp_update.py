#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import io, os, re, sys, json, zipfile, tempfile, urllib.request
from pathlib import Path

import pandas as pd

# ------------------ настройки ввода/вывода ------------------
ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
SRC_DIR  = DATA_DIR / "source"
DATA_DIR.mkdir(parents=True, exist_ok=True)
SRC_DIR.mkdir(parents=True, exist_ok=True)

OUT_PRODUCTS   = DATA_DIR / "products.csv"
OUT_INGRS      = DATA_DIR / "ingredients.csv"
OUT_PRICES     = DATA_DIR / "prices.csv"
OUT_VERSIONTXT = DATA_DIR / "version.txt"  # заполняется джобом отдельно, но не мешает здесь

# Если секрет не задан – читаем зеркальный ZIP из репозитория (как fallback)
BULK_URL = os.environ.get("ESKLP_BULK_URL", "").strip()
LOCAL_ZIP = SRC_DIR / "esklp_bulk.zip"   # fallback: положенный вручную ZIP (или скачанный релизом Actions)

# ------------------ утилиты ------------------
def http_get(url: str, timeout=60) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()

def load_zip_bytes() -> bytes:
    if BULK_URL:
        print(f"BULK ZIP: {BULK_URL}")
        return http_get(BULK_URL, timeout=360)
    # fallback
    if LOCAL_ZIP.exists():
        print(f"BULK ZIP (local): {LOCAL_ZIP}")
        return LOCAL_ZIP.read_bytes()
    raise RuntimeError("Не нашли ни ESKLP_BULK_URL, ни локальный data/source/esklp_bulk.zip")

def find_country_column(columns):
    """Берём первый столбец, содержащий 'страна' (на всякий случай для разных версий выгрузки)."""
    for c in columns:
        if "страна" in str(c).strip().lower():
            return c
    return None

def normalize_str(x):
    if pd.isna(x):
        return ""
    return str(x).strip()

def split_inns(mnn: str):
    """Разбиваем нормализованное МНН по '+' с очисткой."""
    if not mnn:
        return []
    # бывают разделители + с пробелами
    return [p.strip() for p in re.split(r"\s*\+\s*", mnn) if p.strip()]

# ------------------ основная сборка ------------------
def build_csvs_from_esklp_zip(zip_bytes: bytes):
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        # Собираем все XLSX вида esklp_klp_*.xlsx
        names = [n for n in z.namelist() if re.search(r"(^|/)(esklp_klp_.*?\.xlsx)$", n, flags=re.I)]
        if not names:
            raise RuntimeError("В ZIP не найдено esklp_klp_*.xlsx")

        frames = []
        for name in names:
            print(f"Found file: {name}")
            with z.open(name) as f:
                # читаем все листы и ищем тот, где есть нужные колонки
                x = pd.read_excel(f, sheet_name=None, dtype=str, engine="openpyxl")
                for sheet_name, df in x.items():
                    cols = [str(c).strip() for c in df.columns]
                    need = {
                        "Торговое наименование",
                        "Нормализованное МНН",
                        "Нормализованная лекарственная форма",
                        "Нормализованная дозировка",
                        "Код КЛП",
                    }
                    if need.issubset(set(cols)):
                        df = df.rename(columns={c: c.strip() for c in df.columns})
                        frames.append(df)
                        break  # из файла берём один «главный» лист

        if not frames:
            raise RuntimeError("Не нашли подходящих листов с колонками (КЛП/МНН/форма/дозировка/ТН).")

        df = pd.concat(frames, ignore_index=True)

    # Чистим и приводим
    for col in df.columns:
        df[col] = df[col].astype(str).map(lambda x: "" if x == "nan" else x).fillna("")

    col_trade     = "Торговое наименование"
    col_mnn_norm  = "Нормализованное МНН"
    col_form_norm = "Нормализованная лекарственная форма"
    col_dose_norm = "Нормализованная дозировка"
    col_klp       = "Код КЛП"
    col_country   = find_country_column(df.columns)  # может не быть – переживём

    # Отбрасываем пустые КЛП
    df = df[df[col_klp].astype(str).str.strip() != ""].copy()

    # Готовим products.csv
    products = pd.DataFrame()
    products["product_id"]          = df[col_klp].map(lambda x: f"klp_{x.strip()}")
    products["trade_name"]          = df[col_trade].map(normalize_str)
    products["dosage_form"]         = df[col_form_norm].map(normalize_str)
    products["pack"]                = ""  # упаковку аккуратно соберём позже (из первичной/вторичной), сейчас пусто
    products["country"]             = df[col_country].map(normalize_str) if col_country else ""
    products["is_znvlp"]            = False  # ЕСКЛП != ЖНВЛП; проставим позже из другого источника
    products["atc_code"]            = ""     # из ГРЛС/АТХ позже
    products["ru_registry_url"]     = ""     # ссылка на ГРЛС (при желании добавим маппинг)
    products["instruction_url"]     = ""
    products["manufacturer"]        = ""     # при желании дотянем позже
    products["holder_reg_num"]      = ""
    products["normalized_mnn"]      = df[col_mnn_norm].map(normalize_str)
    products["klp_code"]            = df[col_klp].map(normalize_str)
    products["normalized_form"]     = df[col_form_norm].map(normalize_str)
    products["normalized_strength"] = df[col_dose_norm].map(normalize_str)

    # Убираем дубликаты по product_id
    products = products.drop_duplicates(subset=["product_id"])

    # Готовим ingredients.csv (один продукт → несколько МНН)
    rows = []
    for pid, mnn, dose in zip(products["product_id"], products["normalized_mnn"], products["normalized_strength"]):
        for inn in split_inns(mnn):
            rows.append({"product_id": pid, "inn": inn, "strength": dose, "unit": ""})
    ingrs = pd.DataFrame(rows, columns=["product_id", "inn", "strength", "unit"])

    # prices.csv пока пустой (ЕСКЛП не содержит предельные цены)
    prices = pd.DataFrame(columns=["product_id", "znvlp_price_rub", "price_date"])

    # Пишем CSV в UTF-8 без BOM, разделитель — запятая
    products.to_csv(OUT_PRODUCTS, index=False)
    ingrs.to_csv(OUT_INGRS, index=False)
    prices.to_csv(OUT_PRICES, index=False)

    print(f"OK: products={len(products)}, ingrs={len(ingrs)}, prices={len(prices)}")


def main():
    zip_bytes = load_zip_bytes()
    build_csvs_from_esklp_zip(zip_bytes)

if __name__ == "__main__":
    main()
