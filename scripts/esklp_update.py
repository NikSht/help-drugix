#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
from datetime import datetime
import pandas as pd

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

for name, cols in [
    ("products.csv",    ["product_id","trade_name","dosage_form","pack","country","is_znvlp","atc_code","ru_registry_url","instruction_url"]),
    ("ingredients.csv", ["product_id","inn","strength","unit"]),
    ("prices.csv",      ["product_id","znvlp_price_rub","price_date"]),
]:
    path = os.path.join(DATA_DIR, name)
    if not os.path.exists(path):
        pd.DataFrame(columns=cols).to_csv(path, index=False)

with open(os.path.join(DATA_DIR, "version.txt"), "w", encoding="utf-8") as f:
    f.write(datetime.utcnow().isoformat())

print("OK: placeholder update done")
