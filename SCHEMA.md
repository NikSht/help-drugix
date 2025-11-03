# Help.Drugix — RF Medicines Database (Schema)

## CSV files
- data/products.csv
- data/ingredients.csv
- data/prices.csv
- data/atc.csv
- dictionaries/inn_synonyms.csv
- dictionaries/form_normalization.csv

### products.csv
product_id,trade_name,reg_number,reg_status,dosage_form,form_raw,atc_code,pack,country,holder,manufacturer,instruction_url,ru_registry_url,is_znvlp,updated_at

### ingredients.csv
product_id,inn,inn_raw,strength,unit,per_unit,updated_at

### prices.csv
product_id,pack,znvlp_price_rub,price_date,updated_at

### atc.csv
product_id,atc_code,source,updated_at
