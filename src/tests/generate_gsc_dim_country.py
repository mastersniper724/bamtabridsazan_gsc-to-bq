import pycountry
import pandas as pd
import os

# بررسی متغیر محیطی برای مسیر خروجی
output_dir = os.getenv("GSC_DIM_COUNTRY_DIR", "./data/raw/gsc")
os.makedirs(output_dir, exist_ok=True)

# ۱. تولید داده‌ها
data = [(c.alpha_2, c.name) for c in pycountry.countries]
df = pd.DataFrame(data, columns=['country_code', 'country_name'])

# ۲. مرتب کردن بر اساس ستون country_name
df = df.sort_values(by='country_name', ascending=True)

# ۳. مسیر خروجی
output_dir = "./data/raw/gsc"
output_file = "gsc_dim_country.csv"
output_path = os.path.join(output_dir, output_file)

# ۴. ایجاد فولدر در صورت وجود نداشتن
os.makedirs(output_dir, exist_ok=True)

# ۵. ذخیره CSV با UTF-8
df.to_csv(output_path, index=False, encoding='utf-8')

print(f"CSV generated at: {output_path}")
