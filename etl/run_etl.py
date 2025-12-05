from pathlib import Path
import os
import pandas as pd
import numpy as np
import csv, sqlite3
import prettytable
from datetime import date

# --------------------------------------------
# Set project root (one level above /etl)
# --------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
os.chdir(BASE_DIR)
print("Working directory:", BASE_DIR)

# --------------------------------------------
# Load config
# --------------------------------------------
#cfg = yaml.safe_load(open(BASE_DIR / "config.yaml"))
#engine = create_engine(cfg["db"]["uri"])
#print("DB URI:", cfg["db"]["uri"])

# --------------------------------------------
# Locate RAW folder
# --------------------------------------------
prettytable.DEFAULT = 'DEFAULT'

DB_PATH= BASE_DIR / "my_data1.db"
con = sqlite3.connect(DB_PATH)


RAW_DIR = BASE_DIR / "raw"
if not RAW_DIR.exists():
    raise FileNotFoundError(f"Raw folder not found: {RAW_DIR}")

# Build safe paths for expected files
EKKO_PATH = RAW_DIR / "ekko.csv"
EKPO_PATH = RAW_DIR / "EKPO.csv"
EKET_PATH = RAW_DIR / "EKET.csv"
MSEG_PATH = RAW_DIR / "MSEG.csv"
LFA1_PATH = RAW_DIR / "LFA1.csv"

ekko= pd.read_csv(EKKO_PATH, dtype= str, parse_dates=['AEDAT'])
ekpo= pd.read_csv(EKPO_PATH, dtype= str)
eket= pd.read_csv(EKET_PATH, dtype= str)
mseg= pd.read_csv(MSEG_PATH, dtype= str)
lfa1= pd.read_csv(LFA1_PATH, dtype= str)

ekko.to_sql('ekko', con, if_exists='replace', index=False)
ekpo.to_sql('ekpo', con, if_exists='replace', index= False)
eket.to_sql('eket', con, if_exists='replace', index= False)
mseg.to_sql('mseg', con, if_exists='replace', index=False)
lfa1.to_sql('lfa1', con, if_exists='replace', index=False)

cur = con.cursor()

tables = cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
print("Tables in DB:")
for t in tables:
    print(" -", t[0].content)

cur.close()
con.close()
print()