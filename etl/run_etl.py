from pathlib import Path
import os
import pandas as pd
import numpy as np
import yaml
from sqlalchemy import create_engine
from datetime import date
# --------------------
# Project root + config
# --------------------
BASE_DIR = Path(__file__).resolve().parent.parent
os.chdir(BASE_DIR)
print('Working dir:', BASE_DIR)
cfg = yaml.safe_load(open(BASE_DIR / 'config.yaml'))
engine = create_engine(cfg['db']['uri'])
print('DB URI:', cfg['db']['uri'])
# --------------------
# Raw files discovery (case-insensitive)
# --------------------
RAW_DIR = BASE_DIR / (cfg.get('paths', {}).get('raw_dir', 'raw'))
if not RAW_DIR.exists():
raise FileNotFoundError(f'Raw folder not found: {RAW_DIR}')
expected = { 'EKKO': None, 'EKPO': None, 'EKET': None, 'MSEG': None, 'LFA1':
None }
for f in RAW_DIR.iterdir():
if not f.is_file():
continue
key = f.stem.upper()
if key in expected:
expected[key] = f
print('Discovered raw files:')
for k,v in expected.items():
print(f' {k}:', v.name if v else 'MISSING')
missing = [k for k,v in expected.items() if k in ('EKKO','EKPO','MSEG','LFA1')
3
and v is None]
if missing:
raise FileNotFoundError(f'Missing required files: {missing}. Place them in
{RAW_DIR}')
# --------------------
# Load CSVs
# --------------------
ekko = pd.read_csv(expected['EKKO'], dtype=str, parse_dates=[col for col in
['AEDAT'] if col in pd.read_csv(expected['EKKO'], nrows=0).columns],
low_memory=False)
ekpo = pd.read_csv(expected['EKPO'], dtype=str, low_memory=False)
eket = pd.read_csv(expected['EKET'], dtype=str, low_memory=False) if
expected.get('EKET') else pd.DataFrame()
mseg = pd.read_csv(expected['MSEG'], dtype=str, low_memory=False)
lfa1 = pd.read_csv(expected['LFA1'], dtype=str, low_memory=False)
print('Loaded shapes:', ekko.shape, ekpo.shape, eket.shape, mseg.shape,
lfa1.shape)
# --------------------
# Normalize + transform
# --------------------
# common name handling
# coerce numeric
if 'ORDER_QTY' not in ekpo.columns and 'MENGE' in ekpo.columns:
ekpo['ORDER_QTY'] = ekpo['MENGE']
if 'LINE_NETWR' not in ekpo.columns and 'NETWR' in ekpo.columns:
ekpo['LINE_NETWR'] = ekpo['NETWR']
ekpo['ORDER_QTY'] = pd.to_numeric(ekpo.get('ORDER_QTY'), errors='coerce')
ekpo['LINE_NETWR'] = pd.to_numeric(ekpo.get('LINE_NETWR'), errors='coerce')
mseg['GR_QTY'] = pd.to_numeric(mseg.get('GR_QTY', mseg.get('MENGE')),
errors='coerce')
# normalize EBELP to 5-digit string if present
if 'EBELP' in ekpo.columns:
ekpo['EBELP'] = ekpo['EBELP'].astype(str).str.zfill(5)
if 'EBELP' in mseg.columns:
mseg['EBELP'] = mseg['EBELP'].astype(str).str.zfill(5)
# PO_LINE_ID
ekpo['PO_LINE_ID'] = ekpo['EBELN'].astype(str) + '_' + ekpo['EBELP'].astype(str)
if 'EBELN' in mseg.columns and 'EBELP' in mseg.columns:
mseg['PO_LINE_ID'] = mseg['EBELN'].astype(str) + '_' +
mseg['EBELP'].astype(str)
# aggregate GR
4
if 'PO_LINE_ID' in mseg.columns:
gr_agg = mseg.groupby('PO_LINE_ID').agg(GR_DATE=('GR_DATE','min'),
GR_QTY=('GR_QTY','sum')).reset_index()
else:
gr_agg = pd.DataFrame(columns=['PO_LINE_ID','GR_DATE','GR_QTY'])
# merge header
if 'EBELN' not in ekko.columns:
raise KeyError('EKKO must contain EBELN')
po =
ekpo.merge(ekko[['EBELN','LIFNR','AEDAT','WAERS','NETWR','EKORG']].drop_duplicates(['EBELN']),
on='EBELN', how='left')
po = po.merge(gr_agg, on='PO_LINE_ID', how='left')
if 'LIFNR' in lfa1.columns:
po = po.merge(lfa1[['LIFNR','NAME1']].drop_duplicates(['LIFNR']),
on='LIFNR', how='left')
else:
po['NAME1'] = None
# schedule precedence
if not eket.empty and {'EBELN','EBELP','SCHEDULED_DATE'}.issubset(eket.columns):
eket_agg =
eket.groupby(['EBELN','EBELP']).agg(SCHEDULED_DATE=('SCHEDULED_DATE','min')).reset_index()
po = po.merge(eket_agg, on=['EBELN','EBELP'], how='left',
suffixes=('','_eket'))
po['SCHEDULED_DATE'] =
pd.to_datetime(po['SCHEDULED_DATE_eket'].fillna(po.get('SCHEDULED_DATE')),
errors='coerce')
po.drop(columns=[c for c in po.columns if c.endswith('_eket')],
inplace=True)
else:
po['SCHEDULED_DATE'] = pd.to_datetime(po.get('SCHEDULED_DATE'),
errors='coerce')
# parse dates
po['GR_DATE'] = pd.to_datetime(po.get('GR_DATE'), errors='coerce')
po['AEDAT'] = pd.to_datetime(po.get('AEDAT'), errors='coerce')
# on_time logic
po['ON_TIME'] = ((po['GR_DATE'].notna()) & (po['SCHEDULED_DATE'].notna()) &
(po['GR_DATE'] <= po['SCHEDULED_DATE']) & (po['GR_QTY'].fillna(0) >=
po['ORDER_QTY'].fillna(0))).astype(int)
# mark open/late
today = pd.to_datetime(date.today())
po.loc[po['GR_DATE'].isna() & po['SCHEDULED_DATE'].notna() &
(po['SCHEDULED_DATE'] < today), 'ON_TIME'] = 0
5
# age
po['AGE_DAYS'] = (today - po['SCHEDULED_DATE']).dt.days
po.loc[po['GR_DATE'].notna(), 'AGE_DAYS'] = (po['GR_DATE'] -
po['SCHEDULED_DATE']).dt.days
# po_value
po['PO_VALUE'] = pd.to_numeric(po.get('LINE_NETWR'),
errors='coerce').fillna(pd.to_numeric(po.get('NETWR'), errors='coerce'))
# final columns
final_cols = [
('PO_LINE_ID','po_line_id'),('EBELN','ebeln'),('EBELP','ebelp'),
('MATNR','matnr'),('LIFNR','vendor_id'),('NAME1','vendor_name'),
('WERKS','plant'),('EKORG','purchase_org'),('ORDER_QTY','order_qty'),
('LINE_NETWR','line_netwr'),('PO_VALUE','po_value'),
('SCHEDULED_DATE','scheduled_date'),('GR_DATE','gr_date'),('GR_QTY','gr_qty'),
('ON_TIME','on_time'),('AEDAT','created_date'),('WAERS','currency'),
('AGE_DAYS','age_days')
]
po_fact = pd.DataFrame()
for src,dst in final_cols:
po_fact[dst] = po.get(src)
po_fact = po_fact.drop_duplicates(subset=['po_line_id'])
# write
po_fact.to_sql('po_fact', engine, if_exists='replace', index=False)
print('Wrote po_fact rows:', len(po_fact))
