import pandas as pd
import numpy as np
import os
from pathlib import Path
import csv,sqlite3
import prettytable

BASE_PATH= Path(r"D:\Python\data")
DB_PATH= Path(r"D:\Python\data\po_data.db")
print("The DB path is ",DB_PATH)
RAW_PATH= Path(r"D:\Python\data\raw")
print("Working directory ",RAW_PATH)

prettytable.DEFAULT = 'DEFAULT'
ekko_path = RAW_PATH/ "ekko.csv"
ekpo_path = RAW_PATH/ "EKPO.csv"
mseg_path = RAW_PATH/ "MSEG.csv"
eket_path = RAW_PATH/ "EKET.csv"
mara_path = RAW_PATH/ "MARA.csv"
eban_path = RAW_PATH/ "EBAN.csv"
lfa1_path = RAW_PATH/ "LFA1.csv"

ekko= pd.read_csv(ekko_path, dtype= str)
ekpo= pd.read_csv(ekpo_path, dtype= str)
eket= pd.read_csv(eket_path, dtype= str)
mseg= pd.read_csv(mseg_path, dtype= str)
lfa1= pd.read_csv(lfa1_path, dtype= str)

con= sqlite3.connect(DB_PATH)
ekko.to_sql("ekko", con, if_exists='replace', index=False)
ekpo.to_sql("ekpo", con, if_exists='replace', index=False)
eket.to_sql("eket", con, if_exists='replace', index=False)
mseg.to_sql("mseg", con, if_exists='replace', index=False)
lfa1.to_sql("lfa1", con, if_exists='replace', index=False)

cur =con.cursor()
tables= cur.execute("SELECT name FROM sqlite_master WHERE type= 'table' ORDER BY name").fetchall()

for t in tables:
    print("-", t[0])

sql= r""" 
DROP TABLE IF EXISTS po_fact;

CREATE TABLE po_fact AS
WITH gr AS(
   SELECT 
   EBELN ||"_"||printf("%05d", CAST(EBELP AS INTEGER)) AS po_line_id,
   MIN(GR_DATE) AS gr_date,
   SUM(CAST(GR_QTY AS INTEGER)) as gr_qty
   FROM mseg
   WHERE EBELN IS NOT NULL
   GROUP BY EBELN,EBELP
),

sch AS(
   SELECT EBELN, EBELP, MIN(SCHEDULED_DATE) AS scheduled_date
   FROM eket
   WHERE EBELN IS NOT NULL
   GROUP BY EBELN, EBELP
)

SELECT 
  ep.EBELN,
  ep.EBELP,
  (ep.EBELN || '_' || printf('%05d', CAST(ep.EBELP AS INTEGER))) AS po_line_id,
  ep.MATNR,
  ep.ORDER_QTY AS order_qty,
  ep.NETPR AS line_netwr,
  eh.LIFNR AS vendor_id,
  l.NAME1 AS vendor_name,
  ep.WERKS AS plant,
  eh.EKORG AS purchase_org,
  COALESCE(s.scheduled_date, ep.SCH_DATE) AS scheduled_date,
  g.gr_date,
  g.gr_qty,
  eh.AEDAT AS created_date,
  eh.WAERS AS currency

FROM ekpo ep
LEFT JOIN ekko eh ON ep.EBELN= eh.EBELN
LEFT JOIN gr g ON g.po_line_id= (ep.EBELN||'_'||printf("%05d", CAST(ep.EBELP AS INTEGER)))
LEFT JOIN sch s ON ep.EBELN= s.EBELN AND ep.EBELP= s.EBELP
LEFT JOIN lfa1 l ON eh.LIFNR= l.LIFNR
"""

cur.executescript(sql)
con.commit()

count= cur.execute("SELECT COUNT(*) FROM po_fact;").fetchone()[0]
print("po_fact got no:",count)

po_fact= pd.read_sql_query("select * from po_fact",con)
po_fact.to_csv(BASE_PATH/"po_fact.csv", index=False)
print("File exported Happy Day...")
cur.close()
con.close()
