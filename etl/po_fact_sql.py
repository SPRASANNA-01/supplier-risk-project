import sqlite3 
import pandas as pd
from pathlib import Path
import os

BASE_PATH = Path(__file__).resolve().parent.parent
DB_PATH= BASE_PATH / "my_data1.db"
sql = r"""
DROP TABLE IF EXISTS po_fact;

CREATE TABLE po_fact AS
WITH gr AS (
    SELECT
      EBELN || '_' || printf('%05d', CAST(EBELP AS INTEGER)) AS po_line_id,
      MIN(GR_DATE) AS gr_date,
      SUM(CAST(GR_QTY AS NUMERIC)) AS gr_qty
    FROM mseg
    WHERE EBELN IS NOT NULL
    GROUP BY EBELN, EBELP
),
sched AS (
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
  v.NAME1 AS vendor_name,
  ep.WERKS AS plant,
  eh.EKORG AS purchase_org,
  COALESCE(s.scheduled_date, ep.SCHEDULED_DATE) AS scheduled_date,
  g.gr_date,
  g.gr_qty,
  eh.AEDAT AS created_date,
  eh.WAERS AS currency
FROM ekpo ep
LEFT JOIN ekko eh ON ep.EBELN = eh.EBELN
LEFT JOIN gr g ON g.po_line_id = (ep.EBELN || '_' || printf('%05d', CAST(ep.EBELP AS INTEGER)))
LEFT JOIN sched s ON ep.EBELN = s.EBELN AND ep.EBELP = s.EBELP
LEFT JOIN lfa1 v ON v.LIFNR = eh.LIFNR;
"""

con = sqlite3.connect(DB_PATH)
cur = con.cursor()
cur.executescript(sql)
con.commit()

# Verify
count = cur.execute("SELECT COUNT(*) FROM po_fact").fetchone()[0]
print("po_fact rows:", count)
sample = cur.execute("SELECT * FROM po_fact LIMIT 5").fetchall()
print("Sample rows (first 5):")
for r in sample:
    print(r)



pd.read_sql("SELECT * FROM po_fact", con).to_csv("po_fact.csv", index=False)

cur.close()
con.close()