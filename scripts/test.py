#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 18 11:10:25 2026

@author: eloaeza
"""


# To convert to long format

con = duckdb.connect()

# --- discover column names ---
cols = con.execute(
    f"DESCRIBE SELECT * FROM read_parquet('{PARQUET}')"
).fetchdf()["column_name"].tolist()

pair_pats = [re.compile(rf"^P{p}_C{c}_L\d+$") for p, c in PAIRS]


pcl_cols = [c for c in cols if any(pat.match(c) for pat in pair_pats) or c in EXTRA_PCLS]

def key_pcl(name: str):
    m = re.match(r"^P(\d+)_C(\d+)_L(\d+)$", name)
    return (int(m.group(1)), int(m.group(2)), int(m.group(3))) if m else (10**9, 10**9, 10**9)


pcl_cols = sorted(pcl_cols, key=key_pcl)

if not pcl_cols:
    raise ValueError("No PCL columns matched your PAIRS. Check PAIRS and column naming in the parquet.")

# --- build query ---
wide_select = ",\n  ".join([f'"{c}"' for c in pcl_cols])
in_list = ", ".join([f'"{c}"' for c in pcl_cols])

query = f"""
WITH base AS (
  SELECT
    DISCLOSURE_CYCLE,
    OSHPD_FACILITY_NUMBER,
    REPORT_PERIOD_END_DATE,
    {wide_select}
  FROM read_parquet('{PARQUET}')
)
SELECT
  DISCLOSURE_CYCLE,
  OSHPD_FACILITY_NUMBER,
  REPORT_PERIOD_END_DATE,
  pcl,
  value
FROM base
UNPIVOT (value FOR pcl IN ({in_list}))
"""

df_long = con.execute(query).fetchdf()
con.close()

# --- parse P/C/L into fields ---
m = df_long["pcl"].str.extract(r"^P(?P<page>\d+)_C(?P<col>\d+)_L(?P<line>\d+)$")
df_long = pd.concat([df_long, m], axis=1)
df_long[["page", "col", "line"]] = df_long[["page", "col", "line"]].astype("Int64")
df_long["pair"] = "P" + df_long["page"].astype(str) + "_C" + df_long["col"].astype(str)

print(df_long.head())