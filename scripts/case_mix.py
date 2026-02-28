#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 18 12:14:43 2026

@author: eloaeza
"""
import pandas as pd
import re
from pathlib import Path

# read the file
df = pd.read_excel(
    "/Users/eloaeza/projects/hadr-project/data_raw/case-mix-index-d6t7du37/case-mix-index-1996-2024.xlsx"
)

ID_COLS = ["county", "oshpd_id", "hospital"]

# identify value columns (FYxxxx or CYxxxx)
value_cols = [c for c in df.columns if re.match(r"^(FY|CY)\d{4}$", str(c))]

# reshape to long
df_long = df.melt(
    id_vars=ID_COLS,
    value_vars=value_cols,
    var_name="period",
    value_name="case_mix_index",
)

# extract type (FY / CY) and numeric year
df_long["period_type"] = df_long["period"].str[:2]   # FY or CY
df_long["period_year"] = df_long["period"].str[2:].astype(int)

# optional: drop original period string
df_long = df_long.drop(columns="period")

df_long.head()

# Sanity checks
df_long.head()
df_long["period_type"].value_counts()
(df_long["period_year"].min(), df_long["period_year"].max())

df_long = df_long[df_long["period_year"] > 2014]
df_long = df_long.reset_index(drop=True)


out = Path("/Users/eloaeza/projects/hadr-project/outputs/case_mix_data.csv")
out.parent.mkdir(parents=True, exist_ok=True)
df_long.to_csv(out, index=False)
print("wrote:", out)