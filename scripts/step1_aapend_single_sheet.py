#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 18 08:13:19 2026

@author: eloaeza
"""

from pathlib import Path
from collections import Counter
import pandas as pd

xlsx_path = Path("/Users/eloaeza/projects/hadr-project/data_raw/41hospitaldata.xlsx")
sheet_name = "Financial and Utilization Data"
out_parquet = Path("/Users/eloaeza/projects/hadr-project/outputs/test_fin_util_41.parquet")

print("Exists?", xlsx_path.exists())
print("Output folder exists?", out_parquet.parent.exists())


# Step 1 — read the 4 metadata rows

header4 = pd.read_excel(
    xlsx_path,
    sheet_name=sheet_name,
    header=None,
    nrows=4,
    engine="openpyxl",
)

print("header4 shape:", header4.shape)
print(header4.iloc[:, :6])  # first 6 columns, rows 1-4


# Step 2 — build PCL columns EXACTLY like you were doing

def pcl_token(x) -> str:
    """Format Excel numeric tokens so 4, 4.0 -> '4' and 4.1 -> '4.1' (keep dot)."""
    if pd.isna(x):
        return ""
    # Excel often gives floats; normalize deterministically
    try:
        f = float(x)
        if f.is_integer():
            return str(int(f))
        # strip trailing zeros while keeping decimal
        s = f"{f:.10f}".rstrip("0").rstrip(".")
        return s
    except Exception:
        return str(x).strip()
    
    
def build_pcl_column_ids(header4: pd.DataFrame) -> list[str]:
    page_row = header4.iloc[0].tolist()
    col_row  = header4.iloc[1].tolist()
    line_row = header4.iloc[2].tolist()

    out = []
    for j in range(len(page_row)):
        if j == 0:
            out.append("OSHPD_FACILITY_NUMBER"); continue
        if j == 1:
            out.append("REPORT_PERIOD_END_DATE"); continue

        p, c, l = page_row[j], col_row[j], line_row[j]
        if pd.isna(p) or pd.isna(c) or pd.isna(l):
            out.append(f"COL_{j:05d}")
            continue

        p_s, c_s, l_s = pcl_token(p), pcl_token(c), pcl_token(l)
        if not (p_s and c_s and l_s):
            out.append(f"COL_{j:05d}")
            continue

        out.append(f"P{p_s}_C{c_s}_L{l_s}")   # e.g., P4.1_C1_L5
    return out

cols = build_pcl_column_ids(header4)
print("n cols:", len(cols))
print(cols[-1]) # Last column 

# Step 3 — prove whether you truly have duplicates (and what they are)
dups = [c for c, n in Counter(cols).items() if n > 1]
print("duplicate colnames count:", len(dups))
print("first 20 duplicates:", dups[:20])

# show how many times the worst offender repeats
if dups:
    worst = max(dups, key=lambda c: Counter(cols)[c])
    print("worst duplicate:", worst, "count:", Counter(cols)[worst])

# Step 4 — make them unique (deterministically) and re-check
def make_unique(cols: list[str]) -> list[str]:
    seen = Counter()
    out = []
    for c in cols:
        seen[c] += 1
        out.append(c if seen[c] == 1 else f"{c}__dup{seen[c]:03d}")
    return out

cols_u = make_unique(cols)

# sanity checks
print("unique?", len(cols_u) == len(set(cols_u)))
print("duplicates after:", len([c for c, n in Counter(cols_u).items() if n > 1]))

# Step 5 — read a SMALL slice of data and assign columns

df = pd.read_excel(
    xlsx_path,
    sheet_name=sheet_name,
    header=None,
    skiprows=4,
    nrows=200,          # small sample
    engine="openpyxl",
    dtype=object,
)

print("df shape:", df.shape)
print("df cols count:", df.shape[1], "expected:", len(cols_u))

df.columns = cols_u
print("df columns unique?", df.columns.is_unique)

# Step 6 — write the sample parquet
out_parquet.parent.mkdir(parents=True, exist_ok=True)

# convert to string anything that was stored as object
obj_cols = df.select_dtypes(include=["object"]).columns
df[obj_cols] = df[obj_cols].astype("string")


df.to_parquet(out_parquet, index=False, engine="pyarrow")
print("Wrote:", out_parquet, "exists:", out_parquet.exists(), "size:", out_parquet.stat().st_size)


# To check with columns are duplicated
dup_counts = Counter(cols)
dups = {c: n for c, n in dup_counts.items() if n > 1}

len(dups)              # how many distinct duplicated names

list(dups.items())[:20]

# 2) See exactly where each duplicate occurs (column positions)
dup_positions = {
    c: [i for i, x in enumerate(cols) if x == c]
    for c, n in dups.items()
}

# example: inspect one problematic column
dup_positions["P4_C1_L5"]


# 3) Verify how they were disambiguated after make_unique
[c for c in cols_u if c.startswith("P4_C1_L5")]


















import pyarrow
print("pyarrow:", pyarrow.__version__)

print("out_parquet:", out_parquet)
print("parent exists:", out_parquet.parent.exists())

print("columns unique:", df.columns.is_unique)
print("any null colnames:", any(c is None for c in df.columns))
print("colname type sample:", type(df.columns[0]), type(df.columns[-1]))

# force pyarrow explicitly
df.to_parquet(out_parquet, index=False, engine="pyarrow")
print("WROTE?", out_parquet.exists(), "size:", out_parquet.stat().st_size if out_parquet.exists() else None)


# identify the column quickly
s = df["P0_C1_L5"]
print(s.map(type).value_counts().head(10))
print(s.head(20))

