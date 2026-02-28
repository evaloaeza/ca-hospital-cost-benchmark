#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 18 09:34:14 2026

@author: eloaeza
"""
import re
import pandas as pd
from pathlib import Path
import pdfplumber
import duckdb

# Get ids for HOSPITALS THAT SUBMIT NON-COMPARABLE REPORTS
PDF = Path("/Users/eloaeza/projects/hadr-project/data_raw/hadrfull-db-documentation-rpe2015-xx.pdf")  # adjust if needed

# PDF pages are 0-indexed: page 8 -> index 7, page 9 -> index 8
PAGE_IDXS = [7, 8]

ids = []
with pdfplumber.open(PDF) as pdf:
    for i in PAGE_IDXS:
        txt = pdf.pages[i].extract_text() or ""
        ids.extend(re.findall(r"\b\d{9}\b", txt))

exclude_ids = sorted(set(ids))

print("n exclude IDs:", len(exclude_ids))
print(exclude_ids[:10])

# optional: save for later joins/filters
df_exclude = pd.DataFrame({"OSHPD_FACILITY_NUMBER": exclude_ids})
out = Path("/Users/eloaeza/projects/hadr-project/outputs/non_comparable_2015_ids.csv")
out.parent.mkdir(parents=True, exist_ok=True)
df_exclude.to_csv(out, index=False)
print("wrote:", out)

#########
# Import case mix data by hospital and year

# read the file
df = pd.read_excel(
    "/Users/eloaeza/projects/hadr-project/data_raw/case-mix-index-d6t7du37/case-mix-index-1996-2024.xlsx",
    dtype={"oshpd_id": "string"}  # or "object"
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

########### Create a dataframe with selected comlumns
# Step 2 — Decide what you want to keep (authoritative list)

PARQUET = "/Users/eloaeza/projects/hadr-project/outputs/out_step1_single_sheet/fin_util_appended.parquet"

# Columns to select from the parquet file: P, C
PAIRS = [
    ("10", "9"), # Summary of Revenue and Costs_Net Costs as Reallocated
    ("10", "11"), # Summary of Revenue and Costs_Gross Revenue
    ("10", "13"), # Summary of Revenue and Costs_Adjustment for Professional Component
    ("12", "1"), # Gross Inpatient Revenue_Medicare - Traditional
    ("12", "2"), # Gross Outpatient Revenue_Medicare - Traditional
    ("12", "3"), # Gross Inpatient Revenue_Medicare - Managed Care
    ("12", "4"), # Gross Outpatient Revenue_Medicare - Managed Care
    ("12", "13"), # Gross Inpatient Revenue_Private - Traditional
    ("12", "14"), # Gross Outpatient Revenue_Private - Traditional
    ("12", "15"), # Gross Inpatient Revenue_Private - Managed Care
    ("12", "16") # Gross Outpatient Revenue_Private - Managed Care
    
]

# regex that matches any L for the selected P,C pairs
pair_patterns = [
    re.compile(rf"^P{p}_C{c}_L\d+$")
    for p, c in PAIRS
]

EXTRA_PCLS = {
    "P0_C1_L2",
    "P0_C1_L3",
    "P0_C1_L36",
    "P0_C1_L37",
}

con = duckdb.connect()
cols = con.execute(
    f"DESCRIBE SELECT * FROM read_parquet('{PARQUET}')"
).fetchdf()["column_name"]

# select all matching PCLs
needed_pcls = [
    c for c in cols
    if any(pat.match(c) for pat in pair_patterns) or c in EXTRA_PCLS
]

con.close()

print("Number of columns selected:", len(needed_pcls))
print("First 10:", needed_pcls[:10])
print("Last 10:", needed_pcls[-10:])

# Build the light dataframe

select_list = ",\n  ".join([f'"{c}"' for c in needed_pcls])

query = f"""
SELECT
  DISCLOSURE_CYCLE,
  OSHPD_FACILITY_NUMBER,
  REPORT_PERIOD_END_DATE,
  {select_list}
FROM read_parquet('{PARQUET}')
"""

df = duckdb.query(query).to_df()

df["OSHPD_FACILITY_NUMBER"].nunique()

df.dtypes

# select columns that start with "P"
p_cols = df.columns[df.columns.str.startswith("P")]

# convert to numeric
p_cols = [
    c for c in df.columns
    if c.startswith("P") and c not in {"P0_C1_L36", "P0_C1_L37"}
]


df.dtypes

# ensure string
df["OSHPD_FACILITY_NUMBER"] = df["OSHPD_FACILITY_NUMBER"].astype("string")

# 2. Get TYPE_HOSP variables
# split: first 3 chars = hospital_type, rest = oshpd_id (zero-padded if needed)
df["hospital_type"] = df["OSHPD_FACILITY_NUMBER"].str[:3]
df["oshpd_id"] = df["OSHPD_FACILITY_NUMBER"].str[3:].str.zfill(6)
df[["oshpd_id", "hospital_type"]].dtypes
df["hospital_type"].nunique()

# 3. Filter to keep only Comparable hospitals
df = df[~df["OSHPD_FACILITY_NUMBER"].isin(exclude_ids)]

df["OSHPD_FACILITY_NUMBER"].nunique()

# 4. Create year variable by using year of the reporting period end date
#df["YEAR_BEGIN"] = pd.to_datetime(df["P0_C1_L36"], errors="coerce").dt.year
df["YEAR_END"] = pd.to_datetime(df["P0_C1_L37"], errors="coerce").dt.year

df[["YEAR_END"]].head()
df[["YEAR_END"]].describe()


df_long[["oshpd_id", "period_year"]].dtypes

df[["OSHPD_FACILITY_NUMBER", "YEAR_END"]].dtypes


# 5. Merge with the case mix dataset on year and hospital
df = df.merge(
    df_long[["oshpd_id", "period_year", "case_mix_index"]],
    left_on=["oshpd_id", "YEAR_END"],
    right_on=["oshpd_id", "period_year"],
    how="left"
)

# 6. Round case mix index to 2 decimals
df["case_mix_index"] = df["case_mix_index"].round(2)


# 7. Remove duplicates
# 7.a. Calculate period length as the difference in days between reporting end date and reporting begin date
df["DAY_PER"] = (
    pd.to_datetime(df["P0_C1_L37"], errors="coerce")
    - pd.to_datetime(df["P0_C1_L36"], errors="coerce")
).dt.days

df["BEGIN_DATE"] = pd.to_datetime(df["P0_C1_L36"], errors="coerce")
df["END_DATE"] = pd.to_datetime(df["P0_C1_L37"], errors="coerce")


# drop by column names
df = df.drop(columns=["P0_C1_L37", "P0_C1_L36", "P0_C1_L2", "period_year"])
df["END_DATE"].dtypes

# Keep relevant years for the analysis
df = df[(df["YEAR_END"] >= 2018) & (df["YEAR_END"] <= 2022)]
df["YEAR_END"].describe()

# 7.b Sort by hospital name, year, period length (descending), and end date (descending)

df = (
    df.sort_values(
        by=["P0_C1_L3", "YEAR_END", "DAY_PER", "END_DATE"],
        ascending=[True, True, False, False]
    )
    # 7.c Keep first record per hospital per year with the longest period or most recent
    .drop_duplicates(subset=["P0_C1_L3", "YEAR_END"], keep="first")
)

# Drop some columns 
df = df.drop(columns=["DISCLOSURE_CYCLE","REPORT_PERIOD_END_DATE", "hospital_type", "DAY_PER", "BEGIN_DATE", "END_DATE"])


# Reshape the data so it is easy to perform the calculations:
id_vars = [
    "OSHPD_FACILITY_NUMBER",
    "P0_C1_L3",      # hospital id (example)
    "oshpd_id",
    "YEAR_END",
    "case_mix_index",
]


# 2) Select only the PCL columns you want to reshape
value_vars = [c for c in df.columns if c.startswith("P")]

# 3) Melt to long
long = df.melt(
    id_vars=id_vars,
    value_vars=value_vars,
    var_name="pcl",
    value_name="value"
)

# 4) Extract Px_Cx and revenue_center (L)
long["px_cx"] = long["pcl"].str.extract(r"^(P\d+_C\d+)")
long["revenue_center"] = (
    long["pcl"]
    .str.extract(r"_L(\d+)")
    .astype("Int64")
)

# make sure long["value"] is numeric
long["value"] = pd.to_numeric(long["value"], errors="coerce")

# Step 1: pivot back just for the calculation
calc = (
    long
    .pivot_table(
        index=["OSHPD_FACILITY_NUMBER", "P0_C1_L3", "YEAR_END", "revenue_center"],
        columns="px_cx",
        values="value",
        aggfunc="first"
    )
    .reset_index()
)

long.dtypes
calc.dtypes

# Step 2: compute Cost-to-Charge Ratio
calc["cost_to_charge_ratio"] = (
    (calc["P10_C9"].fillna(0) + calc["P10_C13"].fillna(0))
    .div(calc["P10_C11"])
)
calc.loc[calc["P10_C11"].isna() | (calc["P10_C11"] == 0), "cost_to_charge_ratio"] = pd.NA

calc["revenue_center"].nunique()

# Not revenue centers
calc = calc[(calc["revenue_center"] <416)]


# Delete no revenue centers (with all nan)
calc = calc.groupby("revenue_center", group_keys=False).filter(
    lambda g: g["cost_to_charge_ratio"].notna().any()
)

#  7.iii. For each revenue center and payer, calculate cost using cost-to-
#charge ratio from previous step multiplied by the sum of revenue
#center revenue from inpatient, outpatient, traditional care and
#managed care payer revenue

calc["medicare_cost_rc"] = (
    (calc["P12_C1"].fillna(0) + calc["P12_C2"].fillna(0)
     + calc["P12_C3"].fillna(0) + calc["P12_C4"].fillna(0))
    * (calc["cost_to_charge_ratio"])
)

calc["private_cost_rc"] = (
    (calc["P12_C13"].fillna(0) + calc["P12_C14"].fillna(0)
     + calc["P12_C15"].fillna(0) + calc["P12_C16"].fillna(0))
    * (calc["cost_to_charge_ratio"])
)


# 7.b Calculate total costs for Medicare and Commercial payers by summing
#total costs across revenue centers by payer 
df_tot = (
    calc.groupby(["OSHPD_FACILITY_NUMBER", "P0_C1_L3", "YEAR_END"], as_index=False)
        .agg(
            tot_medicare=("medicare_cost_rc", lambda s: s.sum(min_count=1)),
            tot_private=("private_cost_rc", lambda s: s.sum(min_count=1)),
        )
)




summary = (
    calc
    .groupby("revenue_center", as_index=False)["cost_to_charge_ratio"]
    .agg(
        mean="mean",
        median="median",
        min = "min",
        max = "max",
        p25=lambda x: x.quantile(0.25),
        p75=lambda x: x.quantile(0.75),
        n="count"
    )
)

check = calc[(calc["revenue_center"] ==250)]



# read the file
df_final = pd.read_csv(
    "/Users/eloaeza/projects/hadr-project/data_raw/Hospital-Cost-Data-Updated-June-2025-1.csv"
)

df_final["cy"].describe()





























#select_list = ",\n  ".join([
 #   f'"{c}" AS "{fin_p2l.get(c, c)}"'
 #   for c in needed_pcls
#])

# Step 1

PAIRS = [
    ("10", "9"),
    ("10", "11"),
    ("10", "13"),
    ("12", "1"),
    ("12", "2"),
    ("12", "3"),
    ("12", "4"),
]

# These stay WIDE (identifiers)
EXTRA_PCLS = ["P0_C1_L36", "P0_C1_L37"]

pair_patterns = [re.compile(rf"^P{p}_C{c}_L\d+$") for p, c in PAIRS]


# Step 2 — discover columns and split them
con = duckdb.connect()
cols = con.execute(
    f"DESCRIBE SELECT * FROM read_parquet('{PARQUET}')"
).fetchdf()["column_name"].tolist()

# columns to unpivot (measures)
MEASURE_PCLS = [
    c for c in cols
    if any(pat.match(c) for pat in pair_patterns)
]

# sanity: make sure extras exist
missing = [c for c in EXTRA_PCLS if c not in cols]
if missing:
    raise ValueError(f"Missing identifier columns: {missing}")

# Step 3 — UNPIVOT only the measure columns

wide_measures = ",\n  ".join([f'"{c}"' for c in MEASURE_PCLS])
in_list = ", ".join([f'"{c}"' for c in MEASURE_PCLS])

wide_ids = ",\n  ".join(
    ["DISCLOSURE_CYCLE", "OSHPD_FACILITY_NUMBER", "REPORT_PERIOD_END_DATE"]
    + [f'"{c}"' for c in EXTRA_PCLS]
)

query = f"""
WITH base AS (
  SELECT
    {wide_ids},
    {wide_measures}
  FROM read_parquet('{PARQUET}')
)
SELECT
  {wide_ids},
  pcl,
  value
FROM base
UNPIVOT (value FOR pcl IN ({in_list}))
"""

# Step 4 — execute and parse P/C/L

df_long = con.execute(query).fetchdf()
con.close()

m = df_long["pcl"].str.extract(
    r"^P(?P<page>\d+(?:\.\d+)?)_C(?P<col>\d+(?:\.\d+)?)_L(?P<line>\d+)$"
)
df_long = pd.concat([df_long, m], axis=1)

df_long["line"] = df_long["line"].astype("Int64")
df_long["pair"] = "P" + df_long["page"] + "_C" + df_long["col"]

