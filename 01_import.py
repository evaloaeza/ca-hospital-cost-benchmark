#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 11 14:35:18 2026

@author: eloaeza
"""

pip install pandas openpyxl pyarrow duckdb


import re
from pathlib import Path
import pandas as pd


PCL_LABELS_XLSX = Path("/mnt/data/hadrfull-db-pcl-labels-2015-20xx.xlsx")

SHEETS = {
    "financial_utilization": "Financial and Utilization Data",
    "cost_allocation": "Cost Allocation Data",
}

def sanitize(s: str) -> str:
    s = str(s).strip()
    s = re.sub(r"[^\w]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s[:120]

def load_pcl_labels_split(labels_xlsx: Path) -> dict[str, dict[str, str]]:
    """
    Returns:
      {
        "financial_utilization": { "P0_C1_L1": "LEGAL_NAME", ... },
        "cost_allocation":      { "P19_C2_L35": "....", ... }
      }
    Uses column 'Worksheet 1 / 2' to split:
      - NaN/blank -> financial/utilization
      - 'Cost Alloc' -> cost allocation
    """
    df = pd.read_excel(labels_xlsx, sheet_name="HADR", engine="openpyxl")

    # Required columns in your file:
    # 'PCL', 'Data File Description', 'Worksheet 1 / 2'
    df["Worksheet 1 / 2"] = df["Worksheet 1 / 2"].astype("object")

    fin = df[df["Worksheet 1 / 2"].isna()]
    ca  = df[df["Worksheet 1 / 2"].astype(str).str.strip().eq("Cost Alloc")]

    def make_map(sub: pd.DataFrame) -> dict[str, str]:
        m = {}
        for pcl, desc in zip(sub["PCL"], sub["Data File Description"]):
            if isinstance(pcl, str) and re.match(r"^P\d+_C\d+_L\d+$", pcl):
                if pd.notna(desc) and str(desc).strip():
                    m[pcl] = sanitize(desc)
        return m

    return {
        "financial_utilization": make_map(fin),
        "cost_allocation": make_map(ca),
    }

# ---- inside your main() after reading labels ----
pcl_maps = load_pcl_labels_split(PCL_LABELS_XLSX)

# ---- inside process_one_sheet(...) AFTER df.columns = cols ----
# key is "financial_utilization" or "cost_allocation"
def apply_pcl_renames(df: pd.DataFrame, key: str) -> pd.DataFrame:
    pcl_to_label = pcl_maps.get(key, {})
    rename_map = {}
    for c in df.columns:
        if re.match(r"^P\d+_C\d+_L\d+$", str(c)) and c in pcl_to_label:
            # keep stable PCL id + add compact label suffix
            rename_map[c] = f"{c}__{pcl_to_label[c]}"
    if rename_map:
        df = df.rename(columns=rename_map)
    return df
