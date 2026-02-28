#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 18 12:04:15 2026

@author: eloaeza
"""

import re
import pandas as pd
from pathlib import Path

######## Get columns labels
PCL_LABELS_XLSX = Path("/Users/eloaeza/projects/hadr-project/data_raw/hadrfull-db-pcl-labels-2015-20xx.xlsx")

def pcl_token(x) -> str:
    if pd.isna(x):
        return ""
    try:
        f = float(x)
        if f.is_integer():
            return str(int(f))
        return f"{f:.10f}".rstrip("0").rstrip(".")  # keeps 3.3, 4.1, etc.
    except Exception:
        return str(x).strip()

def sanitize(s: str) -> str:
    s = str(s).strip()
    s = re.sub(r"[^\w]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s[:120]

def load_pcl_labels_split_canon(labels_xlsx: Path) -> dict[str, dict[str, str]]:
    df = pd.read_excel(labels_xlsx, sheet_name="HADR", engine="openpyxl")

    # Build canonical PCL using Page/Col/Line (preserves decimals)
    df["PCL_CANON"] = (
        "P" + df["Page"].map(pcl_token)
        + "_C" + df["Col"].map(pcl_token)
        + "_L" + df["Line"].map(pcl_token)
    )

    ws = df["Worksheet 1 / 2"].astype("object")
    fin = df[ws.isna()]
    ca  = df[ws.astype(str).str.strip().eq("Cost Alloc")]

    def make_map(sub: pd.DataFrame) -> dict[str, str]:
        m = {}
        for pcl, desc in zip(sub["PCL_CANON"], sub["Data File Description"]):
            if isinstance(pcl, str) and pcl.startswith("P") and pd.notna(desc) and str(desc).strip():
                m[pcl] = sanitize(desc)
        return m

    return {
        "financial_utilization": make_map(fin),
        "cost_allocation": make_map(ca),
    }


# Step 1 — Load canonical label maps
maps = load_pcl_labels_split_canon(PCL_LABELS_XLSX)
maps["financial_utilization"].get("P3.3_C5_L40")


fin_pcl_to_label = maps["financial_utilization"]

maps = load_pcl_labels_split_canon(PCL_LABELS_XLSX)
fin_p2l = maps["financial_utilization"]



# keep only P10_C11 variables
p10_c11_dict = {
    k: v
    for k, v in fin_pcl_to_label.items()
    if k.startswith("P10_C11_")
}

# view as a DataFrame (nice for inspection/export)
rev_center_dict = (
    pd.DataFrame.from_dict(fin_pcl_to_label, orient="index", columns=["label"])
    .reset_index(names="pcl")
    .query("pcl.str.startswith('P12_C3_')", engine="python")
)

# extract revenue center text after 'SUMM_GR_REV_'
rev_center_dict["revenue_center"] = (
    rev_center_dict["label"]
    .str.extract(r"^REV_IP_MCARE_MC_(.+)$")[0]
)

rev_center_dict


# view as a DataFrame (nice for inspection/export)
rev_center_dict = (
    pd.DataFrame.from_dict(fin_pcl_to_label, orient="index", columns=["label"])
    .reset_index(names="pcl")
    .query("pcl.str.startswith('P12_C2_')", engine="python")
)

# extract revenue center text after 'SUMM_GR_REV_'
rev_center_dict["revenue_center"] = (
    rev_center_dict["label"]
    .str.extract(r"^REV_IP_MCARE_MC_(.+)$")[0]
)

rev_center_dict

