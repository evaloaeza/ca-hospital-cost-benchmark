#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 18 09:07:14 2026

@author: eloaeza
"""

from __future__ import annotations

import re
from pathlib import Path
from collections import Counter

import pandas as pd


DATA_DIR = Path("/Users/eloaeza/projects/hadr-project/data_raw/")
OUT_DIR  = Path("/Users/eloaeza/projects/hadr-project/outputs/out_step1_single_sheet/")
OUT_DIR.mkdir(parents=True, exist_ok=True)

SHEET_NAME = "Financial and Utilization Data"   # change to "Cost Allocation Data" if needed


def pcl_token(x) -> str:
    if pd.isna(x):
        return ""
    try:
        f = float(x)
        if f.is_integer():
            return str(int(f))
        return f"{f:.10f}".rstrip("0").rstrip(".")   # keeps 4.1 as "4.1"
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

        out.append(f"P{p_s}_C{c_s}_L{l_s}")
    return out


def make_unique(cols: list[str]) -> list[str]:
    seen = Counter()
    out = []
    for c in cols:
        seen[c] += 1
        out.append(c if seen[c] == 1 else f"{c}__dup{seen[c]:03d}")
    return out


def disclosure_cycle_from_name(path: Path) -> int:
    # 41hospitaldata.xlsx -> 41
    m = re.search(r"(\d+)", path.stem)
    if not m:
        raise ValueError(f"Could not parse disclosure cycle from {path.name}")
    return int(m.group(1))


def process_file(xlsx_path: Path, sheet_name: str, out_dir: Path) -> Path:
    dc = disclosure_cycle_from_name(xlsx_path)
    out_parquet = out_dir / f"fin_util_{dc}.parquet"

    # metadata rows
    header4 = pd.read_excel(
        xlsx_path,
        sheet_name=sheet_name,
        header=None,
        nrows=4,
        engine="openpyxl",
    )

    cols = build_pcl_column_ids(header4)
    cols_u = make_unique(cols)

    # full data (remove nrows=... for full sheet)
    df = pd.read_excel(
        xlsx_path,
        sheet_name=sheet_name,
        header=None,
        skiprows=4,
        engine="openpyxl",
        dtype=object,
    )

    # align column count defensively (in case of occasional mismatch)
    m = min(df.shape[1], len(cols_u))
    df = df.iloc[:, :m]
    df.columns = cols_u[:m]

    df.insert(0, "DISCLOSURE_CYCLE", dc)

    # pyarrow-safe: normalize all object cols to pandas string
    obj_cols = df.select_dtypes(include=["object"]).columns
    df[obj_cols] = df[obj_cols].astype("string")

    df.to_parquet(out_parquet, index=False, engine="pyarrow")
    return out_parquet


def main():
    files = sorted(DATA_DIR.glob("4[1-9]hospitaldata.xlsx"))
    if not files:
        raise FileNotFoundError(f"No files found in {DATA_DIR}")

    for f in files:
        out = process_file(f, SHEET_NAME, OUT_DIR)
        print("Wrote:", out, "size:", out.stat().st_size)


if __name__ == "__main__":
    main()



# Append all files in a single parquet file
import duckdb

FINAL_PARQUET = OUT_DIR / "fin_util_appended.parquet"

con = duckdb.connect()
con.execute(
    f"""
    COPY (
        SELECT * FROM read_parquet('{OUT_DIR.as_posix()}/fin_util_*.parquet')
    )
    TO '{FINAL_PARQUET.as_posix()}'
    (FORMAT PARQUET);
    """
)
con.close()

print("Final appended file:", FINAL_PARQUET)




