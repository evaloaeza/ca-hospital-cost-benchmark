#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 18 07:42:25 2026

@author: eloaeza
"""

from __future__ import annotations
from collections import Counter
import re
from pathlib import Path

import duckdb
import pandas as pd


DATA_DIR = Path("/Users/eloaeza/projects/hadr-project/data_raw/")  # folder with 41hospitaldata.xlsx ... 49hospitaldata.xlsx
FILES = sorted(DATA_DIR.glob("4[1-9]hospitaldata.xlsx"))

OUT_DIR = Path("/Users/eloaeza/projects/hadr-project/outputs/out_step1")
OUT_DIR.mkdir(exist_ok=True)
YEAR_PARQUET_DIR = OUT_DIR / "year_parquet"
YEAR_PARQUET_DIR.mkdir(exist_ok=True)

SHEETS = {
    "financial_utilization": "Financial and Utilization Data",
    "cost_allocation": "Cost Allocation Data",
}

FINAL = {
    "financial_utilization": OUT_DIR / "financial_utilization_appended.parquet",
    "cost_allocation": OUT_DIR / "cost_allocation_appended.parquet",
}


def build_pcl_column_ids(header4: pd.DataFrame) -> list[str]:
    page_row = header4.iloc[0].tolist()
    col_row = header4.iloc[1].tolist()
    line_row = header4.iloc[2].tolist()

    out = []
    for j in range(len(page_row)):
        if j == 0:
            out.append("OSHPD_FACILITY_NUMBER")
            continue
        if j == 1:
            out.append("REPORT_PERIOD_END_DATE")
            continue

        p, c, l = page_row[j], col_row[j], line_row[j]
        if pd.isna(p) or pd.isna(c) or pd.isna(l):
            out.append(f"COL_{j:05d}")
            continue

        try:
            out.append(f"P{int(float(p))}_C{int(float(c))}_L{int(float(l))}")
        except Exception:
            out.append(f"COL_{j:05d}")
    return out


def make_unique(cols: list[str]) -> list[str]:
    seen = Counter()
    out = []
    for c in cols:
        seen[c] += 1
        out.append(c if seen[c] == 1 else f"{c}__dup{seen[c]:03d}")
    return out


def process_one_sheet(xlsx_path: Path, sheet_name: str, disclosure_cycle: int, out_parquet: Path) -> None:
    header4 = pd.read_excel(
        xlsx_path,
        sheet_name=sheet_name,
        header=None,
        nrows=4,
        engine="openpyxl",
    )
    cols = build_pcl_column_ids(header4)

    df = pd.read_excel(
        xlsx_path,
        sheet_name=sheet_name,
        header=None,
        skiprows=4,          # data starts after 4 metadata rows
        engine="openpyxl",
        dtype=object,        # keep memory down
    )
    df.columns = cols
    df.insert(0, "DISCLOSURE_CYCLE", disclosure_cycle)

    df.to_parquet(out_parquet, index=False)
    del df


def append_parquets(glob_path: str, out_file: Path) -> None:
    con = duckdb.connect()
    con.execute("PRAGMA threads=4;")
    con.execute(
        f"""
        COPY (
            SELECT * FROM read_parquet('{glob_path}')
        )
        TO '{out_file.as_posix()}'
        (FORMAT PARQUET);
        """
    )
    con.close()


def main():
    if not FILES:
        raise FileNotFoundError("No files matched 41hospitaldata.xlsx ... 49hospitaldata.xlsx in DATA_DIR.")

    # write year-level parquet files per sheet
    sheet_year_files = {k: [] for k in SHEETS}
    for xlsx_path in FILES:
        # disclosure cycle from filename: 41..49
        disclosure_cycle = int(re.findall(r"(\d+)", xlsx_path.stem)[0])

        for key, sheet in SHEETS.items():
            out_parquet = YEAR_PARQUET_DIR / f"{key}_{disclosure_cycle}.parquet"
            process_one_sheet(xlsx_path, sheet, disclosure_cycle, out_parquet)
            sheet_year_files[key].append(out_parquet)
            print(f"Wrote {out_parquet}")

    # append per sheet
    for key in SHEETS:
        append_parquets((YEAR_PARQUET_DIR / f"{key}_*.parquet").as_posix(), FINAL[key])
        print(f"Final appended: {FINAL[key]}")


if __name__ == "__main__":
    main()



#########


PCL_LABELS_XLSX = Path("/Users/eloaeza/projects/hadr-project/data_raw/hadrfull-db-pcl-labels-2015-20xx.xlsx")

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
