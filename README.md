# California Hospital Cost Benchmark

Replication of the cost benchmarking methodology used by the California Office of Health Care Affordability (OHCA) to identify "high-cost" hospitals.

## Overview

The California Office of Health Care Affordability (OHCA) uses Hospital Annual Disclosure Report (HADR) data to flag hospitals whose costs exceed defined thresholds. This project replicates those calculations from source data, documents the methodology, and validates the results against OHCA's published findings.

## Data

Primary source: California Department of Health Care Access and Information (HCAI) — **Hospital Annual Disclosure Report (HADR)**.

> ⚠️ Raw data files are stored locally and are not tracked by git. See `data_raw/` in `.gitignore`.

## Project Structure

```
ca-hospital-cost-benchmark/
├── scripts/                  # Python/R/Stata analysis scripts
├── notebooks/                # Jupyter notebooks (exploratory analysis)
├── methodology/              # Documentation of OHCA cost benchmark methodology
├── outputs/                  # Figures and tables
├── environment.yml           # Conda environment specification
└── data_raw/                 # ⚠️ Excluded from git (see .gitignore)
```

## Tools & Languages

- **Python** — data ingestion, processing, replication calculations
- **R** — statistical analysis *(planned)*
- **Stata** — econometric analysis *(planned)*

## References

- California OHCA: [hcai.ca.gov/ohca](https://hcai.ca.gov/ohca)
- HADR data: [California HCAI Open Data Portal](https://data.chhs.ca.gov)

## Status

🔧 Work in progress

## Author

Eva Loaeza · [github.com/evaloaeza](https://github.com/evaloaeza)
