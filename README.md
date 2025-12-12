# HCP Persona Tagging — PySpark Pipeline

**Purpose.**  
This repository contains a PySpark pipeline that ingests HCP (healthcare professional) data (demographics, weekly sales, email activity, calls, and KOL events), engineers behavioral and sales features over rolling windows, computes scores, and assigns personas (High Performer, Growth Potential, At Risk, Low Engagement Stable, Unknown). The pipeline was developed to run in Databricks (Unity Catalog / UC Volume).

## Repository contents
- `hcp_persona_pipeline.py` — Main exported Databricks notebook (PySpark script).  
- `sample_data/` *(optional)* — small sample CSVs for local testing (not included by default).  
- `README.md` — this file.

## How to run on Databricks (short)
1. Upload CSV files to a Unity Catalog volume, e.g.
   `/Volumes/workspace/default/hcp_volume/`:
   - `hcp_demographics.csv`
   - `sales_weekly.csv`
   - `email_activity.csv`
   - `calls.csv`
   - `kol_events.csv`
2. Create or start a Databricks cluster (Runtime 11.x/12.x recommended).
3. Create a new Python notebook and attach it to the cluster.
4. Paste the code from `hcp_persona_pipeline.py` into the notebook OR import the `.py` as a notebook.
5. Update the `BASE` path in the script to your UC volume path (default in script is `/Volumes/workspace/default/hcp_volume`).
6. Run cells sequentially. Outputs are written to the same volume:
   - `hcp_persona_features.parquet`
   - `hcp_persona_features_csv/` (CSV parts)

## Key implementation notes
- Explicit schemas (StructType) are used to avoid parsing errors.
- Sales features use a 24-month window; calls use a 12-month window.
- Small tables (roster) are broadcast to avoid large shuffles.
- Final outputs are written as distributed files (Parquet + CSV parts). Use the provided instructions to create a single downloadable CSV if needed.

## Troubleshooting checklist
- If `Planned_Calls` or `Actual_Calls` appear null, check CSV numeric formatting (e.g., `12.0` may require FloatType or casting).
- If DBFS FileStore is disabled, upload files to a UC volume and update `BASE`.
- If job is slow, tune `spark.sql.shuffle.partitions` and consider `repartition()` or `broadcast()` as in the script.

## License
This project is provided under the MIT License. Use freely; attribution appreciated.

