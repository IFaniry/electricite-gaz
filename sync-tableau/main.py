#!/usr/bin/env python3
"""
Sync Tableau: Export Energy Constellation Data Warehouse to Google Sheets using Polars.

Entrypoint script for running either locally or in Google Cloud Run + Scheduler.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

import polars as pl
from dotenv import load_dotenv

from export_models import (
    build_daily_energy_analytics_lf,
    build_annual_conso_secteur_lf,
    build_segmented_linky_inf36_lf,
)
from sheets_client import GoogleSheetsSyncClient

# Configure clean logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("sync-tableau")


def locate_data_dir(cli_dir: str | None) -> Path:
    """Find the brainstorm-data directory locally or in container."""
    candidates = [
        Path(cli_dir) if cli_dir else None,
        Path("brainstorm-data"),
        Path("../brainstorm-data"),
        Path("/app/brainstorm-data"),
    ]
    for c in candidates:
        if c and c.is_dir() and (c / "eco2mix-regional-tr.csv").exists():
            return c.resolve()

    raise FileNotFoundError(
        "Could not locate 'brainstorm-data' directory containing 'eco2mix-regional-tr.csv'. "
        "Please specify with --data-dir <path>."
    )


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Export Energy Constellation Data Warehouse to Google Sheets for Tableau using Polars."
    )
    parser.add_argument(
        "--spreadsheet-id",
        type=str,
        default=os.getenv("SPREADSHEET_ID"),
        help="Target Google Spreadsheet ID (or set SPREADSHEET_ID env var).",
    )
    parser.add_argument(
        "--credentials",
        type=str,
        default=os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
        help="Path to Google Service Account JSON key (or set GOOGLE_APPLICATION_CREDENTIALS).",
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default=None,
        help="Path to brainstorm-data/ folder (auto-detected by default).",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=2020,
        help="Starting year for data export (default: 2020).",
    )
    parser.add_argument(
        "--mode",
        choices=["all", "daily", "annual", "segmented"],
        default="all",
        help="Export scope: 'all' (all 3 sheets), 'daily' (flat master table only), 'annual', or 'segmented'.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Process data with Polars and print summaries without uploading to Google Sheets.",
    )
    parser.add_argument(
        "--export-csv",
        type=str,
        default=None,
        help="Optional directory to save exported tables as local CSV files.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=5000,
        help="Number of rows per Google Sheets API batch update (default: 5000).",
    )
    return parser.parse_args()


def main() -> int:
    """Main execution orchestrator."""
    load_dotenv()
    args = parse_args()

    start_time = time.perf_counter()
    logger.info("==================================================================")
    logger.info("Starting Sync Tableau (Energy Data Warehouse -> Google Sheets)")
    logger.info("Start Year: %d | Mode: %s | Dry Run: %s", args.start_year, args.mode, args.dry_run)
    logger.info("==================================================================")

    data_dir = locate_data_dir(args.data_dir)
    logger.info("Using raw data directory: %s", data_dir)

    # 1. Build Polars Models
    datasets: dict[str, pl.DataFrame] = {}

    if args.mode in ("all", "daily"):
        logger.info("Building 'Daily_Energy_Analytics' LazyFrame...")
        t0 = time.perf_counter()
        df_daily = build_daily_energy_analytics_lf(data_dir, start_year=args.start_year).collect()
        t1 = time.perf_counter()
        logger.info("Collected 'Daily_Energy_Analytics': %d rows, %d cols in %.2fs", len(df_daily), len(df_daily.columns), t1 - t0)
        datasets["Daily_Energy_Analytics"] = df_daily

    if args.mode in ("all", "annual"):
        logger.info("Building 'Annual_Conso_Secteur' LazyFrame...")
        t0 = time.perf_counter()
        df_annual = build_annual_conso_secteur_lf(data_dir, start_year=args.start_year).collect()
        t1 = time.perf_counter()
        logger.info("Collected 'Annual_Conso_Secteur': %d rows, %d cols in %.2fs", len(df_annual), len(df_annual.columns), t1 - t0)
        datasets["Annual_Conso_Secteur"] = df_annual

    if args.mode in ("all", "segmented"):
        logger.info("Building 'Segmented_Linky_Inf36' LazyFrame...")
        t0 = time.perf_counter()
        df_seg = build_segmented_linky_inf36_lf(data_dir, start_year=args.start_year).collect()
        t1 = time.perf_counter()
        logger.info("Collected 'Segmented_Linky_Inf36': %d rows, %d cols in %.2fs", len(df_seg), len(df_seg.columns), t1 - t0)
        datasets["Segmented_Linky_Inf36"] = df_seg

    # 2. Local CSV Export (if requested)
    if args.export_csv:
        csv_out_dir = Path(args.export_csv)
        csv_out_dir.mkdir(parents=True, exist_ok=True)
        for name, df in datasets.items():
            csv_path = csv_out_dir / f"{name}.csv"
            df.write_csv(csv_path, separator=";")
            logger.info("Saved local CSV: %s (%d rows)", csv_path, len(df))

    # 3. Dry-Run early exit
    if args.dry_run:
        logger.info("\n--- DRY-RUN SUMMARY ---")
        total_cells = 0
        for name, df in datasets.items():
            cells = len(df) * len(df.columns)
            total_cells += cells
            print(f"\n[Sheet: {name}]")
            print(f"  Shape: {df.shape[0]:,} rows x {df.shape[1]} columns = {cells:,} cells")
            print(f"  Memory footprint: {df.estimated_size() / (1024 * 1024):.2f} MB")
            print(df.head(3))
        print(f"\nTotal cells across all sheets: {total_cells:,} / 10,000,000 max allowed by Google Sheets.")
        logger.info("Dry-run completed successfully in %.2fs.", time.perf_counter() - start_time)
        return 0

    # 4. Check Google Sheets Parameters
    if not args.spreadsheet_id:
        logger.error(
            "Target Google Spreadsheet ID is missing! Specify via --spreadsheet-id <ID> "
            "or set SPREADSHEET_ID in environment/.env file."
        )
        return 1

    # 5. Connect and Upload to Google Sheets
    logger.info("Connecting to Google Sheets API...")
    try:
        client = GoogleSheetsSyncClient(
            spreadsheet_id=args.spreadsheet_id,
            credentials_path=args.credentials,
        )
    except Exception as e:
        logger.error("Authentication or connection failed: %s", e)
        return 1

    total_uploaded_rows = 0
    for name, df in datasets.items():
        uploaded = client.upload_polars_dataframe(
            sheet_title=name,
            df=df,
            chunk_size=args.chunk_size,
        )
        total_uploaded_rows += uploaded

    elapsed = time.perf_counter() - start_time
    logger.info("==================================================================")
    logger.info("SUCCESS: All sheets synced to Google Sheets in %.2fs!", elapsed)
    logger.info("Total rows uploaded: %d", total_uploaded_rows)
    logger.info("Tableau can now refresh from Google Sheets ID: %s", args.spreadsheet_id)
    logger.info("==================================================================")
    return 0


if __name__ == "__main__":
    sys.exit(main())
