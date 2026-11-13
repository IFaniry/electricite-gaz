#!/usr/bin/env python3
"""
Google Sheets API Client for Tableau Data Warehouse Sync.

Handles authentication (Service Account file, JSON env var, or Cloud Run ADC),
worksheet management, bulk chunked uploads, and rate-limit resilience.
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
import time
from typing import Any

import google.auth
from google.oauth2 import service_account
import gspread
import polars as pl

logger = logging.getLogger("sync-tableau")

# Scopes required for Google Drive & Google Sheets API
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


class GoogleSheetsSyncClient:
    """High-level client to upload Polars DataFrames into Google Sheets for Tableau."""

    def __init__(
        self,
        spreadsheet_id: str,
        credentials_path: str | None = None,
        credentials_json: str | None = None,
    ):
        self.spreadsheet_id = self._extract_spreadsheet_id(spreadsheet_id)
        self.client = self._authenticate(credentials_path, credentials_json)
        self.spreadsheet = self._open_spreadsheet()

    @staticmethod
    def _extract_spreadsheet_id(raw_id: str) -> str:
        """Extract the 44-character ID from a full Google Sheets URL or raw ID."""
        cleaned = raw_id.strip().strip("'\"")
        match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", cleaned)
        if match:
            return match.group(1)
        return cleaned

    def _authenticate(
        self,
        credentials_path: str | None,
        credentials_json: str | None,
    ) -> gspread.Client:
        """
        Authenticate using one of three strategies:
          1. Direct JSON key file path (local development)
          2. Raw JSON string from env var (Cloud Run / CI secret)
          3. Google Application Default Credentials (ADC - Cloud Run native)
        """
        # 1. From JSON string in environment variable
        raw_json = credentials_json or os.getenv("GCP_SA_KEY") or os.getenv("GOOGLE_CREDENTIALS_JSON")
        if raw_json:
            try:
                info = json.loads(raw_json)
                creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
                logger.info("Authenticated with Google API using Service Account JSON from environment.")
                return gspread.authorize(creds)
            except Exception as e:
                logger.warning("Failed parsing credentials JSON string: %s. Falling back to file/ADC.", e)

        # 2. From file path
        path = credentials_path or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if path and os.path.isfile(path):
            creds = service_account.Credentials.from_service_account_file(path, scopes=SCOPES)
            logger.info("Authenticated with Google API using Service Account file: %s", path)
            return gspread.authorize(creds)

        # 3. Fallback to Application Default Credentials (ADC) for Google Cloud Run
        try:
            creds, _ = google.auth.default(scopes=SCOPES)
            logger.info("Authenticated with Google API using Application Default Credentials (ADC).")
            return gspread.authorize(creds)
        except Exception as e:
            raise RuntimeError(
                "Unable to authenticate to Google Sheets. "
                "Please provide a Service Account JSON file via --credentials, "
                "set GOOGLE_APPLICATION_CREDENTIALS or GCP_SA_KEY, "
                "or run in a GCP environment with Application Default Credentials."
            ) from e

    def _open_spreadsheet(self) -> gspread.Spreadsheet:
        """Open target spreadsheet by key with retry."""
        try:
            sh = self.client.open_by_key(self.spreadsheet_id)
            logger.info("Opened Google Spreadsheet: '%s' (ID: %s)", sh.title, self.spreadsheet_id)
            return sh
        except Exception as e:
            raise RuntimeError(
                f"Failed to open spreadsheet with ID '{self.spreadsheet_id}'. "
                f"Ensure the Service Account has 'Editor' access to the Google Sheet. Error: {e}"
            ) from e

    def get_or_create_worksheet(self, title: str) -> gspread.Worksheet:
        """Retrieve existing worksheet by title or create a new one."""
        try:
            ws = self.spreadsheet.worksheet(title)
            logger.debug("Found existing worksheet '%s'", title)
            return ws
        except gspread.WorksheetNotFound:
            logger.info("Worksheet '%s' not found. Creating it...", title)
            ws = self.spreadsheet.add_worksheet(title=title, rows=100, cols=30)
            return ws

    @staticmethod
    def _serialize_cell(val: Any) -> Any:
        """Format Python/Polars types safely for Google Sheets JSON API."""
        if val is None:
            return ""
        if isinstance(val, float):
            if math.isnan(val) or math.isinf(val):
                return ""
            return val
        if hasattr(val, "isoformat"):
            return val.isoformat()
        if isinstance(val, bool):
            return "TRUE" if val else "FALSE"
        return str(val) if not isinstance(val, (int, float)) else val

    def upload_polars_dataframe(
        self,
        sheet_title: str,
        df: pl.DataFrame,
        chunk_size: int = 5000,
        pause_between_chunks: float = 1.0,
    ) -> int:
        """
        Upload a Polars DataFrame into a Google Sheet tab:
          - Replaces existing data.
          - Resizes the sheet to match exact rows & columns.
          - Chunks writes to respect Google API payloads and rate quotas.
          - Freezes the first header row for Tableau.
        """
        num_rows = len(df)
        num_cols = len(df.columns)
        logger.info(
            "Uploading sheet '%s': %d rows, %d columns (%d cells)...",
            sheet_title,
            num_rows,
            num_cols,
            num_rows * num_cols,
        )

        ws = self.get_or_create_worksheet(sheet_title)

        # Clear existing sheet content
        ws.clear()

        # Ensure sheet dimensions are sufficient
        target_rows = max(num_rows + 1, 10)
        target_cols = max(num_cols, 1)
        try:
            ws.resize(rows=target_rows, cols=target_cols)
        except Exception as e:
            logger.warning("Could not resize worksheet '%s': %s", sheet_title, e)

        # Build headers
        headers = [str(c) for c in df.columns]

        # Convert rows with safe serialization
        # Iterating polars rows is fast and memory-efficient
        all_rows: list[list[Any]] = [headers]
        for row in df.iter_rows():
            all_rows.append([self._serialize_cell(cell) for cell in row])

        total_lines = len(all_rows)

        # Chunked upload to avoid exceeding Google Sheets request size limits
        for i in range(0, total_lines, chunk_size):
            chunk = all_rows[i : i + chunk_size]
            start_row = i + 1
            end_row = i + len(chunk)
            range_name = f"A{start_row}"

            # Exponential backoff retry loop for API rate limits (HTTP 429 / 503)
            max_retries = 5
            for attempt in range(1, max_retries + 1):
                try:
                    ws.update(
                        range_name=range_name,
                        values=chunk,
                        value_input_option="USER_ENTERED",
                    )
                    break
                except Exception as ex:
                    if attempt == max_retries:
                        raise RuntimeError(
                            f"Failed uploading rows {start_row}-{end_row} to '{sheet_title}' after {max_retries} attempts: {ex}"
                        ) from ex
                    sleep_time = 2**attempt
                    logger.warning(
                        "Google API error on '%s' (rows %d-%d): %s. Retrying in %ds (attempt %d/%d)...",
                        sheet_title,
                        start_row,
                        end_row,
                        ex,
                        sleep_time,
                        attempt,
                        max_retries,
                    )
                    time.sleep(sleep_time)

            logger.info("Uploaded rows %d to %d / %d to '%s'", start_row, end_row, total_lines, sheet_title)
            if pause_between_chunks > 0 and end_row < total_lines:
                time.sleep(pause_between_chunks)

        # Freeze the top header row for a clean Tableau presentation
        try:
            ws.freeze(rows=1)
        except Exception:
            pass

        logger.info("Completed upload for '%s'.", sheet_title)
        return num_rows
