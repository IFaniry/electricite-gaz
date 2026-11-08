#!/usr/bin/env python3
"""
Fetch and update `brainstorm-data/REG_conso_ajd.csv` from the Agence ORE Data Fair API:
"Consommation annuelle d'électricité et gaz par région"
(https://opendata.agenceore.fr/data-fair/api/v1/datasets/consommation-annuelle-d-electricite-et-gaz-par-region)

Features:
1. Strict enforcement of Data Fair API rate limits (sliding 60-second window):
   - Anonymous: max 600 req / 60s, max 20s cumulative server processing time / 60s,
     max 500 kB/s bandwidth for API calls (8 MB/s for static /full download).
   - Authenticated (--api-key): max 1200 req / 60s, max 60s cumulative server time / 60s,
     max 1 MB/s bandwidth for API calls (16 MB/s for static /full download).
2. Schema auto-mapping via `GET /schema` to preserve the exact 40 columns
   needed by the Star Schema:
   - Fact metrics: `Conso totale (MWh)`, `Conso moyenne (MWh)`, `Nb sites`, thermosensitive MWh...
   - Dimensions: `FILIERE` (Electricité/Gaz), `annee`, `Code Région`, `Nom Région`,
     `DJU à TR`, `DJU à TN`, and Housing Thermal & Heating Stock (`Taux de chauffage électrique`,
     `Taux de logements collectifs`, dwelling surfaces & construction eras).
3. Two output modes:
   - `--mode raw` (default): Preserves the exact 40-column structure from the API.
   - `--mode star-agg`: Aggregates consumption across operators (`OPERATEUR`) by
     `(annee, Code Région, Nom Région, FILIERE, CODE CATEGORIE CONSOMMATION, CODE GRAND SECTEUR)`
     while retaining non-null regional DJU and housing stock indicators.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from pathlib import Path
from typing import Any

BASE_API_URL = (
    "https://opendata.agenceore.fr/data-fair/api/v1/datasets/"
    "consommation-annuelle-d-electricite-et-gaz-par-region"
)
DEFAULT_OUTPUT = Path("brainstorm-data/REG_conso_ajd.csv")

# Exact 40 columns of brainstorm-data/REG_conso_2011_ajd.csv
ORIGINAL_CSV_COLUMNS: list[str] = [
    "OPERATEUR",
    "FILIERE",
    "annee",
    "Code Région",
    "Nom Région",
    "CODE CATEGORIE CONSOMMATION",
    "CODE GRAND SECTEUR",
    "CODE SECTEUR NAF2",
    "Nb sites",
    "Conso totale (MWh)",
    "Conso moyenne (MWh)",
    "nombre_de_mailles_secretisees",
    "part_thermosensible",
    "Conso totale à usages thermosensibles (MWh)",
    "Conso totale à usages non thermosensibles (MWh)",
    "Thermosensibilité totale (kWh DJU)",
    "conso_totale_corrigee_de_l_alea_climatique_a_usages_thermosensibles_mwh",
    "Conso moyenne à usages thermosensibles (MWh)",
    "Conso moyenne à usages non thermosensibles (MWh)",
    "Thermosensibilité moyenne (kWh DJU)",
    "conso_moyenne_corrigee_de_l_alea_climatique_a_usages_thermosensibles_mwh",
    "DJU à TR",
    "DJU à TN",
    "nombre_d_habitants",
    "Taux de logements collectifs",
    "Taux de résidences principales",
    "superficie_des_logements_30_m2",
    "Superficie des logements 30 à 40 m2",
    "Superficie des logements 40 à 60 m2",
    "Superficie des logements 60 à 80 m2",
    "Superficie des logements 80 à 100 m2",
    "superficie_des_logements_100_m2",
    "Résidences principales avant 1919",
    "Résidences principales de 1919 à 1945",
    "Résidences principales de 1946 à 1970",
    "Résidences principales de 1971 à 1990",
    "Résidences principales de 1991 à 2005",
    "Résidences principales de 2006 à 2018",
    "Résidences principales après 2019",
    "Taux de chauffage électrique",
]


def normalize_slug(text: str) -> str:
    """Normalize a column name or label to an ASCII lowercase alphanumeric slug."""
    nfkd = unicodedata.normalize("NFKD", text)
    ascii_str = "".join(c for c in nfkd if not unicodedata.combining(c))
    slug = "".join(c if c.isalnum() else "_" for c in ascii_str.lower())
    while "__" in slug:
        slug = slug.replace("__", "_")
    return slug.strip("_")


class DataFairRateLimiter:
    """
    Enforces Data Fair API rate limits over a sliding 60-second window:
      - Anonymous: max 600 req/60s, max 20s cumulative server processing time/60s,
        max 500 kB/s bandwidth for API calls.
      - Authenticated: max 1200 req/60s, max 60s cumulative processing time/60s,
        max 1 MB/s bandwidth for API calls.
    Applies a 75% safety margin to prevent HTTP 429 rejections.
    """

    def __init__(self, authenticated: bool = False) -> None:
        self.window_seconds = 60.0
        self.max_requests = int((1200 if authenticated else 600) * 0.75)
        self.max_processing_time = (60.0 if authenticated else 20.0) * 0.75
        self.max_bytes_per_sec = (1024 * 1024 if authenticated else 500 * 1024) * 0.85
        # History of (timestamp_completed, duration_seconds, bytes_downloaded)
        self.history: deque[tuple[float, float, int]] = deque()

    def _prune(self, now: float) -> None:
        cutoff = now - self.window_seconds
        while self.history and self.history[0][0] < cutoff:
            self.history.popleft()

    def wait_if_needed(self) -> None:
        while True:
            now = time.monotonic()
            self._prune(now)

            total_reqs = len(self.history)
            total_proc_time = sum(d for _, d, _ in self.history)
            avg_proc = (total_proc_time / total_reqs) if total_reqs else 0.4

            if (
                total_reqs + 1 > self.max_requests
                or (total_proc_time + avg_proc) > self.max_processing_time
            ):
                oldest_ts = self.history[0][0]
                sleep_for = max(0.5, (oldest_ts + self.window_seconds) - now + 0.2)
                print(
                    f"  [RateLimiter] Cumulative proc={total_proc_time:.1f}s "
                    f"({total_reqs} reqs in last 60s). Pausing {sleep_for:.1f}s...",
                    file=sys.stderr,
                )
                time.sleep(sleep_for)
            else:
                break

    def record(self, duration_seconds: float, n_bytes: int) -> None:
        now = time.monotonic()
        self._prune(now)
        self.history.append((now, duration_seconds, n_bytes))

        # Bandwidth throttling check (500 kB/s anonymous, 1 MB/s authenticated)
        min_transfer_time = n_bytes / self.max_bytes_per_sec
        if duration_seconds < min_transfer_time:
            time.sleep(min_transfer_time - duration_seconds)


def api_get_json(
    url: str,
    limiter: DataFairRateLimiter,
    api_key: str | None = None,
    max_retries: int = 5,
) -> dict[str, Any] | list[Any]:
    """Perform a rate-limited GET request returning parsed JSON with HTTP 429/5xx retry."""
    headers = {
        "Accept": "application/json",
        "User-Agent": "ElectriciteGaz-StarSchema-Updater/1.0",
    }
    if api_key:
        headers["x-apiKey"] = api_key

    for attempt in range(1, max_retries + 1):
        limiter.wait_if_needed()
        t0 = time.monotonic()
        req = urllib.request.Request(url, headers=headers, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                raw = resp.read()
                elapsed = time.monotonic() - t0
                limiter.record(elapsed, len(raw))
                return json.loads(raw.decode("utf-8"))
        except urllib.error.HTTPError as exc:
            elapsed = time.monotonic() - t0
            limiter.record(elapsed, 0)
            if exc.code == 429:
                retry_after = exc.headers.get("Retry-After")
                wait_s = float(retry_after) if retry_after and retry_after.isdigit() else 60.0
                print(
                    f"  [HTTP 429] Rate limit reached (attempt {attempt}/{max_retries}). "
                    f"Cooling down {wait_s:.0f}s...",
                    file=sys.stderr,
                )
                time.sleep(wait_s)
            elif exc.code in (500, 502, 503, 504) and attempt < max_retries:
                wait_s = 5.0 * (2 ** (attempt - 1))
                print(
                    f"  [HTTP {exc.code}] Server error (attempt {attempt}/{max_retries}). "
                    f"Retrying in {wait_s:.0f}s...",
                    file=sys.stderr,
                )
                time.sleep(wait_s)
            else:
                body = exc.read().decode("utf-8", errors="replace")
                raise RuntimeError(f"HTTP {exc.code} for {url}: {body}") from exc
        except urllib.error.URLError as exc:
            elapsed = time.monotonic() - t0
            limiter.record(elapsed, 0)
            if attempt < max_retries:
                wait_s = 5.0 * attempt
                print(
                    f"  [Network Error] {exc.reason} (attempt {attempt}/{max_retries}). "
                    f"Retrying in {wait_s:.0f}s...",
                    file=sys.stderr,
                )
                time.sleep(wait_s)
            else:
                raise RuntimeError(f"Failed to connect to {url}: {exc}") from exc

    raise RuntimeError(f"Exhausted retries for {url}")


def build_column_mapping(
    base_url: str,
    limiter: DataFairRateLimiter,
    api_key: str | None = None,
) -> tuple[dict[str, str], str]:
    """
    Inspect `GET /schema` to map each of the 40 target CSV columns in
    `ORIGINAL_CSV_COLUMNS` to its corresponding Data Fair `key` in `/lines`.
    Also returns the exact Data Fair `key` for the `annee` column.
    """
    schema = api_get_json(f"{base_url}/schema", limiter, api_key)
    if not isinstance(schema, list):
        raise RuntimeError(f"Unexpected /schema response: {schema}")

    # Index schema fields by x-originalName, title, key, and normalized slug
    by_exact: dict[str, str] = {}
    by_slug: dict[str, str] = {}

    for col in schema:
        if not isinstance(col, dict):
            continue
        key = str(col.get("key", ""))
        if not key or key.startswith("_"):
            continue
        orig = str(col.get("x-originalName", ""))
        title = str(col.get("title", ""))

        by_exact[key] = key
        if orig:
            by_exact[orig] = key
            by_slug[normalize_slug(orig)] = key
        if title:
            by_exact[title] = key
            by_slug[normalize_slug(title)] = key
        by_slug[normalize_slug(key)] = key

    col_to_key: dict[str, str] = {}
    for target_col in ORIGINAL_CSV_COLUMNS:
        if target_col in by_exact:
            col_to_key[target_col] = by_exact[target_col]
        else:
            slug = normalize_slug(target_col)
            if slug in by_slug:
                col_to_key[target_col] = by_slug[slug]
            else:
                # Fallback to normalized slug if not found in schema
                col_to_key[target_col] = slug

    annee_key = col_to_key.get("annee", "annee")
    return col_to_key, annee_key


def build_initial_lines_url(
    base_url: str,
    annee_key: str,
    start_year: int | None,
    end_year: int | None,
    page_size: int,
    use_qs_filter: bool = True,
) -> str:
    """Construct the initial `/lines` query URL with year filtering and sorting."""
    query: dict[str, Any] = {
        "size": page_size,
        "sort": f"{annee_key}",
    }
    if use_qs_filter and (start_year is not None or end_year is not None):
        lo = str(start_year) if start_year is not None else "*"
        hi = str(end_year) if end_year is not None else "*"
        query["qs"] = f"{annee_key}:[{lo} TO {hi}]"

    return f"{base_url}/lines?{urllib.parse.urlencode(query)}"


def fetch_all_lines(
    base_url: str,
    annee_key: str,
    start_year: int | None,
    end_year: int | None,
    page_size: int,
    limiter: DataFairRateLimiter,
    api_key: str | None = None,
) -> list[dict[str, Any]]:
    """
    Paginate through `GET /lines` using Data Fair's `next` cursor URL while respecting
    rate limits. Falls back to client-side year filtering if `qs` range query is rejected.
    """
    use_qs = True
    url: str | None = build_initial_lines_url(
        base_url, annee_key, start_year, end_year, page_size, use_qs_filter=True
    )

    all_rows: list[dict[str, Any]] = []
    page_num = 0
    total_expected: int | None = None

    while url:
        page_num += 1
        try:
            payload = api_get_json(url, limiter, api_key)
        except RuntimeError as exc:
            # If the server rejects `qs=annee:[start TO end]` on first page, fallback to unfiltered
            if page_num == 1 and use_qs and "HTTP 400" in str(exc):
                print(
                    "  [Fallback] Server rejected qs range filter; falling back to "
                    "paginated fetch with client-side year filter...",
                    file=sys.stderr,
                )
                use_qs = False
                url = build_initial_lines_url(
                    base_url, annee_key, start_year, end_year, page_size, use_qs_filter=False
                )
                page_num = 0
                continue
            raise

        if not isinstance(payload, dict):
            raise RuntimeError(f"Unexpected /lines payload: {payload}")

        if total_expected is None:
            total_expected = int(payload.get("total", 0))
            print(f"Total matching records reported by API: {total_expected:,}")

        results = payload.get("results", [])
        if not results:
            break

        for row in results:
            if not isinstance(row, dict):
                continue
            # Ensure year filter is respected even when fallback is active
            raw_year = row.get(annee_key)
            if raw_year is not None and (start_year is not None or end_year is not None):
                try:
                    yr = int(str(raw_year)[:4])
                    if start_year is not None and yr < start_year:
                        continue
                    if end_year is not None and yr > end_year:
                        continue
                except ValueError:
                    pass
            all_rows.append(row)

        print(
            f"  Page {page_num}: fetched {len(results):,} lines "
            f"(kept {len(all_rows):,} / {total_expected:,})..."
        )

        next_url = payload.get("next")
        url = str(next_url) if next_url else None

    return all_rows


def write_raw_csv(
    rows: list[dict[str, Any]],
    col_to_key: dict[str, str],
    output_path: Path,
) -> int:
    """
    Write rows preserving the exact 40-column schema of `brainstorm-data/REG_conso_2011_ajd.csv`.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter=",")
        writer.writerow(ORIGINAL_CSV_COLUMNS)

        for row in rows:
            out_vals = []
            for col_name in ORIGINAL_CSV_COLUMNS:
                df_key = col_to_key[col_name]
                val = row.get(df_key, "")
                out_vals.append("" if val is None else val)
            writer.writerow(out_vals)

    return len(rows)


def _to_float(val: Any) -> float | None:
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def write_star_aggregated_csv(
    rows: list[dict[str, Any]],
    col_to_key: dict[str, str],
    output_path: Path,
) -> int:
    """
    Aggregate rows across operators (`OPERATEUR`) and NAF2 sub-sectors so that each
    `(annee, Code Région, Nom Région, FILIERE, CODE CATEGORIE CONSOMMATION, CODE GRAND SECTEUR)`
    has a single consolidated row with summed MWh/sites and preserved regional DJU & housing stock.
    Outputs the exact same 40-column format (with `OPERATEUR="TOUS_OPERATEURS"` and `CODE SECTEUR NAF2=""`)
    for drop-in compatibility with existing pipelines.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Columns that should be summed across operators
    sum_cols = {
        "Nb sites",
        "Conso totale (MWh)",
        "nombre_de_mailles_secretisees",
        "Conso totale à usages thermosensibles (MWh)",
        "Conso totale à usages non thermosensibles (MWh)",
        "Thermosensibilité totale (kWh DJU)",
        "conso_totale_corrigee_de_l_alea_climatique_a_usages_thermosensibles_mwh",
    }

    # Regional / contextual columns where we take the first non-null value for that (annee, region)
    regional_context_cols = [
        "DJU à TR",
        "DJU à TN",
        "nombre_d_habitants",
        "Taux de logements collectifs",
        "Taux de résidences principales",
        "superficie_des_logements_30_m2",
        "Superficie des logements 30 à 40 m2",
        "Superficie des logements 40 à 60 m2",
        "Superficie des logements 60 à 80 m2",
        "Superficie des logements 80 à 100 m2",
        "superficie_des_logements_100_m2",
        "Résidences principales avant 1919",
        "Résidences principales de 1919 à 1945",
        "Résidences principales de 1946 à 1970",
        "Résidences principales de 1971 à 1990",
        "Résidences principales de 1991 à 2005",
        "Résidences principales de 2006 à 2018",
        "Résidences principales après 2019",
        "Taux de chauffage électrique",
    ]

    # Pre-collect regional attributes by (annee, Code Région) so every row in that region-year
    # gets the regional DJU and housing indicators even if an operator row had them blank
    region_year_context: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        yr = str(row.get(col_to_key["annee"], ""))
        reg = str(row.get(col_to_key["Code Région"], ""))
        if not yr or not reg:
            continue
        ctx = region_year_context.setdefault((yr, reg), {})
        for rcol in regional_context_cols:
            val = row.get(col_to_key[rcol])
            if val is not None and val != "" and rcol not in ctx:
                ctx[rcol] = val

    # Group by (FILIERE, annee, Code Région, Nom Région, CODE CATEGORIE CONSOMMATION, CODE GRAND SECTEUR)
    groups: dict[tuple[str, str, str, str, str, str], dict[str, float]] = {}
    has_value: dict[tuple[str, str, str, str, str, str], set[str]] = {}

    for row in rows:
        filiere = str(row.get(col_to_key["FILIERE"], ""))
        yr = str(row.get(col_to_key["annee"], ""))
        code_reg = str(row.get(col_to_key["Code Région"], ""))
        nom_reg = str(row.get(col_to_key["Nom Région"], ""))
        cat = str(row.get(col_to_key["CODE CATEGORIE CONSOMMATION"], ""))
        secteur = str(row.get(col_to_key["CODE GRAND SECTEUR"], ""))

        gkey = (filiere, yr, code_reg, nom_reg, cat, secteur)
        acc = groups.setdefault(gkey, {c: 0.0 for c in sum_cols})
        present = has_value.setdefault(gkey, set())

        for scol in sum_cols:
            fval = _to_float(row.get(col_to_key[scol]))
            if fval is not None:
                acc[scol] += fval
                present.add(scol)

    sorted_keys = sorted(groups.keys(), key=lambda k: (k[1], k[2], k[0], k[4], k[5]))

    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter=",")
        writer.writerow(ORIGINAL_CSV_COLUMNS)

        for gkey in sorted_keys:
            filiere, yr, code_reg, nom_reg, cat, secteur = gkey
            acc = groups[gkey]
            present = has_value[gkey]
            ctx = region_year_context.get((yr, code_reg), {})

            nb_sites = acc["Nb sites"] if "Nb sites" in present else 0.0
            conso_tot = acc["Conso totale (MWh)"] if "Conso totale (MWh)" in present else 0.0
            conso_moy = round(conso_tot / nb_sites, 4) if nb_sites > 0 else ""

            row_dict: dict[str, Any] = {
                "OPERATEUR": "TOUS_OPERATEURS",
                "FILIERE": filiere,
                "annee": yr,
                "Code Région": code_reg,
                "Nom Région": nom_reg,
                "CODE CATEGORIE CONSOMMATION": cat,
                "CODE GRAND SECTEUR": secteur,
                "CODE SECTEUR NAF2": "",
                "Nb sites": round(nb_sites, 2) if "Nb sites" in present else "",
                "Conso totale (MWh)": round(conso_tot, 4) if "Conso totale (MWh)" in present else "",
                "Conso moyenne (MWh)": conso_moy,
            }

            for scol in sum_cols - {"Nb sites", "Conso totale (MWh)"}:
                row_dict[scol] = round(acc[scol], 4) if scol in present else ""

            for rcol in regional_context_cols:
                row_dict[rcol] = ctx.get(rcol, "")

            writer.writerow([row_dict.get(col, "") for col in ORIGINAL_CSV_COLUMNS])

    return len(sorted_keys)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Update brainstorm-data/REG_conso_ajd.csv from the Agence ORE Data Fair API "
            "while respecting rate limits."
        )
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=None,
        help="Minimum year to fetch (e.g. 2020 or 2011). Omit to fetch all available years.",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=None,
        help="Maximum year to fetch. Omit to fetch up to the latest available year.",
    )
    parser.add_argument(
        "--mode",
        choices=["raw", "star-agg"],
        default="raw",
        help=(
            "'raw' (default) keeps the exact original rows from the API; "
            "'star-agg' aggregates across operators and NAF2 codes per (year, region, filiere, sector) "
            "and backfills regional DJU & housing stock columns."
        ),
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=1000,
        help="Number of records per paginated /lines API call (default: 1000, max: 10000).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Output CSV path (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--api-key",
        type=str,
        default=None,
        help="Optional Data Fair API key (doubles rate limit quotas).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    limiter = DataFairRateLimiter(authenticated=bool(args.api_key))

    print(f"Connecting to {BASE_API_URL} ...")
    col_to_key, annee_key = build_column_mapping(BASE_API_URL, limiter, args.api_key)
    print(
        f"Mapped {len(col_to_key)}/40 columns from /schema "
        f"(year field key: '{annee_key}')."
    )

    year_desc = (
        f"years [{args.start_year or '*'} TO {args.end_year or '*'}]"
        if (args.start_year or args.end_year)
        else "all available years (2011 -> present)"
    )
    print(f"Fetching lines for {year_desc} (page_size={args.page_size})...")

    rows = fetch_all_lines(
        base_url=BASE_API_URL,
        annee_key=annee_key,
        start_year=args.start_year,
        end_year=args.end_year,
        page_size=args.page_size,
        limiter=limiter,
        api_key=args.api_key,
    )

    if args.mode == "star-agg":
        written = write_star_aggregated_csv(rows, col_to_key, args.output)
        print(
            f"\nDone! Consolidated {len(rows):,} raw lines into {written:,} "
            f"star-model rows in {args.output}"
        )
    else:
        written = write_raw_csv(rows, col_to_key, args.output)
        print(f"\nDone! Wrote {written:,} rows to {args.output}")


if __name__ == "__main__":
    main()
