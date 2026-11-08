#!/usr/bin/env python3
"""
Fetch and aggregate Enedis segmented electricity consumption (<= 36 kVA) by region,
profile (BASE vs HPHC), and subscribed power from 2020 onwards using the Data Fair API.

Instead of downloading ~2 GB/year of 30-minute raw lines, this script uses Data Fair's
server-side `/values_agg` endpoint to aggregate consumption (Wh -> MWh) and average
withdrawal points (PDL) by time period (month, year, or day), while strictly enforcing
Enedis Data Fair rate limits (request count, bandwidth, and cumulative 20s/60s processing time).

Output: brainstorm-data/conso-inf36-region-agg.csv
"""

from __future__ import annotations

import argparse
import calendar
import csv
import datetime as dt
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from pathlib import Path
from typing import Any

BASE_API_URL = "https://opendata.enedis.fr/data-fair/api/v1/datasets/conso-inf36-region"
DEFAULT_OUTPUT = Path("brainstorm-data/conso-inf36-region-agg.csv")

# Official INSEE Region code -> label mapping for clean star-schema joins
REGION_NAMES: dict[str, str] = {
    "11": "Île-de-France",
    "24": "Centre-Val de Loire",
    "27": "Bourgogne-Franche-Comté",
    "28": "Normandie",
    "32": "Hauts-de-France",
    "44": "Grand Est",
    "52": "Pays de la Loire",
    "53": "Bretagne",
    "75": "Nouvelle-Aquitaine",
    "76": "Occitanie",
    "84": "Auvergne-Rhône-Alpes",
    "93": "Provence-Alpes-Côte d'Azur",
    "94": "Corse",
}


class DataFairRateLimiter:
    """
    Enforces Enedis Data Fair API rate limits over a sliding 60-second window:
      - Anonymous: max 600 requests / 60s, max 20s cumulative server processing time / 60s
      - Authenticated: max 1200 requests / 60s, max 60s cumulative processing time / 60s
    We apply a safety margin (75% of hard limits) to avoid HTTP 429 rejections.
    """

    def __init__(self, authenticated: bool = False) -> None:
        self.window_seconds = 60.0
        self.max_requests = int((1200 if authenticated else 600) * 0.75)
        self.max_processing_time = (60.0 if authenticated else 20.0) * 0.75
        # History of (timestamp_completed, duration_seconds)
        self.history: deque[tuple[float, float]] = deque()

    def _prune(self, now: float) -> None:
        cutoff = now - self.window_seconds
        while self.history and self.history[0][0] < cutoff:
            self.history.popleft()

    def wait_if_needed(self) -> None:
        while True:
            now = time.monotonic()
            self._prune(now)

            total_reqs = len(self.history)
            total_proc_time = sum(d for _, d in self.history)

            # Estimate average processing time per request (default 0.5s if empty)
            avg_proc = (total_proc_time / total_reqs) if total_reqs else 0.5

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

    def record(self, duration_seconds: float) -> None:
        now = time.monotonic()
        self._prune(now)
        self.history.append((now, duration_seconds))


def api_get_json(
    url: str,
    limiter: DataFairRateLimiter,
    api_key: str | None = None,
    max_retries: int = 5,
) -> dict[str, Any] | list[Any]:
    """Perform a rate-limited GET request with automatic HTTP 429 / 5xx retry."""
    headers = {
        "Accept": "application/json",
        "User-Agent": "ElectriciteGaz-DataWarehouse-Aggregator/1.0",
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
                limiter.record(elapsed)
                return json.loads(raw.decode("utf-8"))
        except urllib.error.HTTPError as exc:
            elapsed = time.monotonic() - t0
            limiter.record(elapsed)
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
        except (urllib.error.URLError, TimeoutError) as exc:
            elapsed = time.monotonic() - t0
            limiter.record(elapsed)
            if attempt < max_retries:
                wait_s = 5.0 * attempt
                print(
                    f"  [Network Error] {exc} (attempt {attempt}/{max_retries}). "
                    f"Retrying in {wait_s:.0f}s...",
                    file=sys.stderr,
                )
                time.sleep(wait_s)
            else:
                raise


def resolve_schema_fields(
    base_url: str, limiter: DataFairRateLimiter, api_key: str | None
) -> dict[str, str]:
    """
    Inspect `/schema` to automatically resolve exact column keys in `conso-inf36-region`.
    """
    schema_url = f"{base_url}/schema"
    schema = api_get_json(schema_url, limiter, api_key)
    col_names: set[str] = set()
    if isinstance(schema, list):
        for col in schema:
            key = col.get("key") or col.get("name") or col.get("x-originalName")
            if key:
                col_names.add(str(key))

    def pick(candidates: list[str], default: str) -> str:
        for c in candidates:
            if c in col_names:
                return c
        return default

    resolved = {
        "date": pick(["horodate", "date", "date_heure"], "horodate"),
        "code_region": pick(["code_region", "region_code"], "code_region"),
        "profil": pick(["profil", "code_profil", "sous_profil"], "profil"),
        "puissance": pick(
            ["plage_de_puissance_souscrite", "puissance_souscrite", "p_souscrite"],
            "plage_de_puissance_souscrite",
        ),
        "energie_wh": pick(
            ["total_energie_soutiree_wh", "energie_soutiree_wh", "conso_totale_wh"],
            "total_energie_soutiree_wh",
        ),
        "nb_points": pick(
            [
                "nb_points_soutirage",
                "nombre_de_points_de_soutirage",
                "nb_clients",
            ],
            "nb_points_soutirage",
        ),
    }
    print(f"Resolved dataset schema fields: {resolved}")
    return resolved


def map_profil_to_option(profil: str) -> tuple[str, str]:
    """
    Map Enedis profile code to (categorie_client, option_tarifaire)
    to join directly with `brainstorm-data/tarifs-elec/Option_Base.csv` and `Option_HPHC.csv`.
    """
    p = profil.upper().strip()
    if p.startswith("RES1"):
        return ("RES", "BASE")
    if p.startswith("RES2") or "HP" in p or "HC" in p:
        return ("RES", "HPHC")
    if p.startswith("RES"):
        return ("RES", "AUTRE_RES")
    if p.startswith("PRO"):
        return ("PRO", "PRO")
    return ("AUTRE", p)


def parse_kva_upper_bound(plage_kva: str) -> str:
    """
    Extract upper kVA integer from strings like '[0-3] kVA', ']3-6] kVA', ']6-9] kVA'
    so it maps directly to `P_SOUSCRITE` (3, 6, 9, 12, 15, 18, 24, 30, 36) in Option_Base/HPHC.
    """
    nums = re.findall(r"\d+", plage_kva or "")
    return nums[-1] if nums else ""


def generate_periods(
    start_year: int, end_year: int, granularity: str
) -> list[tuple[str, str, str]]:
    """
    Return list of (period_label, start_iso, end_iso) from start_year to end_year.
    """
    today = dt.date.today()
    periods: list[tuple[str, str, str]] = []

    for year in range(start_year, end_year + 1):
        if granularity == "year":
            start_d = dt.date(year, 1, 1)
            end_d = dt.date(year, 12, 31)
            if start_d > today:
                break
            periods.append(
                (
                    f"{year}",
                    f"{start_d.isoformat()}T00:00:00Z",
                    f"{end_d.isoformat()}T23:59:59Z",
                )
            )
        elif granularity == "month":
            for month in range(1, 13):
                start_d = dt.date(year, month, 1)
                if start_d > today:
                    break
                last_day = calendar.monthrange(year, month)[1]
                end_d = dt.date(year, month, last_day)
                periods.append(
                    (
                        f"{year:04d}-{month:02d}",
                        f"{start_d.isoformat()}T00:00:00Z",
                        f"{end_d.isoformat()}T23:59:59Z",
                    )
                )
        elif granularity == "day":
            curr = dt.date(year, 1, 1)
            end_y = min(dt.date(year, 12, 31), today)
            while curr <= end_y:
                periods.append(
                    (
                        curr.isoformat(),
                        f"{curr.isoformat()}T00:00:00Z",
                        f"{curr.isoformat()}T23:59:59Z",
                    )
                )
                curr += dt.timedelta(days=1)

    return periods


def _query_agg_with_fallback(
    base_url: str,
    group_field: str,
    metric_name: str,
    metric_field: str,
    date_field: str,
    start_iso: str,
    end_iso: str,
    limiter: DataFairRateLimiter,
    api_key: str | None,
) -> dict[str, Any] | list[Any]:
    """
    Query /values_agg with progressive fallbacks to handle Data Fair's Lucene PEG parser:
      1. Lucene qs with double-quoted ISO datetime bounds: horodate:["START" TO "END"]
      2. Data Fair column filters: horodate_gte=START & horodate_lte=END
      3. Lucene qs with date-only bounds (no colons): horodate:[YYYY-MM-DD TO YYYY-MM-DD]
    """
    base_params = {
        "field": group_field,
        "agg_size": "100;100;100",
        "metric": metric_name,
        "metric_field": metric_field,
    }

    # Strategy 1: double-quoted ISO timestamps in qs
    params_1 = dict(base_params)
    params_1["qs"] = f'{date_field}:["{start_iso}" TO "{end_iso}"]'
    try:
        return api_get_json(
            f"{base_url}/values_agg?{urllib.parse.urlencode(params_1)}",
            limiter,
            api_key,
        )
    except RuntimeError as err:
        if "HTTP 400" not in str(err):
            raise
        print(
            f"  [Notice] Quoted qs returned 400, falling back to column filters ({date_field}_gte / _lte)...",
            file=sys.stderr,
        )

    # Strategy 2: Data Fair column filters
    params_2 = dict(base_params)
    params_2[f"{date_field}_gte"] = start_iso
    params_2[f"{date_field}_lte"] = end_iso
    try:
        return api_get_json(
            f"{base_url}/values_agg?{urllib.parse.urlencode(params_2)}",
            limiter,
            api_key,
        )
    except RuntimeError as err:
        if "HTTP 400" not in str(err):
            raise
        print(
            f"  [Notice] Column filters returned 400, falling back to date-only bounds...",
            file=sys.stderr,
        )

    # Strategy 3: date-only bounds (no colons)
    params_3 = dict(base_params)
    params_3["qs"] = f"{date_field}:[{start_iso[:10]} TO {end_iso[:10]}]"
    return api_get_json(
        f"{base_url}/values_agg?{urllib.parse.urlencode(params_3)}",
        limiter,
        api_key,
    )


def fetch_period_aggregates(
    base_url: str,
    fields: dict[str, str],
    start_iso: str,
    end_iso: str,
    limiter: DataFairRateLimiter,
    api_key: str | None,
) -> dict[tuple[str, str, str], dict[str, float]]:
    """
    Query `/values_agg` grouped by `code_region;profil;plage_de_puissance_souscrite`
    for a single time window [start_iso TO end_iso].
    """
    group_field = f"{fields['code_region']};{fields['profil']};{fields['puissance']}"

    # 1. Sum of withdrawn energy (Wh)
    resp_energy = _query_agg_with_fallback(
        base_url=base_url,
        group_field=group_field,
        metric_name="sum",
        metric_field=fields["energie_wh"],
        date_field=fields["date"],
        start_iso=start_iso,
        end_iso=end_iso,
        limiter=limiter,
        api_key=api_key,
    )

    # 2. Average number of withdrawal points (PDL)
    resp_pdl = _query_agg_with_fallback(
        base_url=base_url,
        group_field=group_field,
        metric_name="avg",
        metric_field=fields["nb_points"],
        date_field=fields["date"],
        start_iso=start_iso,
        end_iso=end_iso,
        limiter=limiter,
        api_key=api_key,
    )

    results: dict[tuple[str, str, str], dict[str, float]] = {}

    def extract_nested(
        payload: dict[str, Any] | list[Any], metric_key: str
    ) -> None:
        if not isinstance(payload, dict):
            return
        for reg_bucket in payload.get("aggs", []):
            code_reg = str(reg_bucket.get("value", reg_bucket.get("key", "")))
            for prof_bucket in reg_bucket.get("aggs", []):
                profil = str(prof_bucket.get("value", prof_bucket.get("key", "")))
                for p_bucket in prof_bucket.get("aggs", []):
                    plage = str(p_bucket.get("value", p_bucket.get("key", "")))
                    count = float(p_bucket.get("total", p_bucket.get("count", 0)) or 0)
                    metric_val = float(p_bucket.get("metric", 0.0) or 0.0)
                    key = (code_reg, profil, plage)
                    if key not in results:
                        results[key] = {"nb_pas_30min": count, "energie_wh": 0.0, "nb_pdl_moyen": 0.0}
                    results[key][metric_key] = metric_val

    extract_nested(resp_energy, "energie_wh")
    extract_nested(resp_pdl, "nb_pdl_moyen")
    return results


def load_completed_periods(output_path: Path) -> set[str]:
    if not output_path.exists():
        return set()
    completed: set[str] = set()
    with output_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            if row.get("periode"):
                completed.add(row["periode"])
    return completed


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Aggregate Enedis <=36kVA consumption by region, profile (BASE/HPHC), and power."
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=2020,
        help="Start year (default: 2020)",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=dt.date.today().year,
        help="End year (default: current year)",
    )
    parser.add_argument(
        "--granularity",
        choices=["year", "month", "day"],
        default="month",
        help="Time aggregation grain: 'month' (default, aligns with tariff changes), 'year', or 'day'",
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
        help="Optional Enedis Data Fair API key (increases limit to 1200 req/min & 60s proc/min)",
    )
    args = parser.parse_args()

    args.output.parent.mkdir(parents=True, exist_ok=True)
    limiter = DataFairRateLimiter(authenticated=bool(args.api_key))

    print(f"Connecting to {BASE_API_URL} ...")
    fields = resolve_schema_fields(BASE_API_URL, limiter, args.api_key)

    periods = generate_periods(args.start_year, args.end_year, args.granularity)
    completed = load_completed_periods(args.output)

    fieldnames = [
        "periode",
        "annee",
        "code_region",
        "nom_region",
        "categorie_client",
        "option_tarifaire",
        "profil_enedis",
        "plage_de_puissance_souscrite",
        "p_souscrite_kva",
        "conso_totale_mwh",
        "nb_points_soutirage_moyen",
        "conso_moyenne_par_site_mwh",
        "nb_pas_30min",
    ]

    write_header = not args.output.exists() or args.output.stat().st_size == 0
    with args.output.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        if write_header:
            writer.writeheader()

        for idx, (period_label, start_iso, end_iso) in enumerate(periods, start=1):
            if period_label in completed:
                print(f"[{idx}/{len(periods)}] Skipping already aggregated period {period_label}")
                continue

            print(f"[{idx}/{len(periods)}] Aggregating {period_label} ({start_iso} -> {end_iso})...")
            buckets = fetch_period_aggregates(
                BASE_API_URL, fields, start_iso, end_iso, limiter, args.api_key
            )

            rows_written = 0
            for (code_reg, profil, plage), metrics in sorted(buckets.items()):
                categorie, option_tarif = map_profil_to_option(profil)
                p_kva = parse_kva_upper_bound(plage)
                conso_mwh = metrics["energie_wh"] / 1_000_000.0
                nb_pdl = metrics["nb_pdl_moyen"]
                conso_par_site = (conso_mwh / nb_pdl) if nb_pdl > 0 else 0.0

                writer.writerow(
                    {
                        "periode": period_label,
                        "annee": period_label[:4],
                        "code_region": code_reg,
                        "nom_region": REGION_NAMES.get(code_reg, ""),
                        "categorie_client": categorie,
                        "option_tarifaire": option_tarif,
                        "profil_enedis": profil,
                        "plage_de_puissance_souscrite": plage,
                        "p_souscrite_kva": p_kva,
                        "conso_totale_mwh": f"{conso_mwh:.3f}",
                        "nb_points_soutirage_moyen": f"{nb_pdl:.0f}",
                        "conso_moyenne_par_site_mwh": f"{conso_par_site:.4f}",
                        "nb_pas_30min": int(metrics["nb_pas_30min"]),
                    }
                )
                rows_written += 1

            f.flush()
            print(f"  -> Saved {rows_written} aggregated rows for {period_label}")

    print(f"\nDone! Aggregated dataset saved to: {args.output}")


if __name__ == "__main__":
    main()
