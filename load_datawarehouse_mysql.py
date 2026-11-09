#!/usr/bin/env python3
"""
ETL Pipeline (Polars Lazy API -> MySQL) for the Energy Consumption Data Warehouse.

Loads the 8 tables of the Constellation Schema defined in `Datawarehouse.md` (lines 9-135)
from `brainstorm-data/` into a MySQL database:

Dimensions:
  1. `dim_territoire`
  2. `dim_energie`
  3. `dim_calendrier` (enriched with `jours-feries-france` and `Zone-A-B-C-Corse.ics`)
  4. `dim_tarif_reglemente` (`Option_Base.csv`, `Option_HPHC.csv`, `Option_Tempo.csv`)
  5. `dim_contexte_territoire_annuel` (climate DJU & housing stock from `REG_conso`)

Fact Tables:
  6. `fait_conso_journaliere_elec` (`eco2mix-regional-tr.csv`)
  7. `fait_conso_segmentee_inf36` (`conso-inf36-region-agg.csv`)
  8. `fait_conso_annuelle` (`REG_conso_ajd.csv` / `REG_conso_2011_ajd.csv`)
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
from pathlib import Path
from urllib.parse import quote_plus

import polars as pl
from dotenv import load_dotenv
from jours_feries_france import JoursFeries
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Load environment variables from .env file if present
load_dotenv()

DEFAULT_DATA_DIR = Path("brainstorm-data")

# Official French regions and their academic school holiday zones
TERRITOIRES_REFERENCE: list[dict[str, str]] = [
    {"code_geo": "11", "nom_region": "Île-de-France", "zone_vacances": "Zone C"},
    {"code_geo": "24", "nom_region": "Centre-Val de Loire", "zone_vacances": "Zone B"},
    {"code_geo": "27", "nom_region": "Bourgogne-Franche-Comté", "zone_vacances": "Zone A"},
    {"code_geo": "28", "nom_region": "Normandie", "zone_vacances": "Zone B"},
    {"code_geo": "32", "nom_region": "Hauts-de-France", "zone_vacances": "Zone B"},
    {"code_geo": "44", "nom_region": "Grand Est", "zone_vacances": "Zone B"},
    {"code_geo": "52", "nom_region": "Pays de la Loire", "zone_vacances": "Zone B"},
    {"code_geo": "53", "nom_region": "Bretagne", "zone_vacances": "Zone B"},
    {"code_geo": "75", "nom_region": "Nouvelle-Aquitaine", "zone_vacances": "Zone A"},
    {"code_geo": "76", "nom_region": "Occitanie", "zone_vacances": "Zone C"},
    {"code_geo": "84", "nom_region": "Auvergne-Rhône-Alpes", "zone_vacances": "Zone A"},
    {"code_geo": "93", "nom_region": "Provence-Alpes-Côte d'Azur", "zone_vacances": "Zone B"},
    {"code_geo": "94", "nom_region": "Corse", "zone_vacances": "Corse"},
    # Overseas regions (DROM) present in REG_conso
    {"code_geo": "01", "nom_region": "Guadeloupe", "zone_vacances": "Outre-mer"},
    {"code_geo": "02", "nom_region": "Martinique", "zone_vacances": "Outre-mer"},
    {"code_geo": "03", "nom_region": "Guyane", "zone_vacances": "Outre-mer"},
    {"code_geo": "04", "nom_region": "La Réunion", "zone_vacances": "Outre-mer"},
    {"code_geo": "06", "nom_region": "Mayotte", "zone_vacances": "Outre-mer"},
]

ENERGIES_REFERENCE: list[dict[str, str]] = [
    {
        "id_energie": "ELEC",
        "code_filiere": "Electricité",
        "libelle_energie": "Électricité",
        "unite_mesure": "MWh",
    },
    {
        "id_energie": "GAZ",
        "code_filiere": "Gaz",
        "libelle_energie": "Gaz naturel",
        "unite_mesure": "MWh",
    },
]


# ============================================================================
# 1. Lazy Builders for Dimension Tables
# ============================================================================


def build_dim_territoire() -> pl.LazyFrame:
    """Build `dim_territoire` LazyFrame."""
    return (
        pl.DataFrame(TERRITOIRES_REFERENCE)
        .lazy()
        .select(
            pl.col("code_geo").cast(pl.String),
            pl.col("nom_region").cast(pl.String),
            pl.col("zone_vacances").cast(pl.String),
        )
        .sort("code_geo")
    )


def build_dim_energie() -> pl.LazyFrame:
    """Build `dim_energie` LazyFrame."""
    return (
        pl.DataFrame(ENERGIES_REFERENCE)
        .lazy()
        .select(
            pl.col("id_energie").cast(pl.String),
            pl.col("code_filiere").cast(pl.String),
            pl.col("libelle_energie").cast(pl.String),
            pl.col("unite_mesure").cast(pl.String),
        )
        .sort("id_energie")
    )


def _parse_ics_vacation_intervals(ics_path: Path) -> pl.LazyFrame:
    """
    Parse `Zone-A-B-C-Corse.ics` into a LazyFrame of daily zone flags:
    `(date_jour, vacances_zone_a, vacances_zone_b, vacances_zone_c, vacances_corse)`.
    """
    content = ics_path.read_text(encoding="utf-8-sig")
    raw_events = content.split("BEGIN:VEVENT")[1:]

    records: list[dict[str, object]] = []
    for raw_ev in raw_events:
        props: dict[str, str] = {}
        for line in raw_ev.splitlines():
            if ":" in line:
                k, v = line.split(":", 1)
                props[k.strip()] = v.strip()

        summary = props.get("SUMMARY", "")
        # Ignore teacher-only pre-return days
        if "prérentrée Enseignants" in summary:
            continue

        dtstart_str = props.get("DTSTART;VALUE=DATE") or props.get("DTSTART", "")
        dtend_str = props.get("DTEND;VALUE=DATE") or props.get("DTEND", "")
        location = props.get("LOCATION", "")

        if len(dtstart_str) < 8 or len(dtend_str) < 8:
            continue

        start_date = dt.date.fromisoformat(f"{dtstart_str[:4]}-{dtstart_str[4:6]}-{dtstart_str[6:8]}")
        end_date = dt.date.fromisoformat(f"{dtend_str[:4]}-{dtend_str[4:6]}-{dtend_str[6:8]}")
        if end_date <= start_date:
            continue

        records.append(
            {
                "start_date": start_date,
                "end_date": end_date,
                "z_a": ("Zone A" in location) or ("Zones A/" in location),
                "z_b": ("Zone B" in location) or ("Zones B/" in location) or ("/B" in location),
                "z_c": ("Zone C" in location) or ("/C" in location),
                "z_corse": "Corse" in location,
            }
        )

    if not records:
        return pl.DataFrame(
            schema={
                "date_jour": pl.Date,
                "vacances_zone_a": pl.Boolean,
                "vacances_zone_b": pl.Boolean,
                "vacances_zone_c": pl.Boolean,
                "vacances_corse": pl.Boolean,
            }
        ).lazy()

    # Expand [start_date, end_date) intervals using native Polars date_ranges + explode
    return (
        pl.DataFrame(records)
        .lazy()
        .filter(pl.col("end_date") >= dt.date(2020, 1, 1))
        .with_columns(
            pl.date_ranges(
                pl.col("start_date"),
                pl.col("end_date"),
                interval="1d",
                closed="left",
            ).alias("date_jour")
        )
        .explode("date_jour", empty_as_null=False)
        .group_by("date_jour")
        .agg(
            pl.col("z_a").any().alias("vacances_zone_a"),
            pl.col("z_b").any().alias("vacances_zone_b"),
            pl.col("z_c").any().alias("vacances_zone_c"),
            pl.col("z_corse").any().alias("vacances_corse"),
        )
    )


def _build_french_holidays_lazy(start_year: int, end_year: int) -> pl.LazyFrame:
    """Build a LazyFrame of French public holidays from `jours-feries-france`."""
    rows: list[dict[str, object]] = []
    for year in range(start_year, end_year + 1):
        year_holidays = JoursFeries.for_year(year)
        for holiday_name, holiday_date in year_holidays.items():
            rows.append(
                {
                    "date_jour": holiday_date,
                    "est_jour_ferie": True,
                    "nom_jour_ferie": str(holiday_name),
                }
            )
    return pl.DataFrame(
        rows,
        schema={
            "date_jour": pl.Date,
            "est_jour_ferie": pl.Boolean,
            "nom_jour_ferie": pl.String,
        },
    ).lazy()


def build_dim_calendrier(
    data_dir: Path,
    start_year: int = 2020,
    end_year: int = 2026,
) -> pl.LazyFrame:
    """
    Build `dim_calendrier` LazyFrame for `start_year..end_year` enriched with
    `jours-feries-france` and `Zone-A-B-C-Corse.ics`.
    """
    ics_path = data_dir / "vacances-scolaires" / "Zone-A-B-C-Corse.ics"
    vacations_lf = _parse_ics_vacation_intervals(ics_path)
    holidays_lf = _build_french_holidays_lazy(start_year, end_year)

    base_dates = (
        pl.DataFrame(
            {
                "date_jour": pl.date_range(
                    dt.date(start_year, 1, 1),
                    dt.date(end_year, 12, 31),
                    interval="1d",
                    eager=True,
                )
            }
        )
        .lazy()
    )

    # Month*100 + Day expression for calendar season boundaries
    md = pl.col("date_jour").dt.month() * 100 + pl.col("date_jour").dt.day()

    return (
        base_dates.join(holidays_lf, on="date_jour", how="left")
        .join(vacations_lf, on="date_jour", how="left")
        .with_columns(
            pl.col("date_jour").dt.year().cast(pl.Int32).alias("annee"),
            pl.col("date_jour").dt.strftime("%Y-%m").alias("periode_mois"),
            pl.col("date_jour").dt.month().cast(pl.Int32).alias("mois"),
            pl.col("date_jour").dt.weekday().cast(pl.Int32).alias("jour_semaine"),
            (pl.col("date_jour").dt.weekday() >= 6).alias("est_weekend"),
            pl.when((md >= 320) & (md < 621))
            .then(pl.lit("Printemps"))
            .when((md >= 621) & (md < 922))
            .then(pl.lit("Été"))
            .when((md >= 922) & (md < 1221))
            .then(pl.lit("Automne"))
            .otherwise(pl.lit("Hiver"))
            .alias("saison"),
            pl.col("est_jour_ferie").fill_null(False),
            pl.col("vacances_zone_a").fill_null(False),
            pl.col("vacances_zone_b").fill_null(False),
            pl.col("vacances_zone_c").fill_null(False),
            pl.col("vacances_corse").fill_null(False),
        )
        .select(
            "date_jour",
            "annee",
            "periode_mois",
            "mois",
            "jour_semaine",
            "est_weekend",
            "saison",
            "est_jour_ferie",
            "nom_jour_ferie",
            "vacances_zone_a",
            "vacances_zone_b",
            "vacances_zone_c",
            "vacances_corse",
        )
        .sort("date_jour")
    )


def build_dim_tarif_reglemente(data_dir: Path) -> pl.LazyFrame:
    """
    Build `dim_tarif_reglemente` LazyFrame from `Option_Base.csv`,
    `Option_HPHC.csv`, and `Option_Tempo.csv`.
    """
    tarifs_dir = data_dir / "tarifs-elec"
    null_vals = ["", "N/A", "NULL"]

    base_lf = (
        pl.scan_csv(
            tarifs_dir / "Option_Base.csv",
            separator=";",
            decimal_comma=True,
            null_values=null_vals,
            schema_overrides={
                "DATE_DEBUT": pl.String,
                "DATE_FIN": pl.String,
                "P_SOUSCRITE": pl.Int32,
                "PART_FIXE_TTC": pl.Float64,
                "PART_VARIABLE_TTC": pl.Float64,
            },
        )
        .filter(pl.col("PART_FIXE_TTC").is_not_null())
        .select(
            pl.lit("BASE").alias("option_tarifaire"),
            pl.col("P_SOUSCRITE").alias("p_souscrite_kva"),
            pl.col("DATE_DEBUT").str.to_date("%d/%m/%Y").alias("date_debut"),
            pl.col("DATE_FIN").str.to_date("%d/%m/%Y", strict=False).alias("date_fin"),
            pl.col("PART_FIXE_TTC").alias("part_fixe_ttc"),
            pl.col("PART_VARIABLE_TTC").alias("part_variable_ttc_base"),
            pl.lit(None, dtype=pl.Float64).alias("part_variable_ttc_hp"),
            pl.lit(None, dtype=pl.Float64).alias("part_variable_ttc_hc"),
            pl.lit(None, dtype=pl.Float64).alias("part_variable_bleu_hp"),
            pl.lit(None, dtype=pl.Float64).alias("part_variable_bleu_hc"),
            pl.lit(None, dtype=pl.Float64).alias("part_variable_blanc_hp"),
            pl.lit(None, dtype=pl.Float64).alias("part_variable_blanc_hc"),
            pl.lit(None, dtype=pl.Float64).alias("part_variable_rouge_hp"),
            pl.lit(None, dtype=pl.Float64).alias("part_variable_rouge_hc"),
        )
    )

    hphc_lf = (
        pl.scan_csv(
            tarifs_dir / "Option_HPHC.csv",
            separator=";",
            decimal_comma=True,
            null_values=null_vals,
            schema_overrides={
                "DATE_DEBUT": pl.String,
                "DATE_FIN": pl.String,
                "P_SOUSCRITE": pl.Int32,
                "PART_FIXE_TTC": pl.Float64,
                "PART_VARIABLE_HP_TTC": pl.Float64,
                "PART_VARIABLE_HC_TTC": pl.Float64,
            },
        )
        .filter(pl.col("PART_FIXE_TTC").is_not_null())
        .select(
            pl.lit("HPHC").alias("option_tarifaire"),
            pl.col("P_SOUSCRITE").alias("p_souscrite_kva"),
            pl.col("DATE_DEBUT").str.to_date("%d/%m/%Y").alias("date_debut"),
            pl.col("DATE_FIN").str.to_date("%d/%m/%Y", strict=False).alias("date_fin"),
            pl.col("PART_FIXE_TTC").alias("part_fixe_ttc"),
            pl.lit(None, dtype=pl.Float64).alias("part_variable_ttc_base"),
            pl.col("PART_VARIABLE_HP_TTC").alias("part_variable_ttc_hp"),
            pl.col("PART_VARIABLE_HC_TTC").alias("part_variable_ttc_hc"),
            pl.lit(None, dtype=pl.Float64).alias("part_variable_bleu_hp"),
            pl.lit(None, dtype=pl.Float64).alias("part_variable_bleu_hc"),
            pl.lit(None, dtype=pl.Float64).alias("part_variable_blanc_hp"),
            pl.lit(None, dtype=pl.Float64).alias("part_variable_blanc_hc"),
            pl.lit(None, dtype=pl.Float64).alias("part_variable_rouge_hp"),
            pl.lit(None, dtype=pl.Float64).alias("part_variable_rouge_hc"),
        )
    )

    tempo_lf = (
        pl.scan_csv(
            tarifs_dir / "Option_Tempo.csv",
            separator=";",
            decimal_comma=True,
            null_values=null_vals,
            schema_overrides={
                "DATE_DEBUT": pl.String,
                "DATE_FIN": pl.String,
                "P_SOUSCRITE": pl.Int32,
                "PART_FIXE_TTC": pl.Float64,
                "PART_VARIABLE_HPBleu_TTC": pl.Float64,
                "PART_VARIABLE_HCBleu_TTC": pl.Float64,
                "PART_VARIABLE_HPBlanc_TTC": pl.Float64,
                "PART_VARIABLE_HCBlanc_TTC": pl.Float64,
                "PART_VARIABLE_HPRouge_TTC": pl.Float64,
                "PART_VARIABLE_HCRouge_TTC": pl.Float64,
            },
        )
        .filter(pl.col("PART_FIXE_TTC").is_not_null())
        .select(
            pl.lit("TEMPO").alias("option_tarifaire"),
            pl.col("P_SOUSCRITE").alias("p_souscrite_kva"),
            pl.col("DATE_DEBUT").str.to_date("%d/%m/%Y").alias("date_debut"),
            pl.col("DATE_FIN").str.to_date("%d/%m/%Y", strict=False).alias("date_fin"),
            pl.col("PART_FIXE_TTC").alias("part_fixe_ttc"),
            pl.lit(None, dtype=pl.Float64).alias("part_variable_ttc_base"),
            pl.lit(None, dtype=pl.Float64).alias("part_variable_ttc_hp"),
            pl.lit(None, dtype=pl.Float64).alias("part_variable_ttc_hc"),
            pl.col("PART_VARIABLE_HPBleu_TTC").alias("part_variable_bleu_hp"),
            pl.col("PART_VARIABLE_HCBleu_TTC").alias("part_variable_bleu_hc"),
            pl.col("PART_VARIABLE_HPBlanc_TTC").alias("part_variable_blanc_hp"),
            pl.col("PART_VARIABLE_HCBlanc_TTC").alias("part_variable_blanc_hc"),
            pl.col("PART_VARIABLE_HPRouge_TTC").alias("part_variable_rouge_hp"),
            pl.col("PART_VARIABLE_HCRouge_TTC").alias("part_variable_rouge_hc"),
        )
    )

    return (
        pl.concat([base_lf, hphc_lf, tempo_lf])
        .with_columns(
            pl.concat_str(
                [
                    pl.col("option_tarifaire"),
                    pl.col("p_souscrite_kva").cast(pl.String),
                    pl.col("date_debut").dt.strftime("%Y-%m-%d"),
                ],
                separator="_",
            ).alias("id_tarif")
        )
        .select(
            "id_tarif",
            "option_tarifaire",
            "p_souscrite_kva",
            "date_debut",
            "date_fin",
            "part_fixe_ttc",
            "part_variable_ttc_base",
            "part_variable_ttc_hp",
            "part_variable_ttc_hc",
            "part_variable_bleu_hp",
            "part_variable_bleu_hc",
            "part_variable_blanc_hp",
            "part_variable_blanc_hc",
            "part_variable_rouge_hp",
            "part_variable_rouge_hc",
        )
        .sort("option_tarifaire", "p_souscrite_kva", "date_debut")
    )


def _scan_reg_conso(data_dir: Path) -> pl.LazyFrame:
    """
    Scan `REG_conso_ajd.csv` (or `REG_conso_2011_ajd.csv` if `REG_conso_ajd.csv` is not present)
    with strict schema overrides so regional codes and numeric housing columns are typed at scan.
    """
    primary_path = data_dir / "REG_conso_ajd.csv"
    fallback_path = data_dir / "REG_conso_2011_ajd.csv"
    csv_path = primary_path if primary_path.exists() else fallback_path

    return (
        pl.scan_csv(
            csv_path,
            separator=",",
            null_values=["", "N/A", "NULL"],
            infer_schema_length=10_000,
            schema_overrides={
                "OPERATEUR": pl.String,
                "FILIERE": pl.String,
                "annee": pl.Int32,
                "Code Région": pl.String,
                "Nom Région": pl.String,
                "CODE CATEGORIE CONSOMMATION": pl.String,
                "CODE GRAND SECTEUR": pl.String,
                "CODE SECTEUR NAF2": pl.String,
                "Nb sites": pl.Float64,
                "Conso totale (MWh)": pl.Float64,
                "Conso moyenne (MWh)": pl.Float64,
                "part_thermosensible": pl.Float64,
                "Conso totale à usages thermosensibles (MWh)": pl.Float64,
                "Conso totale à usages non thermosensibles (MWh)": pl.Float64,
                "Thermosensibilité totale (kWh DJU)": pl.Float64,
                "conso_totale_corrigee_de_l_alea_climatique_a_usages_thermosensibles_mwh": pl.Float64,
                "Thermosensibilité moyenne (kWh DJU)": pl.Float64,
                "DJU à TR": pl.Float64,
                "DJU à TN": pl.Float64,
                "nombre_d_habitants": pl.Float64,
                "Taux de logements collectifs": pl.Float64,
                "Taux de résidences principales": pl.Float64,
                "superficie_des_logements_30_m2": pl.Float64,
                "Superficie des logements 30 à 40 m2": pl.Float64,
                "Superficie des logements 40 à 60 m2": pl.Float64,
                "Superficie des logements 60 à 80 m2": pl.Float64,
                "Superficie des logements 80 à 100 m2": pl.Float64,
                "superficie_des_logements_100_m2": pl.Float64,
                "Résidences principales avant 1919": pl.Float64,
                "Résidences principales de 1919 à 1945": pl.Float64,
                "Résidences principales de 1946 à 1970": pl.Float64,
                "Résidences principales de 1971 à 1990": pl.Float64,
                "Résidences principales de 1991 à 2005": pl.Float64,
                "Résidences principales de 2006 à 2018": pl.Float64,
                "Résidences principales après 2019": pl.Float64,
                "Taux de chauffage électrique": pl.Float64,
            },
        )
        .filter(pl.col("annee") >= 2020)
        .with_columns(
            pl.col("Code Région").str.strip_chars().str.zfill(2).alias("code_geo")
        )
    )


def build_dim_contexte_territoire_annuel(reg_conso_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Build `dim_contexte_territoire_annuel` by deduplicating regional climate (DJU)
    and housing stock indicators per `(code_geo, annee)`, backfilling INSEE housing
    census indicators across years within each `code_geo`.
    """
    housing_cols = [
        "taux_chauffage_electrique",
        "taux_logements_collectifs",
        "taux_residences_principales",
        "nombre_d_habitants",
        "superficie_moins_30m2",
        "superficie_30_40m2",
        "superficie_40_60m2",
        "superficie_60_80m2",
        "superficie_80_100m2",
        "superficie_plus_100m2",
        "residences_avant_1919",
        "residences_1919_1945",
        "residences_1946_1970",
        "residences_1971_1990",
        "residences_1991_2005",
        "residences_2006_2018",
        "residences_apres_2019",
    ]

    return (
        reg_conso_lf.group_by("code_geo", "annee")
        .agg(
            pl.col("DJU à TR").drop_nulls().first().alias("dju_tr"),
            pl.col("DJU à TN").drop_nulls().first().alias("dju_tn"),
            pl.col("Thermosensibilité moyenne (kWh DJU)")
            .drop_nulls()
            .first()
            .alias("thermosensibilite_moy_kwh_dju"),
            pl.col("Taux de chauffage électrique")
            .drop_nulls()
            .first()
            .alias("taux_chauffage_electrique"),
            pl.col("Taux de logements collectifs")
            .drop_nulls()
            .first()
            .alias("taux_logements_collectifs"),
            pl.col("Taux de résidences principales")
            .drop_nulls()
            .first()
            .alias("taux_residences_principales"),
            pl.col("nombre_d_habitants")
            .drop_nulls()
            .first()
            .round(0)
            .cast(pl.Int64)
            .alias("nombre_d_habitants"),
            pl.col("superficie_des_logements_30_m2")
            .drop_nulls()
            .first()
            .alias("superficie_moins_30m2"),
            pl.col("Superficie des logements 30 à 40 m2")
            .drop_nulls()
            .first()
            .alias("superficie_30_40m2"),
            pl.col("Superficie des logements 40 à 60 m2")
            .drop_nulls()
            .first()
            .alias("superficie_40_60m2"),
            pl.col("Superficie des logements 60 à 80 m2")
            .drop_nulls()
            .first()
            .alias("superficie_60_80m2"),
            pl.col("Superficie des logements 80 à 100 m2")
            .drop_nulls()
            .first()
            .alias("superficie_80_100m2"),
            pl.col("superficie_des_logements_100_m2")
            .drop_nulls()
            .first()
            .alias("superficie_plus_100m2"),
            pl.col("Résidences principales avant 1919")
            .drop_nulls()
            .first()
            .alias("residences_avant_1919"),
            pl.col("Résidences principales de 1919 à 1945")
            .drop_nulls()
            .first()
            .alias("residences_1919_1945"),
            pl.col("Résidences principales de 1946 à 1970")
            .drop_nulls()
            .first()
            .alias("residences_1946_1970"),
            pl.col("Résidences principales de 1971 à 1990")
            .drop_nulls()
            .first()
            .alias("residences_1971_1990"),
            pl.col("Résidences principales de 1991 à 2005")
            .drop_nulls()
            .first()
            .alias("residences_1991_2005"),
            pl.col("Résidences principales de 2006 à 2018")
            .drop_nulls()
            .first()
            .alias("residences_2006_2018"),
            pl.col("Résidences principales après 2019")
            .drop_nulls()
            .first()
            .alias("residences_apres_2019"),
        )
        .sort("code_geo", "annee")
        .with_columns(
            pl.col(housing_cols)
            .fill_null(strategy="backward")
            .fill_null(strategy="forward")
            .over("code_geo")
        )
        .select(
            "code_geo",
            "annee",
            "dju_tr",
            "dju_tn",
            "thermosensibilite_moy_kwh_dju",
            *housing_cols,
        )
    )


# ============================================================================
# 2. Lazy Builders for Fact Tables
# ============================================================================


def build_fait_conso_journaliere_elec(data_dir: Path) -> pl.LazyFrame:
    """Build `fait_conso_journaliere_elec` LazyFrame from `eco2mix-regional-tr.csv`."""
    return (
        pl.scan_csv(
            data_dir / "eco2mix-regional-tr.csv",
            separator=";",
            null_values=["", "N/A", "NULL"],
            try_parse_dates=True,
            schema_overrides={
                "code_insee_region": pl.String,
                "libelle_region": pl.String,
                "puissance_moyenne_mw": pl.Float64,
                "conso_totale_mwh": pl.Float64,
                "pic_journalier_mw": pl.Float64,
            },
        )
        .filter(pl.col("date") >= dt.date(2020, 1, 1))
        .select(
            pl.col("date").alias("date_jour"),
            pl.col("code_insee_region").str.strip_chars().str.zfill(2).alias("code_geo"),
            pl.lit("ELEC").alias("id_energie"),
            pl.col("conso_totale_mwh"),
            pl.col("puissance_moyenne_mw"),
            pl.col("pic_journalier_mw"),
        )
        .sort("date_jour", "code_geo")
    )


def build_fait_conso_segmentee_inf36(data_dir: Path) -> pl.LazyFrame:
    """
    Build `fait_conso_segmentee_inf36` LazyFrame from `conso-inf36-region-agg.csv`.
    Excludes redundant `P0: Total <= 36 kVA` rows when sub-ranges (`P1..P9`) exist
    for the profile so volumes are not double-counted and primary keys remain unique.
    """
    return (
        pl.scan_csv(
            data_dir / "conso-inf36-region-agg.csv",
            separator=";",
            null_values=["", "N/A", "NULL"],
            schema_overrides={
                "periode": pl.String,
                "annee": pl.Int32,
                "code_region": pl.String,
                "nom_region": pl.String,
                "categorie_client": pl.String,
                "option_tarifaire": pl.String,
                "profil_enedis": pl.String,
                "plage_de_puissance_souscrite": pl.String,
                "p_souscrite_kva": pl.Int32,
                "conso_totale_mwh": pl.Float64,
                "nb_points_soutirage_moyen": pl.Int64,
                "conso_moyenne_par_site_mwh": pl.Float64,
            },
        )
        .filter(
            (pl.col("annee") >= 2020)
            & (
                (~pl.col("plage_de_puissance_souscrite").str.starts_with("P0:"))
                | (pl.col("categorie_client") == "AUTRE")
            )
        )
        .select(
            pl.col("periode"),
            pl.col("code_region").str.strip_chars().str.zfill(2).alias("code_geo"),
            pl.lit("ELEC").alias("id_energie"),
            pl.col("categorie_client"),
            pl.col("option_tarifaire"),
            pl.col("profil_enedis"),
            pl.col("p_souscrite_kva"),
            pl.col("conso_totale_mwh"),
            pl.col("nb_points_soutirage_moyen"),
            pl.col("conso_moyenne_par_site_mwh"),
        )
        .sort(
            "periode",
            "code_geo",
            "categorie_client",
            "option_tarifaire",
            "profil_enedis",
            "p_souscrite_kva",
        )
    )


def build_fait_conso_annuelle(reg_conso_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Build `fait_conso_annuelle` LazyFrame from `REG_conso` by aggregating across
    operators (`OPERATEUR`) and NAF2 codes per
    `(annee, code_geo, id_energie, categorie_consommation, grand_secteur)`.
    """
    thermo_col = "Conso totale à usages thermosensibles (MWh)"
    non_thermo_col = "Conso totale à usages non thermosensibles (MWh)"
    corrigee_col = (
        "conso_totale_corrigee_de_l_alea_climatique_a_usages_thermosensibles_mwh"
    )

    return (
        reg_conso_lf.with_columns(
            pl.when(pl.col("FILIERE") == "Electricité")
            .then(pl.lit("ELEC"))
            .otherwise(pl.lit("GAZ"))
            .alias("id_energie"),
            pl.col("CODE CATEGORIE CONSOMMATION")
            .fill_null("INCONNU")
            .alias("categorie_consommation"),
            pl.col("CODE GRAND SECTEUR")
            .fill_null("INCONNU")
            .alias("grand_secteur"),
        )
        .group_by(
            "annee",
            "code_geo",
            "id_energie",
            "categorie_consommation",
            "grand_secteur",
        )
        .agg(
            pl.col("Conso totale (MWh)").sum().alias("conso_totale_mwh"),
            pl.col("Nb sites").sum().round(0).cast(pl.Int64).alias("nb_sites"),
            pl.when(pl.col(thermo_col).count() > 0)
            .then(pl.col(thermo_col).sum())
            .otherwise(pl.lit(None, dtype=pl.Float64))
            .alias("conso_usages_thermosensibles_mwh"),
            pl.when(pl.col(non_thermo_col).count() > 0)
            .then(pl.col(non_thermo_col).sum())
            .otherwise(pl.lit(None, dtype=pl.Float64))
            .alias("conso_usages_non_thermosensibles_mwh"),
            pl.when(pl.col(corrigee_col).count() > 0)
            .then(pl.col(corrigee_col).sum())
            .otherwise(pl.lit(None, dtype=pl.Float64))
            .alias("conso_corrigee_alea_climatique_mwh"),
            pl.col("part_thermosensible").drop_nulls().mean().alias("_avg_part_thermo"),
        )
        .with_columns(
            pl.when(pl.col("nb_sites") > 0)
            .then(pl.col("conso_totale_mwh") / pl.col("nb_sites"))
            .otherwise(pl.lit(None, dtype=pl.Float64))
            .alias("conso_moyenne_mwh"),
            pl.when(
                pl.col("conso_usages_thermosensibles_mwh").is_not_null()
                & (pl.col("conso_totale_mwh") > 0)
            )
            .then(
                pl.col("conso_usages_thermosensibles_mwh") / pl.col("conso_totale_mwh")
            )
            .otherwise(pl.col("_avg_part_thermo"))
            .alias("part_thermosensible"),
        )
        .select(
            "annee",
            "code_geo",
            "id_energie",
            "categorie_consommation",
            "grand_secteur",
            "conso_totale_mwh",
            "conso_moyenne_mwh",
            "nb_sites",
            "part_thermosensible",
            "conso_usages_thermosensibles_mwh",
            "conso_usages_non_thermosensibles_mwh",
            "conso_corrigee_alea_climatique_mwh",
        )
        .sort(
            "annee",
            "code_geo",
            "id_energie",
            "categorie_consommation",
            "grand_secteur",
        )
    )


# ============================================================================
# 3. MySQL Schema DDL (Constellation Model from Datawarehouse.md lines 9-135)
# ============================================================================

DROP_TABLES_ORDER: list[str] = [
    "fait_conso_annuelle",
    "fait_conso_segmentee_inf36",
    "fait_conso_journaliere_elec",
    "dim_contexte_territoire_annuel",
    "dim_tarif_reglemente",
    "dim_calendrier",
    "dim_energie",
    "dim_territoire",
]

CREATE_TABLES_DDL: dict[str, str] = {
    "dim_territoire": """
        CREATE TABLE IF NOT EXISTS dim_territoire (
            code_geo VARCHAR(10) NOT NULL COMMENT 'Code INSEE Région (11, 24, 84...)',
            nom_region VARCHAR(100) NOT NULL COMMENT 'Libellé officiel (Île-de-France...)',
            zone_vacances VARCHAR(50) NOT NULL COMMENT 'Zone A / Zone B / Zone C / Corse',
            PRIMARY KEY (code_geo)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """,
    "dim_energie": """
        CREATE TABLE IF NOT EXISTS dim_energie (
            id_energie VARCHAR(10) NOT NULL COMMENT 'ELEC / GAZ',
            code_filiere VARCHAR(50) NOT NULL COMMENT 'Electricité / Gaz (jointure REG_conso)',
            libelle_energie VARCHAR(100) NOT NULL COMMENT 'Électricité / Gaz naturel',
            unite_mesure VARCHAR(20) NOT NULL COMMENT 'MWh',
            PRIMARY KEY (id_energie)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """,
    "dim_calendrier": """
        CREATE TABLE IF NOT EXISTS dim_calendrier (
            date_jour DATE NOT NULL COMMENT 'YYYY-MM-DD',
            annee INT NOT NULL COMMENT 'YYYY',
            periode_mois VARCHAR(7) NOT NULL COMMENT 'YYYY-MM',
            mois INT NOT NULL COMMENT '1..12',
            jour_semaine INT NOT NULL COMMENT '1..7 (Lundi..Dimanche)',
            est_weekend BOOLEAN NOT NULL COMMENT 'True si Samedi/Dimanche',
            saison VARCHAR(20) NOT NULL COMMENT 'Hiver / Printemps / Été / Automne',
            est_jour_ferie BOOLEAN NOT NULL COMMENT 'Via bibliothèque Python jours-feries-france',
            nom_jour_ferie VARCHAR(100) NULL COMMENT 'Nom du jour férié',
            vacances_zone_a BOOLEAN NOT NULL COMMENT 'Extrait de Zone-A-B-C-Corse.ics',
            vacances_zone_b BOOLEAN NOT NULL COMMENT 'Extrait de Zone-A-B-C-Corse.ics',
            vacances_zone_c BOOLEAN NOT NULL COMMENT 'Extrait de Zone-A-B-C-Corse.ics',
            vacances_corse BOOLEAN NOT NULL COMMENT 'Extrait de Zone-A-B-C-Corse.ics',
            PRIMARY KEY (date_jour),
            INDEX idx_cal_annee (annee),
            INDEX idx_cal_periode_mois (periode_mois)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """,
    "dim_tarif_reglemente": """
        CREATE TABLE IF NOT EXISTS dim_tarif_reglemente (
            id_tarif VARCHAR(50) NOT NULL COMMENT 'Clé technique (Option + kVA + Date)',
            option_tarifaire VARCHAR(20) NOT NULL COMMENT 'BASE / HPHC / TEMPO',
            p_souscrite_kva INT NOT NULL COMMENT '3, 6, 9, 12, 15, 18, 24, 30, 36',
            date_debut DATE NOT NULL COMMENT 'Date d''entrée en vigueur',
            date_fin DATE NULL COMMENT 'Date de fin de validité',
            part_fixe_ttc DOUBLE NULL COMMENT 'Abonnement annuel (€/an TTC)',
            part_variable_ttc_base DOUBLE NULL COMMENT 'Prix kWh Base (€/kWh TTC)',
            part_variable_ttc_hp DOUBLE NULL COMMENT 'Prix kWh Heures Pleines (€/kWh TTC)',
            part_variable_ttc_hc DOUBLE NULL COMMENT 'Prix kWh Heures Creuses (€/kWh TTC)',
            part_variable_bleu_hp DOUBLE NULL COMMENT 'Tempo Bleu HP (€/kWh TTC)',
            part_variable_bleu_hc DOUBLE NULL COMMENT 'Tempo Bleu HC (€/kWh TTC)',
            part_variable_blanc_hp DOUBLE NULL COMMENT 'Tempo Blanc HP (€/kWh TTC)',
            part_variable_blanc_hc DOUBLE NULL COMMENT 'Tempo Blanc HC (€/kWh TTC)',
            part_variable_rouge_hp DOUBLE NULL COMMENT 'Tempo Rouge HP (€/kWh TTC)',
            part_variable_rouge_hc DOUBLE NULL COMMENT 'Tempo Rouge HC (€/kWh TTC)',
            PRIMARY KEY (id_tarif),
            INDEX idx_tarif_option_kva (option_tarifaire, p_souscrite_kva, date_debut)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """,
    "dim_contexte_territoire_annuel": """
        CREATE TABLE IF NOT EXISTS dim_contexte_territoire_annuel (
            code_geo VARCHAR(10) NOT NULL COMMENT 'Code INSEE Région',
            annee INT NOT NULL COMMENT 'Année (2020..2025)',
            dju_tr DOUBLE NULL COMMENT 'DJU à Température Réelle',
            dju_tn DOUBLE NULL COMMENT 'DJU à Température Normale',
            thermosensibilite_moy_kwh_dju DOUBLE NULL COMMENT 'kWh par DJU',
            taux_chauffage_electrique DOUBLE NULL COMMENT 'Part chauffage élec (0 à 1)',
            taux_logements_collectifs DOUBLE NULL COMMENT 'Part collectif (0 à 1)',
            taux_residences_principales DOUBLE NULL COMMENT 'Part rés. principales (0 à 1)',
            nombre_d_habitants BIGINT NULL COMMENT 'Population régionale',
            superficie_moins_30m2 DOUBLE NULL COMMENT 'Nb logements < 30 m2',
            superficie_30_40m2 DOUBLE NULL COMMENT 'Nb logements 30 à 40 m2',
            superficie_40_60m2 DOUBLE NULL COMMENT 'Nb logements 40 à 60 m2',
            superficie_60_80m2 DOUBLE NULL COMMENT 'Nb logements 60 à 80 m2',
            superficie_80_100m2 DOUBLE NULL COMMENT 'Nb logements 80 à 100 m2',
            superficie_plus_100m2 DOUBLE NULL COMMENT 'Nb logements > 100 m2',
            residences_avant_1919 DOUBLE NULL COMMENT 'Nb rés. avant 1919',
            residences_1919_1945 DOUBLE NULL COMMENT 'Nb rés. 1919-1945',
            residences_1946_1970 DOUBLE NULL COMMENT 'Nb rés. 1946-1970',
            residences_1971_1990 DOUBLE NULL COMMENT 'Nb rés. 1971-1990',
            residences_1991_2005 DOUBLE NULL COMMENT 'Nb rés. 1991-2005',
            residences_2006_2018 DOUBLE NULL COMMENT 'Nb rés. 2006-2018',
            residences_apres_2019 DOUBLE NULL COMMENT 'Nb rés. après 2019',
            PRIMARY KEY (code_geo, annee),
            CONSTRAINT fk_ctx_territoire FOREIGN KEY (code_geo) REFERENCES dim_territoire (code_geo)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """,
    "fait_conso_journaliere_elec": """
        CREATE TABLE IF NOT EXISTS fait_conso_journaliere_elec (
            date_jour DATE NOT NULL COMMENT 'Clé vers dim_calendrier (YYYY-MM-DD)',
            code_geo VARCHAR(10) NOT NULL COMMENT 'Clé vers dim_territoire (code_insee_region)',
            id_energie VARCHAR(10) NOT NULL COMMENT 'Toujours ELEC',
            conso_totale_mwh DOUBLE NULL COMMENT 'Énergie totale du jour (MWh)',
            puissance_moyenne_mw DOUBLE NULL COMMENT 'Puissance moyenne journalière (MW)',
            pic_journalier_mw DOUBLE NULL COMMENT 'Pointe de consommation du jour (MW)',
            PRIMARY KEY (date_jour, code_geo, id_energie),
            CONSTRAINT fk_jour_calendrier FOREIGN KEY (date_jour) REFERENCES dim_calendrier (date_jour),
            CONSTRAINT fk_jour_territoire FOREIGN KEY (code_geo) REFERENCES dim_territoire (code_geo),
            CONSTRAINT fk_jour_energie FOREIGN KEY (id_energie) REFERENCES dim_energie (id_energie)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """,
    "fait_conso_segmentee_inf36": """
        CREATE TABLE IF NOT EXISTS fait_conso_segmentee_inf36 (
            periode VARCHAR(10) NOT NULL COMMENT 'YYYY-MM (ou YYYY)',
            code_geo VARCHAR(10) NOT NULL COMMENT 'Code INSEE Région',
            id_energie VARCHAR(10) NOT NULL COMMENT 'Toujours ELEC',
            categorie_client VARCHAR(20) NOT NULL COMMENT 'RES / PRO',
            option_tarifaire VARCHAR(50) NOT NULL COMMENT 'BASE / HPHC',
            profil_enedis VARCHAR(50) NOT NULL COMMENT 'RES1, RES11, RES2, PRO1...',
            p_souscrite_kva INT NOT NULL COMMENT 'Puissance souscrite (kVA)',
            conso_totale_mwh DOUBLE NULL COMMENT 'Volume total soutiré (MWh)',
            nb_points_soutirage_moyen BIGINT NULL COMMENT 'Nombre moyen de compteurs (PDL)',
            conso_moyenne_par_site_mwh DOUBLE NULL COMMENT 'Conso par compteur (MWh/site)',
            PRIMARY KEY (periode, code_geo, id_energie, categorie_client, option_tarifaire, profil_enedis, p_souscrite_kva),
            INDEX idx_seg_option_kva (option_tarifaire, p_souscrite_kva),
            CONSTRAINT fk_seg_territoire FOREIGN KEY (code_geo) REFERENCES dim_territoire (code_geo),
            CONSTRAINT fk_seg_energie FOREIGN KEY (id_energie) REFERENCES dim_energie (id_energie)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """,
    "fait_conso_annuelle": """
        CREATE TABLE IF NOT EXISTS fait_conso_annuelle (
            annee INT NOT NULL COMMENT 'YYYY',
            code_geo VARCHAR(10) NOT NULL COMMENT 'Code INSEE Région',
            id_energie VARCHAR(10) NOT NULL COMMENT 'ELEC / GAZ',
            categorie_consommation VARCHAR(30) NOT NULL COMMENT 'RES / ENT_PRO / ENT',
            grand_secteur VARCHAR(30) NOT NULL COMMENT 'RESIDENTIEL / TERTIAIRE / INDUSTRIE / AGRICULTURE',
            conso_totale_mwh DOUBLE NULL COMMENT 'Consommation totale (MWh)',
            conso_moyenne_mwh DOUBLE NULL COMMENT 'Consommation moyenne par site (MWh)',
            nb_sites BIGINT NULL COMMENT 'Nombre de points de livraison',
            part_thermosensible DOUBLE NULL COMMENT 'Part thermosensible (0 à 1)',
            conso_usages_thermosensibles_mwh DOUBLE NULL COMMENT 'MWh thermosensibles',
            conso_usages_non_thermosensibles_mwh DOUBLE NULL COMMENT 'MWh non thermosensibles',
            conso_corrigee_alea_climatique_mwh DOUBLE NULL COMMENT 'MWh corrigés du climat (à DJU normal)',
            PRIMARY KEY (annee, code_geo, id_energie, categorie_consommation, grand_secteur),
            CONSTRAINT fk_ann_territoire FOREIGN KEY (code_geo) REFERENCES dim_territoire (code_geo),
            CONSTRAINT fk_ann_energie FOREIGN KEY (id_energie) REFERENCES dim_energie (id_energie),
            CONSTRAINT fk_ann_contexte FOREIGN KEY (code_geo, annee) REFERENCES dim_contexte_territoire_annuel (code_geo, annee)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """,
}


def build_all_dataframes(data_dir: Path) -> dict[str, pl.DataFrame]:
    """
    Build all 8 lazy plans and execute them in one single `pl.collect_all()` call.
    """
    reg_conso_lf = _scan_reg_conso(data_dir)

    table_names = [
        "dim_territoire",
        "dim_energie",
        "dim_calendrier",
        "dim_tarif_reglemente",
        "dim_contexte_territoire_annuel",
        "fait_conso_journaliere_elec",
        "fait_conso_segmentee_inf36",
        "fait_conso_annuelle",
    ]

    lazy_plans = [
        build_dim_territoire(),
        build_dim_energie(),
        build_dim_calendrier(data_dir),
        build_dim_tarif_reglemente(data_dir),
        build_dim_contexte_territoire_annuel(reg_conso_lf),
        build_fait_conso_journaliere_elec(data_dir),
        build_fait_conso_segmentee_inf36(data_dir),
        build_fait_conso_annuelle(reg_conso_lf),
    ]

    collected = pl.collect_all(lazy_plans)
    return dict(zip(table_names, collected, strict=True))


def create_mysql_engine(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    db_url: str | None = None,
) -> Engine:
    """
    Ensure the target MySQL database exists, then return a SQLAlchemy Engine connected to it.
    """
    if db_url:
        return create_engine(db_url, pool_pre_ping=True)

    encoded_user = quote_plus(user)
    encoded_pwd = quote_plus(password)
    server_url = f"mysql+pymysql://{encoded_user}:{encoded_pwd}@{host}:{port}/?charset=utf8mb4"
    server_engine = create_engine(server_url, isolation_level="AUTOCOMMIT")

    safe_db_name = database.replace("`", "``")
    with server_engine.connect() as conn:
        conn.execute(
            text(
                f"CREATE DATABASE IF NOT EXISTS `{safe_db_name}` "
                "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
            )
        )
    server_engine.dispose()

    target_url = (
        f"mysql+pymysql://{encoded_user}:{encoded_pwd}@{host}:{port}/"
        f"{quote_plus(database)}?charset=utf8mb4"
    )
    return create_engine(target_url, pool_pre_ping=True)


def load_into_mysql(
    tables: dict[str, pl.DataFrame],
    engine: Engine,
    recreate_schema: bool = True,
    batch_size: int = 5_000,
) -> None:
    """
    Create the Constellation schema in MySQL and load all 8 Polars DataFrames
    in batches using parameterized SQL inserts (no pandas dependency required).
    """
    with engine.begin() as conn:
        if recreate_schema:
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
            for tbl in DROP_TABLES_ORDER:
                conn.execute(text(f"DROP TABLE IF EXISTS `{tbl}`;"))
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))

        for tbl_name, ddl in CREATE_TABLES_DDL.items():
            conn.execute(text(ddl))

    for tbl_name, df in tables.items():
        print(f"Loading `{tbl_name}` ({df.height:,} rows, {df.width} cols)...")
        if df.is_empty():
            continue

        cols_quoted = ", ".join(f"`{c}`" for c in df.columns)
        bind_params = ", ".join(f":{c}" for c in df.columns)
        insert_stmt = text(f"INSERT INTO `{tbl_name}` ({cols_quoted}) VALUES ({bind_params})")

        with engine.begin() as conn:
            for chunk in df.iter_slices(n_rows=batch_size):
                conn.execute(insert_stmt, chunk.to_dicts())

        print(f"  -> `{tbl_name}` loaded successfully.")


def parse_args() -> argparse.Namespace:
    pre_parser = argparse.ArgumentParser(add_help=False)
    pre_parser.add_argument("--env-file", type=Path, default=None)
    pre_args, _ = pre_parser.parse_known_args()
    if pre_args.env_file:
        load_dotenv(dotenv_path=pre_args.env_file, override=True)

    parser = argparse.ArgumentParser(
        description=(
            "Load the Energy Consumption Data Warehouse (Datawarehouse.md) "
            "into MySQL using Polars."
        )
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=None,
        help="Optional path to a custom .env file (default: auto-loads .env via python-dotenv)",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help=f"Directory containing source CSV/ICS files (default: {DEFAULT_DATA_DIR})",
    )
    parser.add_argument(
        "--host",
        type=str,
        default=os.environ.get("MYSQL_HOST", "localhost"),
        help="MySQL host (default: $MYSQL_HOST or 'localhost')",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("MYSQL_PORT", "3306")),
        help="MySQL port (default: $MYSQL_PORT or 3306)",
    )
    parser.add_argument(
        "--user",
        type=str,
        default=os.environ.get("MYSQL_USER", "root"),
        help="MySQL user (default: $MYSQL_USER or 'root')",
    )
    parser.add_argument(
        "--password",
        type=str,
        default=os.environ.get("MYSQL_PASSWORD", ""),
        help="MySQL password (default: $MYSQL_PASSWORD or empty)",
    )
    parser.add_argument(
        "--database",
        type=str,
        default=os.environ.get("MYSQL_DATABASE", "electricite_gaz_dw"),
        help="MySQL database name (default: $MYSQL_DATABASE or 'electricite_gaz_dw')",
    )
    parser.add_argument(
        "--db-url",
        type=str,
        default=os.environ.get("DATABASE_URL"),
        help="Full SQLAlchemy connection URL (overrides --host/--port/--user/--password/--database)",
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="Append to existing tables without dropping/recreating them first.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build and validate all 8 Polars DataFrames and print schemas/counts without connecting to MySQL.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.data_dir.exists():
        print(f"Error: Data directory '{args.data_dir}' does not exist.", file=sys.stderr)
        sys.exit(1)

    print(f"Building all 8 Data Warehouse tables with Polars {pl.__version__} (lazy API)...")
    tables = build_all_dataframes(args.data_dir)

    print("\n=== Data Warehouse Transformation Summary ===")
    for name, df in tables.items():
        null_counts = {k: v for k, v in df.null_count().to_dicts()[0].items() if v > 0}
        print(f"  - {name:<32} : {df.height:>7,} rows | {df.width:>2} cols | nulls: {null_counts or 'none'}")

    if args.dry_run:
        print("\n[--dry-run] Skipping MySQL connection. Sample previews:")
        for name, df in tables.items():
            print(f"\n--- {name} ---")
            print(df.head(3))
        return

    print(
        f"\nConnecting to MySQL ({args.db_url or f'{args.user}@{args.host}:{args.port}/{args.database}'})..."
    )
    engine = create_mysql_engine(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
        db_url=args.db_url,
    )
    try:
        load_into_mysql(tables, engine, recreate_schema=not args.append)
    finally:
        engine.dispose()

    print("\nAll 8 Data Warehouse tables have been loaded into MySQL successfully!")


if __name__ == "__main__":
    main()
