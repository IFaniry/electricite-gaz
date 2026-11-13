#!/usr/bin/env python3
"""
Polars Lazy Builders for Tableau Google Sheets Export.

Extracts, enriches, and flattens the energy constellation data warehouse
from `brainstorm-data/` into analytics-ready tables for Tableau.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

import polars as pl
from jours_feries_france import JoursFeries

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


def parse_ics_vacation_intervals(ics_path: Path, start_year: int = 2020) -> pl.LazyFrame:
    """
    Parse `Zone-A-B-C-Corse.ics` into a daily LazyFrame of holiday flags:
    `(date_jour, vacances_zone_a, vacances_zone_b, vacances_zone_c, vacances_corse)`.
    """
    if not ics_path.exists():
        return pl.DataFrame(
            schema={
                "date_jour": pl.Date,
                "vacances_zone_a": pl.Boolean,
                "vacances_zone_b": pl.Boolean,
                "vacances_zone_c": pl.Boolean,
                "vacances_corse": pl.Boolean,
            }
        ).lazy()

    content = ics_path.read_text(encoding="utf-8-sig")
    raw_events = content.split("BEGIN:VEVENT")[1:]

    records: list[dict[str, Any]] = []
    for raw_ev in raw_events:
        props: dict[str, str] = {}
        for line in raw_ev.splitlines():
            if ":" in line:
                k, v = line.split(":", 1)
                props[k.strip()] = v.strip()

        summary = props.get("SUMMARY", "")
        if "prérentrée Enseignants" in summary:
            continue

        dtstart_str = props.get("DTSTART;VALUE=DATE") or props.get("DTSTART", "")
        dtend_str = props.get("DTEND;VALUE=DATE") or props.get("DTEND", "")
        location = props.get("LOCATION", "")

        if len(dtstart_str) < 8 or len(dtend_str) < 8:
            continue

        try:
            start_date = dt.date.fromisoformat(f"{dtstart_str[:4]}-{dtstart_str[4:6]}-{dtstart_str[6:8]}")
            end_date = dt.date.fromisoformat(f"{dtend_str[:4]}-{dtend_str[4:6]}-{dtend_str[6:8]}")
        except ValueError:
            continue

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

    return (
        pl.DataFrame(records)
        .lazy()
        .filter(pl.col("end_date") >= dt.date(start_year, 1, 1))
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
    vacations_lf = parse_ics_vacation_intervals(ics_path, start_year=start_year)

    holiday_rows: list[dict[str, Any]] = []
    for yr in range(start_year, end_year + 1):
        year_holidays = JoursFeries.for_year(yr)
        for h_name, h_date in year_holidays.items():
            holiday_rows.append(
                {
                    "date_jour": h_date,
                    "est_jour_ferie": True,
                    "nom_jour_ferie": str(h_name),
                }
            )

    holidays_lf = pl.DataFrame(
        holiday_rows,
        schema={
            "date_jour": pl.Date,
            "est_jour_ferie": pl.Boolean,
            "nom_jour_ferie": pl.String,
        },
    ).lazy()

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

    md = (
        pl.col("date_jour").dt.month().cast(pl.Int32) * 100
        + pl.col("date_jour").dt.day().cast(pl.Int32)
    )

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
    )


def scan_reg_conso(data_dir: Path, start_year: int = 2020) -> pl.LazyFrame:
    """Scan `REG_conso_ajd.csv` with strict typing."""
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
        .filter(pl.col("annee") >= start_year)
        .with_columns(
            pl.col("Code Région").str.strip_chars().str.zfill(2).alias("code_geo")
        )
    )


def build_dim_contexte_territoire_annuel(reg_conso_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Build `dim_contexte_territoire_annuel` from `REG_conso` by aggregating
    climate DJU and backfilling INSEE housing census data across years per `code_geo`.
    """
    grouped = (
        reg_conso_lf.group_by("code_geo", "annee")
        .agg(
            pl.col("DJU à TR").drop_nulls().first().alias("dju_tr"),
            pl.col("DJU à TN").drop_nulls().first().alias("dju_tn"),
            pl.col("Thermosensibilité moyenne (kWh DJU)").drop_nulls().first().alias("thermosensibilite_moy_kwh_dju"),
            pl.col("Taux de chauffage électrique").drop_nulls().first().alias("taux_chauffage_electrique"),
            pl.col("Taux de logements collectifs").drop_nulls().first().alias("taux_logements_collectifs"),
            pl.col("Taux de résidences principales").drop_nulls().first().alias("taux_residences_principales"),
            pl.col("nombre_d_habitants").drop_nulls().first().cast(pl.Int64).alias("nombre_d_habitants"),
        )
    )

    return (
        grouped.sort("code_geo", "annee")
        .with_columns(
            pl.col("taux_chauffage_electrique").forward_fill().backward_fill().over("code_geo"),
            pl.col("taux_logements_collectifs").forward_fill().backward_fill().over("code_geo"),
            pl.col("taux_residences_principales").forward_fill().backward_fill().over("code_geo"),
            pl.col("nombre_d_habitants").forward_fill().backward_fill().over("code_geo"),
        )
    )


# ============================================================================
# Core Export Model 1: Daily Energy Analytics (Flat Table)
# ============================================================================

def build_daily_energy_analytics_lf(
    data_dir: Path,
    start_year: int = 2020,
    end_year: int = 2026,
) -> pl.LazyFrame:
    """
    Build the Master Flat Table `Daily_Energy_Analytics` at daily regional grain:
    `(date_jour, code_geo)`.
    
    Joins:
      - Daily Electricity (MWh, MW mean, peak MW) from éCO2mix
      - Daily Gas (MWh) from NaTran / GRTgaz & Teréga
      - Dimensions: Territory & Vacation Zone
      - Dimensions: Calendar, Day of week, Seasons, Holidays & Regional Vacation Match
      - Dimensions: Climate DJU, Heating shares & Population influencers
    """
    territoire_lf = build_dim_territoire()
    calendrier_lf = build_dim_calendrier(data_dir, start_year=start_year, end_year=end_year)
    reg_conso_lf = scan_reg_conso(data_dir, start_year=start_year)
    contexte_lf = build_dim_contexte_territoire_annuel(reg_conso_lf)

    valid_codes = [t["code_geo"] for t in TERRITOIRES_REFERENCE]

    # 1. Daily Elec
    elec_path = data_dir / "eco2mix-regional-tr.csv"
    elec_lf = (
        pl.scan_csv(
            elec_path,
            separator=";",
            null_values=["", "N/A", "NULL"],
            try_parse_dates=True,
            schema_overrides={
                "code_insee_region": pl.String,
                "puissance_moyenne_mw": pl.Float64,
                "conso_totale_mwh": pl.Float64,
                "pic_journalier_mw": pl.Float64,
            },
        )
        .with_columns(
            pl.col("code_insee_region").cast(pl.String).str.strip_chars().str.zfill(2).alias("code_geo")
        )
        .filter(
            (pl.col("date") >= dt.date(start_year, 1, 1))
            & pl.col("code_geo").is_in(valid_codes)
        )
        .select(
            pl.col("date").alias("date_jour"),
            pl.col("code_geo"),
            pl.col("conso_totale_mwh").alias("conso_elec_mwh"),
            pl.col("puissance_moyenne_mw").alias("puissance_moy_elec_mw"),
            pl.col("pic_journalier_mw").alias("pic_elec_mw"),
        )
    )

    # 2. Daily Gas
    gaz_path = data_dir / "conso-gaz-regional-quotidien.csv"
    gaz_lf = (
        pl.scan_csv(
            gaz_path,
            separator=";",
            null_values=["", "N/A", "NULL"],
            schema_overrides={"code_region": pl.String},
        )
        .with_columns(
            pl.col("date").str.slice(0, 10).str.to_date("%Y-%m-%d").alias("date_jour"),
            pl.col("code_region").cast(pl.String).str.strip_chars().str.zfill(2).alias("code_geo"),
        )
        .filter(
            (pl.col("date_jour") >= dt.date(start_year, 1, 1))
            & pl.col("code_geo").is_in(valid_codes)
        )
        .group_by("date_jour", "code_geo")
        .agg(pl.col("conso_totale_mwh").sum().alias("conso_gaz_mwh"))
    )

    # Full outer join on (date_jour, code_geo) to retain any day with elec or gaz
    daily_energy = elec_lf.join(gaz_lf, on=["date_jour", "code_geo"], how="full", coalesce=True)

    # Join Territory and Calendar
    merged = (
        daily_energy
        .join(territoire_lf, on="code_geo", how="inner")
        .join(calendrier_lf, on="date_jour", how="inner")
        .join(contexte_lf, on=["code_geo", "annee"], how="left")
    )

    # Dynamic vacation matching for the specific region's zone
    est_vacances_region = (
        pl.when(pl.col("zone_vacances") == "Zone A")
        .then(pl.col("vacances_zone_a"))
        .when(pl.col("zone_vacances") == "Zone B")
        .then(pl.col("vacances_zone_b"))
        .when(pl.col("zone_vacances") == "Zone C")
        .then(pl.col("vacances_zone_c"))
        .when(pl.col("zone_vacances") == "Corse")
        .then(pl.col("vacances_corse"))
        .otherwise(False)
    )

    # Compute high-value Tableau analytics metrics
    return (
        merged.with_columns(
            est_vacances_region.alias("est_vacances_scolaires"),
            (pl.coalesce(pl.col("conso_elec_mwh"), pl.lit(0.0)) + pl.coalesce(pl.col("conso_gaz_mwh"), pl.lit(0.0)))
            .alias("conso_totale_energie_mwh"),
            (pl.col("dju_tr") - pl.col("dju_tn")).round(2).alias("ecart_dju"),
        )
        .with_columns(
            pl.when(pl.col("conso_totale_energie_mwh") > 0)
            .then((pl.coalesce(pl.col("conso_elec_mwh"), pl.lit(0.0)) / pl.col("conso_totale_energie_mwh") * 100).round(2))
            .otherwise(None)
            .alias("part_elec_pct"),
            pl.when(pl.col("nombre_d_habitants") > 0)
            .then((pl.col("conso_totale_energie_mwh") * 1000.0 / pl.col("nombre_d_habitants")).round(3))
            .otherwise(None)
            .alias("conso_kwh_par_habitant"),
        )
        .select(
            "date_jour",
            "code_geo",
            "nom_region",
            "zone_vacances",
            "annee",
            "periode_mois",
            "mois",
            "jour_semaine",
            "est_weekend",
            "saison",
            "est_jour_ferie",
            "nom_jour_ferie",
            "est_vacances_scolaires",
            "conso_elec_mwh",
            "puissance_moy_elec_mw",
            "pic_elec_mw",
            "conso_gaz_mwh",
            "conso_totale_energie_mwh",
            "part_elec_pct",
            "conso_kwh_par_habitant",
            "dju_tr",
            "dju_tn",
            "ecart_dju",
            "thermosensibilite_moy_kwh_dju",
            "taux_chauffage_electrique",
            "taux_logements_collectifs",
            "nombre_d_habitants",
        )
        .sort("date_jour", "code_geo")
    )


# ============================================================================
# Core Export Model 2: Annual Sectoral Breakdown (Agile Aggregation)
# ============================================================================

def build_annual_conso_secteur_lf(
    data_dir: Path,
    start_year: int = 2020,
) -> pl.LazyFrame:
    """
    Build `Annual_Conso_Secteur` by consolidating annual consumption across operators
    per `(annee, code_geo, filiere, categorie_conso, grand_secteur)` and joining
    demographic & thermal influencers.
    """
    reg_conso_lf = scan_reg_conso(data_dir, start_year=start_year)
    contexte_lf = build_dim_contexte_territoire_annuel(reg_conso_lf)
    territoire_lf = build_dim_territoire()

    agg_lf = (
        reg_conso_lf.group_by(
            "annee",
            "code_geo",
            "FILIERE",
            "CODE CATEGORIE CONSOMMATION",
            "CODE GRAND SECTEUR",
        )
        .agg(
            pl.col("Conso totale (MWh)").sum().alias("conso_totale_mwh"),
            pl.col("Nb sites").sum().alias("nb_sites"),
            pl.col("Conso totale à usages thermosensibles (MWh)").sum().alias("conso_thermosensible_mwh"),
            pl.col("Conso totale à usages non thermosensibles (MWh)").sum().alias("conso_non_thermosensible_mwh"),
            pl.col("part_thermosensible").mean().round(3).alias("part_thermosensible_moy"),
        )
        .with_columns(
            pl.when(pl.col("nb_sites") > 0)
            .then((pl.col("conso_totale_mwh") / pl.col("nb_sites")).round(2))
            .otherwise(None)
            .alias("conso_moyenne_mwh_par_site"),
            pl.col("FILIERE").alias("filiere"),
            pl.col("CODE CATEGORIE CONSOMMATION").alias("categorie_conso"),
            pl.col("CODE GRAND SECTEUR").alias("grand_secteur"),
        )
    )

    return (
        agg_lf.join(territoire_lf, on="code_geo", how="inner")
        .join(contexte_lf, on=["code_geo", "annee"], how="left")
        .select(
            "annee",
            "code_geo",
            "nom_region",
            "filiere",
            "categorie_conso",
            "grand_secteur",
            "conso_totale_mwh",
            "nb_sites",
            "conso_moyenne_mwh_par_site",
            "conso_thermosensible_mwh",
            "conso_non_thermosensible_mwh",
            "part_thermosensible_moy",
            "dju_tr",
            "dju_tn",
            "thermosensibilite_moy_kwh_dju",
            "taux_chauffage_electrique",
            "taux_logements_collectifs",
            "taux_residences_principales",
            "nombre_d_habitants",
        )
        .sort("annee", "code_geo", "filiere", "categorie_conso")
    )


# ============================================================================
# Core Export Model 3: Segmented Residential Linky Inf36
# ============================================================================

def build_segmented_linky_inf36_lf(
    data_dir: Path,
    start_year: int = 2020,
) -> pl.LazyFrame:
    """
    Build `Segmented_Linky_Inf36` from Enedis Linky profiles and tariffs.
    """
    territoire_lf = build_dim_territoire()
    inf36_path = data_dir / "conso-inf36-region-agg.csv"

    return (
        pl.scan_csv(
            inf36_path,
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
        .with_columns(
            pl.col("code_region").cast(pl.String).str.strip_chars().str.zfill(2).alias("code_geo")
        )
        .filter(
            (pl.col("annee") >= start_year)
            & (
                (~pl.col("plage_de_puissance_souscrite").str.starts_with("P0:"))
                | (pl.col("categorie_client") == "AUTRE")
            )
        )
        .join(territoire_lf.select("code_geo", "nom_region"), on="code_geo", how="inner")
        .select(
            "periode",
            "annee",
            "code_geo",
            "nom_region",
            "categorie_client",
            "option_tarifaire",
            "profil_enedis",
            "plage_de_puissance_souscrite",
            "p_souscrite_kva",
            "conso_totale_mwh",
            "nb_points_soutirage_moyen",
            "conso_moyenne_par_site_mwh",
        )
        .sort("periode", "code_geo", "categorie_client", "p_souscrite_kva")
    )
