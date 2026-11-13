# [Données sur la consommation d'énergie](https://www.data.gouv.fr/pages/donnees-energie)

## [Electricité et gaz](https://www.data.gouv.fr/pages/donnees-energie)

- brainstorm-data/REG_conso_2011_ajd.csv :

```
python fetch_reg_conso.py --start-year 2020
```

## [Electricité](https://www.data.gouv.fr/pages/donnees-energie)

- brainstorm-data/eco2mix-regional-tr.csv :

Agrège les données demi-horaires éCO2mix (`eco2mix-regional-cons-def`) au pas journalier depuis 2020 :
- `group_by=code_insee_region, libelle_region, date` : Regroupement par région et par jour.
- `avg(consommation) as puissance_moyenne_mw` : Puissance moyenne appelée sur la journée (MW).
- `sum(consommation)/2 as conso_totale_mwh` : Conversion des puissances demi-horaires en énergie totale journalière en MWh ($\text{MWh} = \text{MW} \times 0{,}5\text{ h} = \sum \text{MW} / 2$).
- `max(consommation) as pic_journalier_mw` : Pic de puissance maximal atteint dans la journée (MW).
- `where=date_heure >= date'2020-01-01' AND consommation IS NOT NULL` : Filtre à partir de 2020 en excluant les valeurs nulles.

```bash
curl -L "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-regional-cons-def/exports/csv?select=code_insee_region%2C%20libelle_region%2C%20date%2C%20avg(consommation)%20as%20puissance_moyenne_mw%2C%20sum(consommation)%2F2%20as%20conso_totale_mwh%2C%20max(consommation)%20as%20pic_journalier_mw&where=date_heure%20%3E%3D%20date%272020-01-01%27%20AND%20consommation%20IS%20NOT%20NULL&group_by=code_insee_region%2C%20libelle_region%2C%20date&delimiter=%3B" -o brainstorm-data/eco2mix-regional-tr.csv
```

## [Gaz](https://odre.opendatasoft.com/explore/dataset/courbe-de-charge-eldgrd-regional-grtgaz-terega/api/?disjunctive.region)

- brainstorm-data/conso-gaz-regional-quotidien.csv :

Agrège la consommation journalière de gaz des distributions publiques et ELD (`courbe-de-charge-eldgrd-regional-grtgaz-terega`) depuis 2020 :
- `group_by=code_region, region, date` : Regroupement par région et date, sans distinction d'opérateur.
- `sum(conso_journaliere_mwh_pcs_0degc) as conso_totale_mwh` : Consolide et additionne les volumes de gaz de tous les gestionnaires de transport (**NaTran** / ex-GRTgaz et **Teréga**). Dans les régions du Sud-Ouest (Nouvelle-Aquitaine, Occitanie) desservies conjointement par les deux opérateurs, cela produit une ligne unique par jour avec le volume total ($\text{conso}_{\text{NaTran}} + \text{conso}_{\text{Teréga}}$).
- Élimine les 24 colonnes horaires (`00_00_00` à `23_00`) pour ne conserver que le total journalier en MWh.

```bash
curl -L "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/courbe-de-charge-eldgrd-regional-grtgaz-terega/exports/csv?select=code_region%2C%20region%2C%20date%2C%20sum(conso_journaliere_mwh_pcs_0degc)%20as%20conso_totale_mwh&where=date%20%3E%3D%20date%272020-01-01%27%20AND%20conso_journaliere_mwh_pcs_0degc%20IS%20NOT%20NULL&group_by=code_region%2C%20region%2C%20date&delimiter=%3B" -o brainstorm-data/conso-gaz-regional-quotidien.csv
```

# Agrégats segmentés de consommation électrique au pas 1/2 h des points de soutirage <= 36kVA – Maille régionale

- brainstorm-data/conso-inf36-region-agg.csv :

```
python fetch_enedis_conso_inf36.py --granularity year
```

# Prix de l'énergie (data.gouv.fr)

- brainstorm-data\tarifs-elec : [Historique des tarifs réglementés de vente d'électricité pour les consommateurs résidentiels](https://www.data.gouv.fr/datasets/historique-des-tarifs-reglementes-de-vente-delectricite-pour-les-consommateurs-residentiels)
