# [Données sur la consommation d'énergie](https://www.data.gouv.fr/pages/donnees-energie)

## [Electricité et gaz](https://www.data.gouv.fr/pages/donnees-energie)

- brainstorm-data/REG_conso_2011_ajd.csv :

```
python fetch_reg_conso.py --start-year 2020
```

## [Electricité](https://www.data.gouv.fr/pages/donnees-energie)

- brainstorm-data/eco2mix-regional-tr.csv :

```
curl -L "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-regional-cons-def/exports/csv?select=code_insee_region%2C%20libelle_region%2C%20date%2C%20avg(consommation)%20as%20puissance_moyenne_mw%2C%20sum(consommation)%2F2%20as%20conso_totale_mwh%2C%20max(consommation)%20as%20pic_journalier_mw&where=date_heure%20%3E%3D%20date%272020-01-01%27%20AND%20consommation%20IS%20NOT%20NULL&group_by=code_insee_region%2C%20libelle_region%2C%20date&delimiter=%3B" -o brainstorm-data/eco2mix-regional-tr.csv
```

# Agrégats segmentés de consommation électrique au pas 1/2 h des points de soutirage <= 36kVA – Maille régionale

- brainstorm-data/conso-inf36-region-agg.csv :

```
python fetch_enedis_conso_inf36.py --granularity year
```

# Prix de l'énergie (data.gouv.fr)

- brainstorm-data\tarifs-elec : [Historique des tarifs réglementés de vente d'électricité pour les consommateurs résidentiels](https://www.data.gouv.fr/datasets/historique-des-tarifs-reglementes-de-vente-delectricite-pour-les-consommateurs-residentiels)
