# Électricité & Gaz — Pipeline d'Agrégation Enedis (`fetch_enedis_conso_inf36.py`)

Ce script Python permet d'extraire et d'agréger les données du jeu de données Enedis Open Data :  
**[Agrégats segmentés de consommation électrique au pas 1/2 h des points de soutirage <= 36kVA – Maille régionale](https://opendata.enedis.fr/data-fair/api/v1/datasets/conso-inf36-region/api-docs.json)**.

Il permet notamment de répondre à la question :  
> **Qui consomme le plus d'électricité entre l'option tarifaire `BASE` et l'option `Heures Pleines / Heures Creuses (HPHC)` ?**

---

## ⚡ Pourquoi ce script ?

Le jeu de données brut contient des mesures toutes les 30 minutes, pesant **environ 2 Go par an** (~14 Go pour la période 2020–présent).

Au lieu de télécharger l'intégralité des lignes brutes, le script tire parti du point de terminaison d'agrégation côté serveur de Data Fair (`/values_agg`) :
1. **Légèreté maximale** : Enedis effectue le calcul sur ses serveurs. Le fichier de sortie final ne pèse que **~250 Ko** (maille annuelle) ou **~2 Mo** (maille mensuelle).
2. **Respect strict des quotas Enedis** : Gestion des fenêtres glissantes de 60 secondes (max 600 requêtes / 60s et max 20s de temps de calcul serveur / 60s pour un utilisateur anonyme, avec marge de sécurité et gestion des codes HTTP 429 / backoff exponentiel).
3. **Reprise sur incident (Checkpointing)** : Sauvegarde période par période. Si l'exécution est interrompue, le script reprend là où il s'est arrêté sans réinterroger les périodes déjà calculées.
4. **Alignement avec le modèle en étoile** : Normalisation automatique des profils en options tarifaires (`BASE` vs `HPHC`) et des plages de puissance en entiers (`3, 6, 9, 12... kVA`) pour se joindre directement avec `brainstorm-data/tarifs-elec/Option_Base.csv` et `Option_HPHC.csv`.

---

## 📋 Prérequis

* **Python 3.8+**
* **Aucune dépendance externe** (utilise exclusivement la bibliothèque standard : `urllib`, `json`, `csv`, `argparse`, etc.).

---

## 🚀 Exemples d'utilisation

### 1. Agrégation mensuelle (Recommandé / Par défaut)
Idéal pour analyser les variations saisonnières et s'aligner sur les révisions tarifaires semestrielles (février / août) :

```bash
python fetch_enedis_conso_inf36.py
```
*(Agrège les données depuis 2020 jusqu'à l'année en cours au pas mensuel).*

---

### 2. Agrégation annuelle (Le plus rapide)
Idéal pour des comparaisons macro-économiques et annuelles (seulement ~14 requêtes API au total) :

```bash
python fetch_enedis_conso_inf36.py --granularity year
```

---

### 3. Agrégation journalière
Pour croiser finement la consommation avec les jours fériés, les vacances scolaires ou les alertes EcoWatt :

```bash
python fetch_enedis_conso_inf36.py --granularity day --start-year 2022
```

---

### 4. Personnaliser la période ou le fichier de sortie

```bash
python fetch_enedis_conso_inf36.py --start-year 2021 --end-year 2024 --granularity month --output brainstorm-data/mon_export_conso.csv
```

---

### 5. Utiliser une clé d'API Enedis (Optionnel)
Si vous disposez d'un compte développeur Enedis Data Fair, vous pouvez renseigner votre clé d'API pour doubler les plafonds de requêtes (1200 req / 60s et 60s de calcul / 60s) :

```bash
python fetch_enedis_conso_inf36.py --api-key "VOTRE_CLE_API"
```

---

## ⚙️ Options en ligne de commande

| Argument | Type | Valeur par défaut | Description |
| :--- | :---: | :---: | :--- |
| `--start-year` | `int` | `2020` | Année de début de l'extraction. |
| `--end-year` | `int` | Année en cours | Année de fin de l'extraction. |
| `--granularity` | `str` | `month` | Pas d'agrégation temporelle : `year`, `month` ou `day`. |
| `--output` | `path` | `brainstorm-data/conso-inf36-region-agg.csv` | Chemin du fichier CSV de sortie. |
| `--api-key` | `str` | `None` | Clé d'API Data Fair (optionnelle). |

---

## 📊 Schéma du fichier généré (`conso-inf36-region-agg.csv`)

Le fichier produit est délimité par des points-virgules (`;`) et encodé en UTF-8 :

| Colonne | Type | Exemple | Description |
| :--- | :---: | :---: | :--- |
| `periode` | `string` | `2023-01` | Identifiant de la période (`YYYY`, `YYYY-MM` ou `YYYY-MM-DD`). |
| `annee` | `string` | `2023` | Année (clé étrangère vers `dim_calendrier` / `REG_conso`). |
| `code_region` | `string` | `11` | Code officiel INSEE de la région (clé vers `dim_territoire`). |
| `nom_region` | `string` | `Île-de-France` | Libellé officiel de la région. |
| `categorie_client` | `string` | `RES` | `RES` (Résidentiel) ou `PRO` (Professionnel $\le 36\text{ kVA}$). |
| **`option_tarifaire`** | `string` | **`BASE`** ou **`HPHC`** | Option déduite du profil Enedis (`RES1*` $\rightarrow$ `BASE`, `RES2*` $\rightarrow$ `HPHC`). |
| `profil_enedis` | `string` | `RES1` | Code profil brut Enedis. |
| `plage_de_puissance_souscrite` | `string` | `]3-6] kVA` | Plage de puissance brute. |
| **`p_souscrite_kva`** | `int` | `6` | Puissance souscrite entière normalisée (jointure avec `P_SOUSCRITE` des tarifs réglementés). |
| **`conso_totale_mwh`** | `float` | `15420.350` | Volume total d'électricité consommé (converti de Wh en MWh). |
| **`nb_points_soutirage_moyen`** | `int` | `125400` | Nombre moyen de compteurs / sites actifs sur la période. |
| **`conso_moyenne_par_site_mwh`** | `float` | `0.1230` | Consommation moyenne par compteur (`conso_totale_mwh / nb_points_soutirage_moyen`). |
| `nb_pas_30min` | `int` | `1488` | Nombre d'intervalles de 30 minutes agrégés. |

---

# 🏢 Mise à jour de `REG_conso_ajd.csv` (`fetch_reg_conso.py`)

Ce script Python génère et met à jour [`brainstorm-data/REG_conso_ajd.csv`](file:///c:/Users/formation/Documents/GitHub/electricite-gaz/brainstorm-data/REG_conso_ajd.csv) à partir de l'API Data Fair de l'Agence ORE :  
**[Consommation annuelle d’électricité et gaz par région](https://opendata.agenceore.fr/data-fair/api/v1/datasets/consommation-annuelle-d-electricite-et-gaz-par-region)**.

Il alimente votre modèle en étoile en :
* **Faits de consommation annuelle** (`Electricité` et `Gaz`, `Conso totale (MWh)`, `Nb sites`, parts thermosensibles et corrigées de l'aléa climatique),
* **Dimension Température / Climat** (`DJU à TR`, `DJU à TN`, `Thermosensibilité totale (kWh DJU)`),
* **Dimension Parc de Logements & Chauffage** (`Taux de chauffage électrique`, `Taux de logements collectifs`, superficie et âge des résidences principales).

## 🚀 Exemples d'utilisation (`fetch_reg_conso.py`)

### 1. Mettre à jour l'intégralité du fichier (`2011` à aujourd'hui, format exact 40 colonnes)
```bash
python fetch_reg_conso.py
```

### 2. Filtrer uniquement à partir de 2020 (aligné avec les autres tables)
```bash
python fetch_reg_conso.py --start-year 2020
```

### 3. Mode agrégé pour le Modèle en Étoile (`--mode star-agg`)
Dans le jeu de données brut, plusieurs opérateurs (`Enedis`, `GRDF`, `Antargaz`, ELD...) et codes `NAF2` coexistent pour une même région/année, et certains petits opérateurs ont les colonnes régionales (`DJU`, `Taux de chauffage électrique`) vides.  
L'option `--mode star-agg` consolide les consommations tous opérateurs confondus par `(annee, Code Région, FILIERE, CODE CATEGORIE CONSOMMATION, CODE GRAND SECTEUR)` et propage automatiquement les indicateurs régionaux (`DJU`, parc de logements) sur chaque ligne :
```bash
python fetch_reg_conso.py --start-year 2020 --mode star-agg
```

