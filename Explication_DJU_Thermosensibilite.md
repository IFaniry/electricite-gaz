# Explication des variables climatiques et de thermosensibilité

Ce document détaille la signification et l'utilisation des variables climatiques présentes dans les données de consommation d'électricité et de gaz (notamment dans [`REG_conso_2011_ajd.csv`](brainstorm-data/REG_conso_2011_ajd.csv), ainsi que les mailles départementales, EPCI et communales).

Ces variables constituent le cadre de référence statistique utilisé par le Ministère de la Transition écologique (SDES) et les gestionnaires de réseau (Enedis, GRDF, RTE) pour **isoler l'impact météorologique des évolutions réelles de consommation d'énergie**.

---

## 1. Définitions fondamentales

### A. Qu'est-ce qu'un DJU (Degré-Jour Unifié) ?
Le **DJU** (Degré-Jour Unifié) est un indicateur météorologique qui mesure la sévérité du froid (ou du besoin de chauffage) au cours d'une période par rapport à une température de consigne intérieure de référence (traditionnellement **18 °C** en France, parfois 17 °C).

Pour chaque journée où la température moyenne extérieure quotidienne $T_{\text{ext}}$ est inférieure à 18 °C :
$$DJU_{\text{jour}} = 18 - T_{\text{ext}}$$

*(Si la température extérieure moyenne est supérieure ou égale à 18 °C, le DJU de chauffage de la journée vaut 0).*

Le DJU annuel correspond à la somme des DJU journaliers sur les 365 jours de l'année.  
**Plus un hiver est long et rigoureux, plus le total annuel des DJU est élevé.**

---

### B. `DJU à TR` (Température Réelle)
* **Définition** : Il s'agit des **Degrés-Jours Unifiés réellement observés** par les stations météo dans la région au cours de l'année considérée ($N$).
* **Comportement** : Cette valeur fluctue chaque année selon les aléas climatiques. Un hiver très rigoureux (comme 2012 ou 2018) affichera un `DJU à TR` élevé, tandis qu'un hiver exceptionnellement doux (comme 2020) affichera un `DJU à TR` faible.

---

### C. `DJU à TN` (Température Normale)
* **Définition** : Il s'agit des **Degrés-Jours Unifiés de la normale climatique trentenaire** calculée par Météo-France pour la région (par exemple sur les périodes de référence 1981-2010 ou 1991-2020).
* **Comportement** : Cette valeur représente le climat « moyen standard » attendu pour la région. Elle sert de référence fixe pour neutraliser la variabilité interannuelle du climat.

---

### D. `Thermosensibilité` (en kWh / DJU)
La **thermosensibilité** représente la sensibilité de la demande énergétique au refroidissement des températures.

Elle répond à la question suivante :  
*« Pour chaque DJU supplémentaire (c'est-à-dire une baisse de 1 °C de la température extérieure moyenne pendant 24 heures), combien de kilowattheures (kWh) supplémentaires d'électricité ou de gaz sont consommés pour le chauffage ? »*

Le fichier distingue deux grandeurs :
* **`Thermosensibilité totale (kWh DJU)`** : La pente de thermosensibilité cumulée pour l'ensemble du secteur / de la région (en kWh/DJU).
* **`Thermosensibilité moyenne (kWh DJU)`** : La thermosensibilité ramenée par point de livraison ou par logement (en kWh/DJU/site).

---

### E. `part_thermosensible`
* **Définition** : La part (comprise entre 0 et 1, ou en %) de la consommation d'énergie qui dépend directement de la météo (principalement le chauffage d'appoint ou principal, et dans une moindre mesure la climatisation en été).
* **Complémentaire** : Le reste (la part non thermosensible) correspond aux usages de base incompressibles indépendants de la température (cuisson, eau chaude sanitaire, électroménager, éclairage, procédés industriels constants).

---

## 2. La formule de correction de l'aléa climatique

Ces variables permettent de calculer la colonne clé :  
**`conso_totale_corrigee_de_l_alea_climatique_a_usages_thermosensibles_mwh`**

La formule de correction s'écrit :

$$\text{Conso Corrigée} = \text{Conso Observée} + \left[ \frac{\text{Thermosensibilité totale} \times (\text{DJU à TN} - \text{DJU à TR})}{1000} \right]$$

*(La division par 1 000 permet de convertir les kWh de la thermosensibilité en MWh pour les sommer à la consommation totale).*

---

## 3. Interprétation selon les scénarios climatiques

| Conjoncture météo | Comparaison des DJU | Impact sur la consommation observée | Effet du calcul de correction |
| :--- | :--- | :--- | :--- |
| **Hiver plus froid que la normale** | $\text{DJU à TR} > \text{DJU à TN}$ | La consommation observée a été gonflée artificiellement par des vagues de froid exceptionnelles. | Le terme $(\text{DJU à TN} - \text{DJU à TR})$ est **négatif** : la formule **soustrait** le surplus de consommation pour ramener le chiffre à une année normale. |
| **Hiver plus doux que la normale** | $\text{DJU à TR} < \text{DJU à TN}$ | La consommation observée est plus basse simplement parce qu'il n'a pas fait froid. | Le terme $(\text{DJU à TN} - \text{DJU à TR})$ est **positif** : la formule **rajoute** de la consommation pour simuler ce qui aurait été consommé lors d'un hiver standard. |

---

## 4. Pourquoi cette distinction est cruciale ?

Pour tout modèle prédictif, analyse d'efficacité énergétique ou tableau de bord décisionnel :

1. **Évaluer les politiques publiques et la rénovation thermique** :  
   Si la consommation brute baisse de 5 % entre deux années, il est impossible de savoir s'il s'agit d'une réelle amélioration de l'isolation des logements ou simplement d'un hiver anormalement doux sans analyser la **consommation corrigée de l'aléa climatique**.
2. **Modélisation prédictive de la charge électrique** :  
   Pour prédire la consommation journalière ou horaire en fonction des prévisions météo à J-1, la variable déterminante est la **thermosensibilité** ($\approx 2\,400\text{ MW/}^\circ\text{C}$ au niveau national en hiver), car elle pilote la hauteur des pointes hivernales de consommation électrique.
