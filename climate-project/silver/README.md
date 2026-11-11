# Couche Silver : Bronze → observations journalières propres

Projet « Observation des extrêmes climatiques en France » · module Architecture de projet Big Data (ESGI, IABD).

## Ce que fait le Silver

```
bronze/v2_journaliere ─┐                    ┌─▶ silver/observations_jour   table propre (format large)
                       ├─▶ build_silver.py ─┼─▶ silver/quarantaine        lignes inutilisables + motif
bronze/ref_stations ───┘     (PySpark)      └─▶ _meta/rapport_qualite_silver.csv   bilan par batch
```

Pour chaque ligne du Bronze :
1. **Conversion** : texte → nombres (`tx`, `tn`, `tm`, `rr`) et dates.
2. **Jointure avec le référentiel** (sa dernière version) : nom, département, latitude, longitude, altitude de la station.
3. **Quarantaine** des lignes inutilisables : date illisible ou hors période du batch (Q3), station inconnue (Q6).
4. **Dédoublonnage** : une seule ligne par `(station_id, date)` ; la plus récente gagne (`_ingere_le`, puis `_batch_id`).
5. **Contrôles de qualité par mesure** : `qc_tx`, `qc_tn`, `qc_rr` ∈ {OK, SUSPECT, INVALIDE, ABSENT}.
6. **Écriture** rangée par `annee` / `mois`, triée par station et date.
7. **Bilan par batch** : lignes Bronze = Silver + doublons + quarantaine → statut `PUBLIE` (sinon `ECHEC`).

## Format LARGE (décision d'équipe)

Une ligne = une station × un jour, avec une colonne par mesure.

> Format large retenu, car le produit Gold (épisodes extrêmes) exploite tx, tn et rr conjointement par station et par jour. Le format long, proposé dans le cours, est mieux adapté à un catalogue de nombreuses variables hétérogènes, ce qui n'est pas notre cas (3 variables).
> Point faible : si l'on ajoute beaucoup de mesures (vent, pression, neige…), le format long deviendra plus intéressant.

## Règles de qualité

| Règle | Contrôle | Résultat |
|---|---|---|
| Absence | valeur vide | `ABSENT` (la valeur reste NULL, jamais 0) |
| Conversion | texte non convertible (ex. `mq`) | `INVALIDE` |
| Q1 | `tx` ou `tn` hors de [−50 ; 55] °C, `rr` < 0 | `INVALIDE` |
| Q1 | `rr` > 500 mm en un jour | `SUSPECT` |
| Q2 | `tn` > `tx` le même jour | `tx` et `tn` `INVALIDE` |
| Q3 | date illisible ou hors période du batch | ligne en **quarantaine** |
| Q6 | station absente du référentiel | ligne en **quarantaine** |

Les seuils sont dans `climate-project/ingestion_batch/config.yaml` (section `qualite`).
Un problème sur **une mesure** marque seulement cette mesure : la ligne reste en Silver. Un problème sur **toute la ligne** l'envoie en quarantaine.

## Lancer (depuis la racine du dépôt)

Prérequis : l'ingestion Bronze a été faite (voir `climate-project/ingestion_batch/README.md`).

```bash
# Publier dans le Silver les batchs Bronze au statut CHARGE pas encore publiés
python3 climate-project/silver/build_silver.py

# Avec Docker
docker compose run --rm ingestion python3 climate-project/silver/build_silver.py
```

## Rejeu et reconstruction

| Action | Commande | Résultat attendu |
|---|---|---|
| Relancer | `build_silver.py` | `Rien a publier` : aucun doublon |
| Reconstruire tout le Silver depuis le Bronze | `build_silver.py --tout` | `30 publie(s)`, même contenu qu'avant |

## Test des règles de qualité

```bash
python3 climate-project/tests/test_silver_qualite.py
```
Ce test fabrique 10 lignes piégées dans un dossier temporaire (les vraies données ne sont pas touchées), lance `build_silver.py` dessus et vérifie chaque règle. Attendu : **`11/11 tests reussis`**.

## Résultats sur le périmètre de test

- 30 batchs publiés : 10 950 lignes Bronze = 10 950 lignes Silver, 0 doublon, 0 quarantaine.
- 10 950 couples (station, date) distincts.
- Exemple : Grenoble-St Geoirs, 12/08/2003 : tx 38,6 °C, tn 17,4 °C, rr 0,0 mm, tout OK.

## Documents liés

- Contrat de données : `climate-project/contracts/silver_observations_jour.odcs.yaml`
- Rapport de qualité : `climate-project/data/_meta/rapport_qualite_silver.csv` (non versionné)

## Limites

- Périmètre de test, exécution locale. Le passage au stockage objet de l'école et au cluster viendra plus tard.
- Bornes horaires de la « journée » de l'API (0 h–24 h ou 6 h–6 h UTC) : à confirmer avec le professeur.
- Contrôles Q4 (valeur figée) et Q5 (saut brutal) du cours : non implémentés pour l'instant.