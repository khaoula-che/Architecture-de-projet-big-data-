# Couche Gold : catalogue des épisodes extrêmes

Projet « Observation des extrêmes climatiques en France » · module Architecture de projet Big Data (ESGI, IABD).

## Ce que fait le Gold

```
silver/observations_jour ─┐                   ┌─▶ gold/episodes             catalogue des épisodes extrêmes
                          ├─▶ build_gold.py ──┼─▶ gold/records_stations     nos records par station et par mois
bronze/v2_records ────────┘     (PySpark)     └─▶ gold/validation_records   comparaison avec /v2/records
```

## Produit 1 : le catalogue des épisodes

**Une ligne = un épisode** : une station, un type, une période continue.

| Type | Un jour compte si… | Durée minimale |
|---|---|---|
| Canicule | `tx` ≥ 35 °C | 3 jours |
| Vague de froid | `tn` ≤ −5 °C | 3 jours |
| Fortes pluies | `rr` ≥ 50 mm | 1 jour (jours consécutifs regroupés) |

### Règles de détection

- Un jour **dépasse** le seuil si sa mesure est **OK ou SUSPECT** et franchit le seuil.
- Un jour est **manquant** si sa mesure est **ABSENT ou INVALIDE**.
- Un jour **normal** coupe l'épisode.
- **Un seul jour manquant** entre deux jours qui dépassent **ne coupe pas** l'épisode ; deux jours manquants de suite le coupent (D21, provisoire).
- Un épisode **commence et finit** sur un jour qui dépasse le seuil.
- Les valeurs **SUSPECT** sont utilisées et **signalées** (`contient_suspect`) ; INVALIDE et ABSENT ne le sont jamais (D22).

Technique : « gaps and islands » avec des fenêtres Spark (`lag`, sommes cumulées), station par station.

### Méthode de détection (D19)

| Méthode | Principe | Quand |
|---|---|---|
| **A** | seuils fixes nationaux | **actuelle** (périmètre de test) |
| **B** | seuils propres à chaque station (percentiles sur ~30 ans) | passage à l'échelle |
| **C** | méthode Météo-France (canicule : moyennes sur 3 jours de tn et tx, seuils départementaux) | bonus, pour comparer |

Tous les paramètres sont dans `climate-project/ingestion_batch/config.yaml`, section `episodes`.

## Produit 2 : les records par station

Une ligne = une station × un mois du calendrier : jour le plus chaud, nuit la plus froide, jour le plus pluvieux, et leurs dates. C'est le même grain que `/v2/records`.

## Produit 3 : la validation avec `/v2/records`

| Vérification | Question | Résultat |
|---|---|---|
| **Cohérence** | Nos valeurs dépassent-elles un record historique (depuis 1941) ? Elles ne doivent jamais le faire | OK / KO |
| **Concordance** | Quand le record date d'une de nos années, notre valeur ce jour-là est-elle identique ? | OK / KO / NA (record hors de nos années) |

Les records de l'API viennent de la même source (v2) : c'est un **contrôle de cohérence de la chaîne** API → Bronze → Silver → Gold, pas une référence indépendante.

## Lancer (depuis la racine du dépôt)

Prérequis : Bronze (avec `v2_records`) et Silver à jour.

```bash
python3 climate-project/gold/build_gold.py

# Avec Docker
docker compose run --rm ingestion python3 climate-project/gold/build_gold.py
```
Le Gold est **recalculé en entier** à chaque exécution (le calcul incrémental viendra plus tard). Le programme se termine en erreur s'il y a au moins un KO.

## Test de la détection

```bash
python3 climate-project/tests/test_gold_episodes.py
```
Ce test fabrique des séries de jours en mémoire et vérifie : durée minimale, tolérance d'un jour manquant, coupure, bords, froid, pluie (1 jour, plusieurs jours, cumul), SUSPECT. Attendu : **`10/10 tests reussis`**.

## Résultats sur le périmètre de test

- **28 canicules, 13 vagues de froid, 99 épisodes de fortes pluies.**
- Les canicules les plus longues sont celles d'**août 2003** : Toulouse et Grenoble (12 jours), Lyon et Bordeaux (11), Paris (9), Strasbourg (8). On retrouve aussi juin 2019 et juin-juillet 2022.
- Validation : **cohérence 120/120** (tx et tn), **concordance 17 OK (tx) et 7 OK (tn), 0 KO**.
  Exemple : record d'août à Grenoble = 38,8 °C le 13/08/2003, identique dans notre Silver.

## Limites

- Méthode A : un seuil fixe n'a pas le même sens partout. Le nombre de fortes pluies est élevé dans certaines stations (ex. Brest), à réexaminer avec la méthode B.
- Années non contiguës (2003, 2019, 2022) : un épisode à cheval sur deux années peut être tronqué.
- Bornes horaires de la « journée » de l'API (0 h–24 h ou 6 h–6 h UTC) : à confirmer avec le professeur.
- Périmètre de test, exécution locale.

## Documents liés

- Contrat de données : `climate-project/contracts/gold_episodes.odcs.yaml`
- Journal des décisions : `docs/decisions-architecture.md` (D19 à D22)