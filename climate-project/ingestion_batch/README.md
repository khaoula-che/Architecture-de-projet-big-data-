# Ingestion par batchs : API climato → zone brute → Bronze

Projet « Observation des extrêmes climatiques en France » · module Architecture de projet Big Data (ESGI, IABD).

## Ce que fait l'ingestion

```
API climato
   │  extract_api.py (Python)
   ▼
ZONE BRUTE   data/raw/      réponses JSON telles quelles + empreinte SHA-256   → batch log : RECU
   │  load_bronze.py (PySpark)
   ▼
BRONZE       data/bronze/   Parquet, tous les champs en texte + colonnes techniques → batch log : CHARGE
```

- **Un batch = une station × une période.** Nom fixe : `v2j_<station>_<debut>_<fin>`. Le référentiel des stations forme un batch par jour d'extraction : `ref_stations_<date>`.
- **Batch log** (`data/_meta/batch_log.csv`) : une ligne par batch, avec son empreinte, son nombre de lignes et son statut (`RECU`, `CHARGE`, `ECHEC`).
- **Bronze** : aucune conversion ni filtre. Partitionné par `_batch_id`. Colonnes techniques : `_source`, `_endpoint`, `_batch_id`, `_fichier`, `_ligne`, `_checksum`, `_ingere_le`.

## Périmètre actuel (TEST)

Défini dans `config.yaml` :
- **10 stations Météo-France** : Lille, Brest, Paris-Montsouris, Strasbourg, Lyon, Grenoble, Bordeaux, Toulouse, Marseille, Nice ;
- **années complètes 2003, 2019 et 2022**.

Cela donne 30 batchs de mesures (10 950 lignes) et 1 batch de référentiel (21 559 stations). Pour agrandir le périmètre, il suffit de modifier `config.yaml` : le code ne change pas.

## Prérequis

- Python 3.12, Java 17
- `pip install -r climate-project/ingestion_batch/requirements.txt`

## Lancer (depuis la racine du dépôt)

```bash
# 1. Télécharger les batchs (ceux déjà reçus et intacts sont ignorés)
python3 climate-project/ingestion_batch/extract_api.py

# 2. Charger dans Bronze les batchs au statut RECU
python3 climate-project/ingestion_batch/load_bronze.py
```

## Lancer avec Docker (recommandé pour l'équipe)

Seul **Docker Desktop** est nécessaire : l'image contient Java 17, Python 3.12 et PySpark 4.1.1. Les fichiers `Dockerfile`, `docker-compose.yml` et `.dockerignore` sont à la racine du dépôt. Toutes les commandes se lancent **depuis la racine**.

```bash
# Construire l'image (la première fois : quelques minutes)
docker compose build

# Lancer l'ingestion dans le conteneur
docker compose run --rm ingestion python3 climate-project/ingestion_batch/extract_api.py
docker compose run --rm ingestion python3 climate-project/ingestion_batch/load_bronze.py

# Voir la Spark UI pendant l'exécution (http://localhost:4040)
docker compose run --rm --service-ports ingestion python3 climate-project/ingestion_batch/load_bronze.py
```

Le dossier du projet est **partagé** avec le conteneur : le code est lu depuis le disque local, et `climate-project/data/` y est écrit. Il n'est pas nécessaire de reconstruire l'image après une modification du code, seulement après une modification de `requirements.txt`.

> Le fichier `climate-project/docker-compose.yml` correspond à l'ancienne ingestion Kafka (branche `ingestion`). Il n'est pas utilisé ici.

## Rejeu et reconstruction

| Action | Commande | Résultat attendu |
|---|---|---|
| Relancer l'extraction | `extract_api.py` | `0 recu, 31 ignore` : aucun doublon, pas d'appel à l'API |
| Relancer le chargement | `load_bronze.py` | `Rien a charger` |
| Reconstruire tout le Bronze depuis la zone brute | `rm -rf climate-project/data/bronze` puis `load_bronze.py --tout` | `31 charge`, mêmes comptes qu'avant |

## Points à connaître sur l'API

- La date de fin `to` est **exclue** : une année se demande du 01/01 au 01/01 suivant.
- **20 000 lignes au plus** par réponse : la pagination (`tronque` / `suite.from`) est gérée.
- Environ 3 à 4 s par appel : un appel à la fois, avec une pause et une nouvelle tentative sur les erreurs 503.

## Règles

- `climate-project/data/` **n'est jamais versionné** : les données se régénèrent avec les programmes.
- Les identifiants S3 ne seront **jamais** écrits dans le code (variables d'environnement uniquement).

## Limites et suite

- Périmètre de test, exécution locale (`local[*]`). Le passage au stockage objet de l'école (S3) et au cluster viendra plus tard ; seuls les chemins de `config.yaml` changeront.
- `/ref/station-parametre` n'est pas encore ingéré.
- L'ancienne ingestion Kafka (branche `ingestion`) est conservée mais n'est plus utilisée (décision d'équipe : ingestion par batchs, comme demandé en S2).
- Le data contract Bronze (ODCS) est à venir.