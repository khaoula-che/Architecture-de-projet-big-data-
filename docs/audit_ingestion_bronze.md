# Audit de l'ingestion (branche `ingestion`) face au jalon S2

Audit en lecture seule : le code n'a été ni modifié ni exécuté. Il a été relu par rapport aux supports de cours S1, S2 et S3.

**La grille n'est pas dans le dépôt.** [docs/](./) ne contient que S1, S2 et S3 : il n'y a ni le guide, ni la grille « Verifier_ingestion_Bronze ». Les critères ont donc été reconstitués à partir de S2 : la page 13 (Bronze), les pages 15 à 17 (cycle de vie du batch, idempotence), la page 20 (vérifications de l'atelier) et la page 32 (jalon). Ils sont complétés par S1, pages 17 et 49. Le tableau sera à aligner sur la vraie grille dès qu'elle sera disponible.

**En bref :** le code n'appelle pas l'API et n'écrit pas encore de table Bronze. Rejouer l'ingestion crée des doublons, et ce point bloque directement le jalon.

## 1. Ce que fait le code

Le dépôt contient seulement trois fichiers : [producer_stations.py](../climate-project/ingestion/producer_stations.py), [docker-compose.yml](../climate-project/docker-compose.yml) et un `.gitignore`.

| Aspect | Constat |
|---|---|
| **Source** | Un CSV local, `climate-project/data/source/station.csv` ([l.12](../climate-project/ingestion/producer_stations.py#L12)). Il n'est pas dans le dépôt, car `*.csv` est ignoré par git. Le code lit une colonne `features` qui contient des « features » GeoJSON de stations, au format JSON ou au format dict Python (`ast.literal_eval`, [l.21](../climate-project/ingestion/producer_stations.py#L21)). **Aucun appel à climato.chom.engineering.** |
| **Périmètre** | Uniquement le référentiel des stations. Rien depuis `/v2/journaliere` (les observations), rien depuis `/ref/station-parametre`. Aucun périmètre de test n'est défini (stations, variables, période). |
| **Traitement** | Chaque ligne du CSV est validée : nombre de colonnes, `features` lisible, `ic_id` non vide. Elle est ensuite emballée dans une enveloppe `{event_id, run_id, ingested_at, source_file, source_record, dataset, raw_data}` ([l.130-140](../climate-project/ingestion/producer_stations.py#L130-L140)). |
| **Sortie** | Le topic Kafka `climate.stations.raw`, avec `ic_id` comme clé ([l.158](../climate-project/ingestion/producer_stations.py#L158)). **Il n'y a pas de consumer** : rien n'arrive dans une table Bronze. |
| **Technologies** | Python et `confluent_kafka` (producteur idempotent, `acks=all`). Kafka 4.1.2 en mode KRaft, sur un seul nœud, via Docker Compose. |

Les points solides : le producteur vérifie bien les erreurs de livraison et attend les confirmations avant de terminer ([l.47-53](../climate-project/ingestion/producer_stations.py#L47-L53), [l.148-185](../climate-project/ingestion/producer_stations.py#L148-L185)). L'encodage `utf-8-sig` et la détection du séparateur sont bien gérés. Enfin, la ligne source est conservée telle quelle dans `raw_data` ([l.139](../climate-project/ingestion/producer_stations.py#L139)).

## 2. Critères du jalon S2

| # | Critère (source dans les supports) | Statut | Où / pourquoi |
|---|---|---|---|
| 1 | Périmètre de test défini et réduit au besoin du Gold (S2 p.4, S1 p.36) | **Manquant** | Rien dans le code ni dans la doc |
| 2 | Sources conformes au thème : API journalière + référentiels | **À corriger** | [l.12](../climate-project/ingestion/producer_stations.py#L12) : CSV local, stations seulement |
| 3 | Zone brute immuable + checksum SHA-256 du fichier (S2 p.13-14) | **Manquant** | Pas de copie dans une zone brute, aucun checksum |
| 4 | Lecture permissive, tout en texte, lignes illisibles gardées et comptées (S2 p.18, 20) | **À corriger** | [l.92-127](../climate-project/ingestion/producer_stations.py#L92-L127) : la première ligne illisible arrête tout. `csv.Error` n'est pas intercepté (`strict=True` [l.81](../climate-project/ingestion/producer_stations.py#L81), et une `features` de plus de 128 Ko dépasse la limite `csv.field_size_limit`) |
| 5 | Aucune règle métier en Bronze (S2 p.13, 20) | **À corriger** | [l.121-127](../climate-project/ingestion/producer_stations.py#L121-L127) : une ligne avec un `ic_id` invalide est rejetée. C'est une règle de validation, qui relève de Silver. Le contenu, lui, est bien conservé ([l.139](../climate-project/ingestion/producer_stations.py#L139) OK) |
| 6 | Colonnes techniques `_source`, `_batch_id`, `_fichier`, `_ligne`, `_checksum`, `_ingere_le` (S2 p.13) | **À corriger** | [l.130-140](../climate-project/ingestion/producer_stations.py#L130-L140) : `ingested_at`, `source_file` et `source_record` sont OK. Il manque `_source` (le fournisseur) et `_checksum`. `run_id` est un uuid4 aléatoire, pas un `batch_id` déterministe |
| 7 | Table Bronze en Parquet, partitionnée par `_batch_id` (S2 p.13, 17, 20) | **Manquant** | Pas de consumer ni d'écriture Parquet ; les données restent dans Kafka |
| 8 | Batch log : un statut par batch, `CHARGÉ` seulement après l'écriture (S2 p.15, 17) | **Manquant** | Les compteurs sont seulement affichés ([l.175-177](../climate-project/ingestion/producer_stations.py#L175-L177)), rien n'est enregistré dans `_meta/` |
| 9 | Batch rejoué sans doublon (S2 p.17, 32) | **À corriger** | `enable.idempotence` ([l.38](../climate-project/ingestion/producer_stations.py#L38)) n'évite les doublons qu'à l'intérieur d'une même exécution. À chaque relance, `run_id` ([l.42](../climate-project/ingestion/producer_stations.py#L42)) et `event_id` ([l.131](../climate-project/ingestion/producer_stations.py#L131)) sont de nouveaux uuid4, et tout est réécrit dans le topic. Le topic n'est pas compacté |
| 10 | Effacer, relancer : mêmes tables, mêmes comptes, mêmes rejets (S2 p.2, 32) | **Manquant** | Il n'y a pas de table, et les identifiants changent à chaque exécution |
| 11 | Réconciliation : lignes lues = écrites + illisibles (S2 p.20, 29) | **À corriger** | Les compteurs `sent` et `delivered` existent ([l.44-45](../climate-project/ingestion/producer_stations.py#L44-L45)), mais il n'y a pas de compteur d'illisibles et rien n'est enregistré. En cas d'arrêt en cours de route, le `finally` ([l.172](../climate-project/ingestion/producer_stations.py#L172)) envoie quand même un batch partiel, qui n'est pas tracé |
| 12 | Data contract Bronze au format ODCS (S2 p.32) | **Manquant** | Aucun fichier `*.odcs.yaml` |
| 13 | Exécution reproductible (infra, dépendances) | **À corriger** | [compose l.23](../climate-project/docker-compose.yml#L23) désactive la création automatique des topics, et aucun service ne crée `climate.stations.raw` : le premier envoi échouera (`UNKNOWN_TOPIC_OR_PART`). Il n'y a pas de `requirements.txt`. Le broker est codé en dur ([l.37](../climate-project/ingestion/producer_stations.py#L37)) |
| 14 | Livraison Kafka fiable | **OK** | [l.36-40](../climate-project/ingestion/producer_stations.py#L36-L40), [l.148-185](../climate-project/ingestion/producer_stations.py#L148-L185) |
| 15 | Encodage et séparateur hétérogènes (S2 p.18) | **OK** | [l.56-72](../climate-project/ingestion/producer_stations.py#L56-L72) |

## 3. Ce qu'il faut installer

**Pour exécuter le code tel qu'il est :**
- **Docker Desktop**, avec Compose v2, pour l'image `apache/kafka:4.1.2`.
- **Python 3.8 ou plus** et `pip install confluent-kafka`. Les wheels macOS incluent déjà librdkafka.
- **Créer le topic à la main**, puisque la création automatique est désactivée :
  ```
  docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create \
    --topic climate.stations.raw --partitions 1 --replication-factor 1 \
    --bootstrap-server localhost:9092
  ```
- **Fournir le fichier `climate-project/data/source/station.csv`**, qui n'est pas versionné.

**Pour atteindre le jalon S2 :**
- **Java 17** et `pyspark` (S1 p.49 demande « PySpark fonctionnel en local »).
- `pyarrow` et `requests` (ou `httpx`).
- Optionnel : `datacontract-cli`, pour valider les contrats ODCS.

## 4. Corrections proposées (non appliquées)

**Bloquant pour le jalon**
1. **Ingérer depuis l'API.** Écrire un extracteur pour `/ref/stations`, `/ref/station-parametre` et `/v2/journaliere`, limité au périmètre de test : une liste de stations, les variables utiles aux extrêmes (tx, tn, rr…) et une période fixée. Chaque réponse brute est écrite telle quelle dans `raw/source=climato/batch=<batch_id>/`, avec son SHA-256. Les paramètres de l'API n'ont pas été vérifiés : à confirmer dans sa documentation.
2. **Utiliser un `batch_id` déterministe**, construit à partir de la source, de l'endpoint et de la période ou date d'extraction, plutôt qu'un uuid4. Remplacer aussi `event_id` par un identifiant stable, par exemple `batch_id` + numéro de ligne.
3. **Écrire la table Bronze** en Parquet, avec tous les champs en texte, partitionnée par `_batch_id` et en `overwrite` dynamique (S2 p.17, 19). Ajouter les colonnes `_source`, `_batch_id`, `_fichier`, `_ligne`, `_checksum` et `_ingere_le`.
4. **Tenir un batch log** dans `_meta/batchs.parquet`, avec les colonnes batch_id, source, checksum, nb_lignes, nb_illisibles, statut et maj_le. Ignorer un batch dont le checksum est déjà connu, et ne le passer en `CHARGÉ` qu'après une écriture réussie.
5. **Lire en mode permissif.** Une ligne illisible est conservée dans `_ligne_illisible` et comptée, sans arrêter l'exécution. Retirer le contrôle sur `ic_id` du Bronze (il part en Silver, règle Q6).
6. **Vérifier et enregistrer la réconciliation** : lues = écrites + illisibles. Ajouter un script qui efface Bronze, relance deux fois et compare les comptes.

**Choix d'architecture à trancher en équipe.** S2 retient l'ingestion par batchs (p.6). Un topic où chaque ligne est un message n'apporte rien au replay, et il le complique. Recommandation : garder Kafka seulement pour **annoncer l'arrivée d'un batch** sous forme d'événement CloudEvents (S2 p.16, avec la clé `(source, id)` comme garantie d'idempotence), ou le retirer. Si l'équipe garde des messages ligne par ligne, il faudra au minimum un consumer vers Parquet, une clé stable et un topic compacté. Ce choix devra être justifié dans le dossier.

**Infra et qualité du dépôt**
7. Créer le topic au démarrage, via un service d'init dans le compose ou un script. Annoncer `127.0.0.1` plutôt que `localhost` ([compose l.12](../climate-project/docker-compose.yml#L12)), pour que l'adresse corresponde à celle qu'utilise le producteur.
8. Sortir le broker, le topic et les chemins dans des variables d'environnement ou un fichier de config. Ajouter un `requirements.txt` et un README qui explique comment lancer le projet. Le [README.md](../README.md) actuel est encodé en UTF-16, et git le traite comme un fichier binaire.
9. Intercepter `csv.Error` et relever `csv.field_size_limit`. Accepter uniquement du JSON en entrée : si un parseur Python est nécessaire, c'est le signe d'un export mal fait en amont.
10. Ajouter un data contract Bronze au format ODCS (`bronze_*.odcs.yaml`), et le préparer aussi pour Silver.
11. Ajouter les PDF manquants (le guide et la grille) dans `docs/`, pour pouvoir confirmer cet audit critère par critère.
