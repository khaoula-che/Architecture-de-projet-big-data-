# Journal des décisions d'architecture — Projet « Extrêmes climatiques en France »

À reprendre dans le dossier d'architecture (S4) et la soutenance.

| # | Décision | Justification |
|---|---|---|
| D1 | Kafka retiré : ingestion par batchs | Choix du cours (S2 p.6) : batchs incrémentaux. L'ancien code (streaming ligne par ligne) ne produisait pas de table Bronze et dupliquait à chaque relance. |
| D2 | Source : API climato `/v2/journaliere` + `/ref/stations` | Remplace un CSV local non versionné. |
| D3 | Périmètre de test : 10 stations Météo-France × années complètes 2003, 2019, 2022 | Petit pour construire et vérifier vite ; canicules connues ; années complètes pour couvrir chaleur, froid et pluie, sans appel API supplémentaire. |
| D4 | Architecture API → zone brute → Bronze (2 programmes) | Schéma du cours (S2 p.13). Reconstruction sans rappeler l'API, preuve de ce qui a été reçu (SHA-256). |
| D5 | 1 batch = 1 station × 1 année ; batch_id déterministe | Une correction ou un échec ne touche qu'un batch ; même nom pour la même demande, ce qui garantit l'idempotence. |
| D6 | Batch log en CSV (RECU / CHARGE / ECHEC) | Lisible ; passage en Parquet possible plus tard. |
| D7 | Bronze en Parquet, tout en texte, partitionné par `_batch_id` | Règle du Bronze (aucune conversion) ; format du lakehouse. |
| D8 | Stockage local pour le test, stockage objet S3 de l'école ensuite | Le cluster doit lire les mêmes fichiers ; seuls les chemins de `config.yaml` changent. |
| D9 | Docker (Java 17, Python 3.12, PySpark 4.1.1) pour l'équipe | Même environnement pour tous ; ne sert pas au déploiement sur le cluster de l'école. |
| D10 | **Silver en format LARGE** (une ligne par station et par jour, colonnes tx / tn / rr + un qc_flag par mesure) | **« Format large retenu, car le produit Gold (épisodes extrêmes) exploite tx, tn et rr conjointement par station et par jour. Le format long, proposé dans le cours, est mieux adapté à un catalogue de nombreuses variables hétérogènes, ce qui n'est pas notre cas (3 variables). »** Point faible : si l'on ajoute beaucoup de mesures (vent, pression, neige…), le format long deviendra plus intéressant. |
| D11 | Un qc par mesure (qc_tx, qc_tn, qc_rr) : OK / SUSPECT / INVALIDE / ABSENT | Une mesure douteuse n'empêche pas d'utiliser les autres du même jour ; ABSENT applique « absence ≠ zéro ». |
| D12 | Problème sur une mesure → qc (la valeur est gardée et marquée) ; problème sur toute la ligne → quarantaine | Option « conserver avec un indicateur » (S1 p.18) + quarantaine pour les lignes inutilisables. On ne supprime rien : le Gold choisit ce qu'il utilise (OK ; SUSPECT à décider ; INVALIDE et ABSENT exclus). |
| D13 | Seuils : température possible entre −50 et 55 °C ; pluie > 500 mm/jour = SUSPECT | Exemple du cours ; seuils dans `config.yaml`. |
| D14 | Dédoublonnage : une ligne par (station, date), la plus récente gagne | Règle déterministe (S2). |
| D15 | Jointure avec la dernière version du référentiel (left join) | Ajoute nom, département, position, altitude ; rend détectables les stations inconnues (Q6). Le référentiel évolue (21 559 stations le 10/11, 21 556 le 12/11), d'où un batch de référentiel par jour. |
| D16 | Silver partitionné par **année / mois**, trié par station et date | Conseil du cours (S2 p.30) ; le Gold cherche par période (partition pruning). |
| D17 | Suivi du Silver dans son rapport de qualité (PUBLIE / ECHEC), sans modifier le batch log du Bronze | Le code de l'ingestion reste intact. |
| D18 | Test des règles sur des données fabriquées, dans un dossier temporaire | Les vraies données sont propres ; le test prouve les règles sans toucher aux vraies données (11/11). |

## Points à réévaluer au passage à l'échelle
- **Partitionnement du Silver (D16)** : au périmètre complet (≈ 3 000 stations, estimation), un dossier par mois ferait environ 3 à 10 Mo, sous la cible du cours (128–512 Mo, risque de « small files »). Un dossier par année ferait environ 40 à 120 Mo. **À mesurer** (taille des fichiers, temps de lecture du Gold) puis comparer année/mois et année seule, à résultat identique. Prévoir de rendre le choix configurable dans `config.yaml`. Formulation possible pour le dossier : « Partitionnement initial année/mois (conseil du cours). Au passage à l'échelle, fichiers mensuels de X Mo, sous la cible de 128–512 Mo ; passage à un partitionnement annuel : fichiers de Y Mo, temps de lecture du Gold passé de A à B s, résultat identique. »

## Questions ouvertes (envoyées au professeur)
- Accès au S3 de l'équipe et au cluster ; versions de Spark et de Python sur le cluster.
- Périmètre complet attendu et délai pour produire le Gold.
- Résultat de référence et extrait brut (late data, corrections).
- Bornes horaires de la « journée » de `/v2/journaliere` (0 h–24 h ou 6 h–6 h UTC) ; la date `to` est exclue, contrairement à la documentation.
