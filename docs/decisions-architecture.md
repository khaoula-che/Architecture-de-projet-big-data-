# Journal des décisions d'architecture — Projet « Extrêmes climatiques en France »

À reprendre dans le dossier d'architecture (S4) et la soutenance. Chaque ligne = une question possible du jury.

## Ingestion (Bronze)

| # | Décision | Justification |
|---|---|---|
| D1 | Kafka retiré : ingestion par batchs | Choix du cours (S2 p.6) : batchs incrémentaux. L'ancien code (streaming ligne par ligne) ne produisait pas de table Bronze et dupliquait à chaque relance. |
| D2 | Source : API climato `/v2/journaliere` + `/ref/stations` (+ `/v2/records`, voir D23) | Remplace un CSV local non versionné. |
| D3 | Périmètre de test : 10 stations Météo-France × années complètes 2003, 2019, 2022 | Petit pour construire et vérifier vite ; canicules connues ; années complètes pour couvrir chaleur, froid et pluie, sans appel API supplémentaire. |
| D4 | Architecture API → zone brute → Bronze (2 programmes) | Schéma du cours (S2 p.13). Reconstruction sans rappeler l'API, preuve de ce qui a été reçu (SHA-256). |
| D5 | 1 batch = 1 station × 1 année ; batch_id déterministe | Une correction ou un échec ne touche qu'un batch ; même nom pour la même demande, ce qui garantit l'idempotence. |
| D6 | Batch log en CSV (RECU / CHARGE / ECHEC) | Lisible ; passage en Parquet possible plus tard. |
| D7 | Bronze en Parquet, tout en texte, partitionné par `_batch_id` | Règle du Bronze (aucune conversion) ; format du lakehouse. |
| D8 | Stockage local pour le test, stockage objet S3 de l'école ensuite | Le cluster doit lire les mêmes fichiers ; seuls les chemins de `config.yaml` changent. |
| D9 | Docker (Java 17, Python 3.12, PySpark 4.1.1) pour l'équipe | Même environnement pour tous ; ne sert pas au déploiement sur le cluster de l'école. |

## Silver

| # | Décision | Justification |
|---|---|---|
| D10 | **Silver en format LARGE** (une ligne par station et par jour, colonnes tx / tn / rr + un qc_flag par mesure) | **« Format large retenu, car le produit Gold (épisodes extrêmes) exploite tx, tn et rr conjointement par station et par jour. Le format long, proposé dans le cours, est mieux adapté à un catalogue de nombreuses variables hétérogènes, ce qui n'est pas notre cas (3 variables). »** Point faible : si l'on ajoute beaucoup de mesures (vent, pression, neige…), le format long deviendra plus intéressant. |
| D11 | Un qc par mesure (qc_tx, qc_tn, qc_rr) : OK / SUSPECT / INVALIDE / ABSENT | Une mesure douteuse n'empêche pas d'utiliser les autres du même jour ; ABSENT applique « absence ≠ zéro ». |
| D12 | Problème sur une mesure → qc (la valeur est gardée et marquée) ; problème sur toute la ligne → quarantaine | Option « conserver avec un indicateur » (S1 p.18) + quarantaine pour les lignes inutilisables. On ne supprime rien : le Gold choisit ce qu'il utilise (OK et SUSPECT selon D22 ; INVALIDE et ABSENT exclus). |
| D13 | Seuils : température possible entre −50 et 55 °C ; pluie > 500 mm/jour = SUSPECT | Exemple du cours ; seuils dans `config.yaml`. |
| D14 | Dédoublonnage : une ligne par (station, date), la plus récente gagne | Règle déterministe (S2). |
| D15 | Jointure avec la dernière version du référentiel (left join) | Ajoute nom, département, position, altitude ; rend détectables les stations inconnues (Q6). Le référentiel évolue (21 559 stations le 10/11, 21 556 le 12/11), d'où un batch de référentiel par jour. |
| D16 | Silver partitionné par **année / mois**, trié par station et date | Conseil du cours (S2 p.30) ; le Gold cherche par période (partition pruning). À réévaluer au passage à l'échelle (voir plus bas). |
| D17 | Suivi du Silver dans son rapport de qualité (PUBLIE / ECHEC), sans modifier le batch log du Bronze | Le code de l'ingestion reste intact. |
| D18 | Test des règles sur des données fabriquées, dans un dossier temporaire | Les vraies données sont propres ; le test prouve les règles sans toucher aux vraies données (11/11). |

## Gold

| # | Décision | Justification |
|---|---|---|
| D19 | **Définition des épisodes en 3 temps** : A (seuils fixes) pour le test → B (seuils propres à chaque station, percentiles sur une période de référence ~30 ans) au passage à l'échelle → C (méthode Météo-France pour la canicule : moyennes sur 3 jours de tn et tx, comparées à des seuils départementaux) en bonus, pour comparer | A : simple, vérifiable avant la S4. B : plus réaliste (un même seuil n'a pas le même sens à Brest et à Marseille), mais exige un long historique (nos 3 années de test sont chaudes et biaiseraient les percentiles). C : référence officielle, sert de validation de la détection ; demande une table de seuils départementaux à trouver et vérifier, et ne concerne que la canicule. Paramètres dans `config.yaml`. |
| D20 | **Seuils (méthode A)** : canicule = tx ≥ 35 °C au moins 3 jours de suite ; vague de froid = tn ≤ −5 °C au moins 3 jours de suite ; fortes pluies = rr ≥ 50 mm, 1 jour suffit (jours consécutifs regroupés) | 35 °C = très forte chaleur en France ; 3 jours distinguent un épisode d'un jour isolé ; les pluies extrêmes tiennent souvent en un jour (orages, épisodes cévenols). |
| D21 | **Tolérance d'un jour manquant** (ABSENT ou INVALIDE) au sein d'un épisode — **provisoire, peut évoluer** : 1 jour manquant ne coupe pas l'épisode, 2 jours manquants de suite le coupent ; il faut au moins 3 jours (chaleur/froid) dépassant réellement le seuil ; le jour manquant ne compte pas ; un épisode commence et finit sur un jour qui dépasse ; le nombre de jours manquants est indiqué | Plus réaliste : un trou de mesure ne doit pas faire disparaître une vraie canicule. Transparence via la colonne `nb_jours_manquants`. Prouvé par le test (S3, S4, S9). |
| D22 | **Valeurs SUSPECT gardées avec un signalement** (`contient_suspect`) ; INVALIDE et ABSENT jamais utilisés comme valeurs | Une pluie extrême est précisément ce que l'on cherche ; l'exclure risquerait d'effacer un vrai record. |
| D23 | **Records de l'API (`/v2/records`) ingérés par la chaîne Bronze**, modification faite sur la branche `ingestion-batch` puis fusionnée (`git merge`) dans `silver-batch` et `gold-batch` | Référence de validation rangée comme les autres sources (zone brute, checksum, batch log) ; chaque modification dans la branche de son sujet, historique lisible. Test de non-régression : 30 batchs de mesures ignorés, Bronze reconstruit (42 batchs), 10 950 mesures inchangées. |
| D24 | **Validation du Gold** : cohérence (nos extrêmes ≤ records historiques depuis 1941) et concordance (même valeur le jour du record s'il est dans nos années) | Preuve de « correction » demandée par le jury. Limite assumée : même source (v2), donc contrôle de la chaîne et non référence indépendante. Résultat : cohérence 120/120, concordance 17 + 7 OK, 0 KO. |
| D25 | **Gold recalculé en entier** à chaque exécution | Simple et sûr sur un petit volume ; le calcul incrémental (late data, partitions touchées) sera traité pour la S4. |
| D26 | **Test de la détection en mémoire** (séries fabriquées, appel direct de `detecter()`) | Nos données n'ont aucun jour manquant : D21 n'aurait jamais été vérifiée sans ce test (10/10). |

## Points à réévaluer au passage à l'échelle
- **Partitionnement du Silver (D16)** : au périmètre complet (≈ 3 000 stations, estimation), un dossier par mois ferait environ 3 à 10 Mo, sous la cible du cours (128–512 Mo, risque de « small files »). Un dossier par année ferait environ 40 à 120 Mo. **À mesurer** (taille des fichiers, temps de lecture du Gold) puis comparer année/mois et année seule, à résultat identique. Prévoir de rendre le choix configurable dans `config.yaml`. Formulation pour le dossier : « Partitionnement initial année/mois (conseil du cours). Au passage à l'échelle, fichiers mensuels de X Mo, sous la cible de 128–512 Mo ; passage à un partitionnement annuel : fichiers de Y Mo, temps de lecture du Gold passé de A à B s, résultat identique. »
- **Définition des épisodes (D19)** : passer de la méthode A à la méthode B (télécharger une période de référence, idéalement 1991–2020), puis comparer à la méthode C (Météo-France) pour la canicule.
- **Fortes pluies** : 99 épisodes en test, nombre élevé dans certaines stations (Brest 15, Grenoble 18) ; à réexaminer avec la méthode B.
- **Épisodes tronqués** : années non contiguës (2003, 2019, 2022) ; disparaîtra avec des années continues.

## Questions ouvertes (envoyées au professeur)
- Accès au S3 de l'équipe et au cluster ; versions de Spark et de Python sur le cluster.
- Périmètre complet attendu et délai pour produire le Gold.
- Résultat de référence et extrait brut (late data, corrections).
- Bornes horaires de la « journée » de `/v2/journaliere` (0 h–24 h ou 6 h–6 h UTC) ; la date `to` est exclue, contrairement à la documentation.
