"""
build_gold.py - Couche Gold : Silver -> produits finaux (PySpark).

Produit 3 tables (+ 1 table mise de cote) :
  gold/episodes            catalogue OFFICIEL des episodes extremes, une ligne par episode
                           (types dont le statut est "publie" : canicule, vague de froid)
  gold/episodes_experimental  episodes des types "experimental" (fortes pluies, D31 :
                           pluie de la source non fiable), meme schema, hors catalogue
  gold/records_stations    nos records par station et par mois du calendrier
  gold/validation_records  comparaison de nos records avec /v2/records de l'API
                           (coherence + concordance)

Regles (decisions D19 a D22, parametres dans config.yaml section "episodes") :
  - un jour "depasse" si sa mesure est OK ou SUSPECT et franchit le seuil ;
  - un jour "manquant" si sa mesure est ABSENT ou INVALIDE (ou s'il n'existe pas) ;
  - un jour "normal" coupe l'episode ; N jours manquants consecutifs (N <= tolerance)
    ne le coupent pas ; un episode commence et finit sur un jour qui depasse ;
  - un episode est garde s'il a au moins jours_min jours qui depassent.

Usage (depuis la racine du depot) :
  python3 climate-project/gold/build_gold.py
"""
import os
import shutil
import sys
from pathlib import Path

from pyspark.sql import SparkSession, Window, functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ingestion_batch"))
from extract_api import CONFIG, PROJET  # noqa: E402

BRONZE = PROJET / CONFIG["chemins"]["bronze"]
SILVER = PROJET / CONFIG["chemins"]["silver"]
GOLD = PROJET / CONFIG["chemins"]["gold"]
EP = CONFIG["episodes"]
TYPES = ["canicule", "vague_froid", "fortes_pluies"]
# D31 : seuls les types "publie" vont dans le catalogue officiel
PUBLIES = [t for t in TYPES if EP[t].get("statut", "publie") == "publie"]
EXPERIMENTAUX = [t for t in TYPES if t not in PUBLIES]
VALIDES = ["OK", "SUSPECT"]       # D22 : SUSPECT utilise mais signale
MANQUANTS = ["ABSENT", "INVALIDE"]
EPS = 0.05                        # tolerance pour comparer des nombres a virgule


def creer_spark():
    # Ou tourne Spark : local[*] par defaut (tous les coeurs du Mac). Reglable sans
    # toucher au code : SPARK_MASTER=local[2] pour les mesures, spark://... sur le cluster.
    b = (SparkSession.builder.appName("build_gold")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.sql.session.timeZone", "UTC")
         # Le Gold est recalcule en entier : on remplace toute la table
         .config("spark.sql.sources.partitionOverwriteMode", "static"))
    # Mesures (S3) : journal d'execution Spark (duree, shuffle, spill) si demande
    if os.environ.get("SPARK_EVENTLOG_DIR"):
        b = (b.config("spark.eventLog.enabled", "true")
              .config("spark.eventLog.dir", os.environ["SPARK_EVENTLOG_DIR"])
              .config("spark.eventLog.compress", "false"))
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# ---------- Produit 1 : catalogue des episodes ----------
def detecter(silver, type_ep, regle, tolerance):
    """Detecte les episodes d'un type, station par station ("gaps and islands")."""
    m, qc = regle["mesure"], f"qc_{regle['mesure']}"
    val = F.col(m)
    depasse = (val >= regle["seuil"]) if regle["sens"] == "haut" else (val <= regle["seuil"])

    # 1. Une marque par jour : H (depasse), M (manquant), N (normal)
    jours = silver.withColumn("marque",
        F.when(F.col(qc).isin(MANQUANTS), "M")
         .when(F.col(qc).isin(VALIDES) & depasse, "H")
         .otherwise("N"))

    # 2. Compteur cumule des jours normaux (pour savoir s'il y en a entre deux H)
    w = Window.partitionBy("station_id").orderBy("date")
    jours = jours.withColumn("cum_normaux",
                             F.sum((F.col("marque") == "N").cast("int")).over(w))

    # 3. On ne garde que les H et on regarde le H precedent
    h = (jours.where(F.col("marque") == "H")
         .withColumn("prev_date", F.lag("date").over(w))
         .withColumn("normaux_entre", F.col("cum_normaux") - F.lag("cum_normaux").over(w))
         # jours entre deux H qui ne sont pas normaux = manquants (ou absents du Silver)
         .withColumn("manquants_entre",
                     F.datediff("date", "prev_date") - 1 - F.col("normaux_entre"))
         # nouvel episode si : premier H, ou un jour normal entre, ou trop de manquants
         .withColumn("nouveau", (F.col("prev_date").isNull()
                                 | (F.col("normaux_entre") > 0)
                                 | (F.col("manquants_entre") > tolerance)).cast("int"))
         .withColumn("num_episode", F.sum("nouveau").over(w))
         .withColumn("manquants_inclus",
                     F.when(F.col("nouveau") == 0, F.col("manquants_entre")).otherwise(0)))

    # 4. Un episode = un groupe de H avec le meme numero
    intensite = F.max(m) if regle["sens"] == "haut" else F.min(m)
    ep = (h.groupBy("station_id", "nom_station", "departement", "latitude", "longitude",
                    "num_episode")
           .agg(F.min("date").alias("date_debut"), F.max("date").alias("date_fin"),
                F.count("*").alias("nb_jours_seuil"),
                F.sum("manquants_inclus").cast("int").alias("nb_jours_manquants"),
                intensite.alias("intensite"),
                F.round(F.avg(m), 1).alias("moyenne"),
                F.round(F.sum(m), 1).alias("cumul"),
                F.max((F.col(qc) == "SUSPECT").cast("int")).alias("_suspect"))
           .where(F.col("nb_jours_seuil") >= regle["jours_min"]))

    return ep.select(
        F.concat_ws("_", "station_id", F.lit(type_ep),
                    F.col("date_debut").cast("string")).alias("episode_id"),
        "station_id", "nom_station", "departement", "latitude", "longitude",
        F.lit(type_ep).alias("type"), "date_debut", "date_fin",
        (F.datediff("date_fin", "date_debut") + 1).alias("duree_jours"),
        "nb_jours_seuil", "nb_jours_manquants", "intensite", "moyenne",
        (F.col("cumul") if m == "rr" else F.lit(None).cast("double")).alias("cumul_rr"),
        (F.col("_suspect") == 1).alias("contient_suspect"),
        F.lit(EP["methode"]).alias("methode"),
        F.lit(float(regle["seuil"])).alias("seuil"),
        F.year("date_debut").alias("annee"))


# ---------- Produit 2 : nos records par station et mois du calendrier ----------
def records(silver):
    s = silver.withColumn("mois_calendaire", F.month("date"))
    cles = ["station_id", "mois_calendaire"]

    def extreme(m, nom, plus_grand):
        ordre = F.col(m).desc() if plus_grand else F.col(m).asc()
        w = Window.partitionBy(*cles).orderBy(ordre, F.col("date").asc())
        return (s.where(F.col(f"qc_{m}").isin(VALIDES))
                 .withColumn("_r", F.row_number().over(w)).where("_r = 1")
                 .select(*cles, F.col(m).alias(nom), F.col("date").alias(f"{nom}_date")))

    base = s.groupBy(*cles, "nom_station").agg(
        F.count("*").alias("n_jours"),
        F.concat_ws(",", F.array_sort(F.collect_set(F.year("date").cast("string"))))
         .alias("annees_couvertes"))
    return (base.join(extreme("tx", "tx_max", True), cles, "left")
                .join(extreme("tn", "tn_min", False), cles, "left")
                .join(extreme("rr", "rr_max", True), cles, "left"))


# ---------- Produit 3 : validation avec /v2/records ----------
def validation(spark, recs, silver):
    api = spark.read.parquet(str(BRONZE / "v2_records"))
    # derniere version des records pour chaque station
    api = (api.withColumn("_dernier", F.max("_batch_id").over(Window.partitionBy("ic_id")))
              .where(F.col("_batch_id") == F.col("_dernier"))
              .select(F.col("ic_id").alias("station_id"),
                      F.col("mois_calendaire").cast("int").alias("mois_calendaire"),
                      F.col("txx_max").cast("double").alias("api_tx_max"),
                      F.to_date("txx_max_date").alias("api_tx_max_date"),
                      F.col("tnn_min").cast("double").alias("api_tn_min"),
                      F.to_date("tnn_min_date").alias("api_tn_min_date")))
    # notre valeur le jour du record de l'API (si ce jour est dans nos donnees)
    tx_jour = silver.select("station_id", F.col("date").alias("api_tx_max_date"),
                            F.col("tx").alias("notre_tx_ce_jour"))
    tn_jour = silver.select("station_id", F.col("date").alias("api_tn_min_date"),
                            F.col("tn").alias("notre_tn_ce_jour"))
    v = (recs.select("station_id", "nom_station", "mois_calendaire", "annees_couvertes",
                     "tx_max", "tn_min")
             .join(api, ["station_id", "mois_calendaire"], "left")
             .join(tx_jour, ["station_id", "api_tx_max_date"], "left")
             .join(tn_jour, ["station_id", "api_tn_min_date"], "left"))

    def verdict(a, b, ok):
        return F.when(F.col(a).isNull() | F.col(b).isNull(), "NA").when(ok, "OK").otherwise("KO")

    return (v
        # coherence : nos extremes ne depassent jamais les records historiques
        .withColumn("coherence_tx", verdict("tx_max", "api_tx_max",
                    F.col("tx_max") <= F.col("api_tx_max") + EPS))
        .withColumn("coherence_tn", verdict("tn_min", "api_tn_min",
                    F.col("tn_min") >= F.col("api_tn_min") - EPS))
        # concordance : si le jour du record est dans nos donnees, meme valeur
        .withColumn("concordance_tx", verdict("notre_tx_ce_jour", "api_tx_max",
                    F.abs(F.col("notre_tx_ce_jour") - F.col("api_tx_max")) <= EPS))
        .withColumn("concordance_tn", verdict("notre_tn_ce_jour", "api_tn_min",
                    F.abs(F.col("notre_tn_ce_jour") - F.col("api_tn_min")) <= EPS)))


def main():
    spark = creer_spark()
    silver = (spark.read.parquet(str(SILVER / "observations_jour"))
                   .select("station_id", "nom_station", "departement", "latitude", "longitude",
                           "date", "tx", "qc_tx", "tn", "qc_tn", "rr", "qc_rr")
                   .cache())

    # Produit 1 : catalogue officiel (types "publie") + types mis de cote (D31)
    tol = EP["jours_manquants_toleres"]

    def ecrire_episodes(types, nom):
        if not types:                      # aucun type : on retire l'ancienne table
            shutil.rmtree(GOLD / nom, ignore_errors=True)
            return
        episodes = None
        for t in types:
            e = detecter(silver, t, EP[t], tol)
            episodes = e if episodes is None else episodes.unionByName(e)
        (episodes.repartition("annee").sortWithinPartitions("station_id", "date_debut")
                 .write.mode("overwrite").partitionBy("annee").parquet(str(GOLD / nom)))

    ecrire_episodes(PUBLIES, "episodes")
    ecrire_episodes(EXPERIMENTAUX, "episodes_experimental")

    # Produit 2
    recs = records(silver)
    recs.coalesce(1).write.mode("overwrite").parquet(str(GOLD / "records_stations"))

    # Produit 3
    recs = spark.read.parquet(str(GOLD / "records_stations"))
    validation(spark, recs, silver).coalesce(1).write.mode("overwrite") \
        .parquet(str(GOLD / "validation_records"))

    # ---------- Bilan ----------
    ep = spark.read.parquet(str(GOLD / "episodes"))
    print("=== Catalogue officiel des episodes ===")
    for r in ep.groupBy("type").count().orderBy("type").collect():
        print(f"  {r['type']:<14} {r['count']} episode(s)")
    if EXPERIMENTAUX:
        ex = spark.read.parquet(str(GOLD / "episodes_experimental"))
        print("\n=== Mis de cote (experimental, hors catalogue - D31) ===")
        for r in ex.groupBy("type").count().orderBy("type").collect():
            print(f"  {r['type']:<14} {r['count']} episode(s)")

    v = spark.read.parquet(str(GOLD / "validation_records"))
    print(f"\n=== Validation avec /v2/records ({v.count()} couples station x mois) ===")
    ko = 0
    for c in ["coherence_tx", "coherence_tn", "concordance_tx", "concordance_tn"]:
        n = {r[c]: r["count"] for r in v.groupBy(c).count().collect()}
        ko += n.get("KO", 0)
        print(f"  {c:<15} OK={n.get('OK', 0):<4} KO={n.get('KO', 0):<4} NA={n.get('NA', 0)}")

    spark.stop()
    print(f"\nBilan : {'VALIDE' if ko == 0 else 'A VERIFIER'} ({ko} KO)")
    sys.exit(1 if ko else 0)


if __name__ == "__main__":
    main()