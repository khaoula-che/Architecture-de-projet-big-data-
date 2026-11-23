"""
build_silver.py - Couche Silver : Bronze -> Silver (PySpark), format LARGE.

Une ligne par station et par jour : mesures converties en nombres, controles
de qualite (qc_tx, qc_tn, qc_rr), infos de la station (referentiel),
dedoublonnage, quarantaine des lignes inutilisables, rangement par annee/mois,
et bilan par batch : Bronze = Silver + doublons + quarantaine.

Traitement incremental (donnees en retard, corrections) :
  - le Silver est recalcule seulement pour les MOIS touches par les nouveaux batchs ;
  - la quarantaine et le bilan sont recalcules sur la TOTALITE des batchs concernes
    (nouveaux batchs + batchs deja presents dans ces mois).

Usage (depuis la racine du depot) :
  python3 climate-project/silver/build_silver.py          # nouveaux batchs seulement
  python3 climate-project/silver/build_silver.py --tout   # reconstruit tout le Silver
"""
import csv
import os
import shutil
import sys
from pathlib import Path

from pyspark.sql import SparkSession, Window, functions as F

# Reutiliser la config et les outils de l'ingestion (sans les modifier)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ingestion_batch"))
from extract_api import CONFIG, PROJET, lire_log, maintenant  # noqa: E402

BRONZE = PROJET / CONFIG["chemins"]["bronze"]
SILVER = PROJET / CONFIG["chemins"]["silver"]
RAPPORT = PROJET / CONFIG["chemins"]["rapport_silver"]
T_MIN = CONFIG["qualite"]["temp_min"]
T_MAX = CONFIG["qualite"]["temp_max"]
PLUIE_SUSPECTE = CONFIG["qualite"]["pluie_suspecte"]

MESURES_QC = ["tx", "tn", "rr"]
ETATS_QC = ["ok", "suspect", "invalide", "absent"]
COLONNES_RAPPORT = (["batch_id", "lignes_bronze", "lignes_silver", "doublons", "quarantaine"]
                    + [f"{m}_{e}" for m in MESURES_QC for e in ETATS_QC]
                    + ["statut", "maj_le"])


# ---------- Rapport de qualite (= suivi du Silver, un batch par ligne) ----------
def lire_rapport():
    if not RAPPORT.exists():
        return {}
    with open(RAPPORT, newline="", encoding="utf-8") as f:
        return {l["batch_id"]: l for l in csv.DictReader(f)}


def ecrire_rapport(rapport):
    RAPPORT.parent.mkdir(parents=True, exist_ok=True)
    tmp = RAPPORT.with_suffix(".tmp")
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLONNES_RAPPORT, extrasaction="ignore")
        w.writeheader()
        for l in sorted(rapport.values(), key=lambda x: x["batch_id"]):
            w.writerow(l)
    os.replace(tmp, RAPPORT)


# ---------- Spark ----------
def creer_spark():
    spark = (SparkSession.builder
             .appName("build_silver")
             .master("local[*]")
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
             .config("spark.sql.session.timeZone", "UTC")
             # Un texte non convertible (ex. "mq") donne NULL au lieu d'arreter le programme
             .config("spark.sql.ansi.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# ---------- Etape A : quels batchs traiter ----------
def batchs_a_traiter(tout):
    charges = {b["batch_id"] for b in lire_log().values()
               if b["endpoint"] == "v2_journaliere" and b["statut"] == "CHARGE"}
    if tout:
        return sorted(charges)
    publies = {b for b, l in lire_rapport().items() if l["statut"] == "PUBLIE"}
    return sorted(charges - publies)


# ---------- Etape B : le referentiel (derniere version, une ligne par station) ----------
def referentiel(spark):
    ref = spark.read.parquet(str(BRONZE / "ref_stations"))
    dernier = ref.agg(F.max("_batch_id")).first()[0]
    w = Window.partitionBy("ic_id").orderBy("station_uid")
    return (ref.where(F.col("_batch_id") == dernier)
               .withColumn("_r", F.row_number().over(w)).where("_r = 1")
               .select(F.col("ic_id").alias("station_id"),
                       F.col("libelle").alias("nom_station"),
                       "departement",
                       F.col("latitude").cast("double").alias("latitude"),
                       F.col("longitude").cast("double").alias("longitude"),
                       F.col("altitude").cast("double").alias("altitude")))


# ---------- Etapes B + D : preparer les lignes (date, periode, station, motif) ----------
def preparer(df, ref):
    """Ajoute la date, la periode du batch, les infos de la station et le motif
    de quarantaine (NULL si la ligne est utilisable)."""
    df = (df.withColumn("d", F.to_date("date", "yyyy-MM-dd"))
            # periode du batch, lue dans son nom v2j_<station>_<debut>_<fin> (pour Q3)
            .withColumn("p_debut", F.to_date(F.regexp_extract(
                "_batch_id", r"_(\d{4}-\d{2}-\d{2})_\d{4}-\d{2}-\d{2}$", 1)))
            .withColumn("p_fin", F.to_date(F.regexp_extract(
                "_batch_id", r"_(\d{4}-\d{2}-\d{2})$", 1))))
    # left join : on garde les stations inconnues pour pouvoir les detecter (Q6)
    df = df.join(F.broadcast(ref), df.ic_id == ref.station_id, "left")
    motif = (F.when(F.col("d").isNull(), "Q3 date illisible")
              .when((F.col("d") < F.col("p_debut")) | (F.col("d") >= F.col("p_fin")),
                    "Q3 date hors periode du batch")
              .when(F.col("station_id").isNull(), "Q6 station inconnue du referentiel"))
    return df.withColumn("motif", motif)


# ---------- Etape E : le controle de qualite d'une mesure ----------
def qc(m):
    """ABSENT / INVALIDE / SUSPECT / OK, dans cet ordre (premiere regle vraie)."""
    brut, val = F.col(m), F.col(m).cast("double")
    q = (F.when(brut.isNull() | (F.trim(brut) == ""), "ABSENT")   # absence != zero
          .when(val.isNull(), "INVALIDE"))                         # texte non convertible
    if m in ("tx", "tn"):
        q = q.when((val < T_MIN) | (val > T_MAX), "INVALIDE")      # Q1 temperature impossible
    else:
        q = (q.when(val < 0, "INVALIDE")                            # Q1 pluie negative
              .when(val > PLUIE_SUSPECTE, "SUSPECT"))               # pluie a verifier
    return q.otherwise("OK")


def compter(df):
    return {r["_batch_id"]: r["n"] for r in
            df.groupBy("_batch_id").agg(F.count("*").alias("n")).collect()}


def main():
    tout = "--tout" in sys.argv
    nouveaux = batchs_a_traiter(tout)
    if not nouveaux:
        print("Rien a publier : tous les batchs CHARGE sont deja dans le Silver.")
        return
    if tout:
        shutil.rmtree(SILVER, ignore_errors=True)   # reconstruction complete depuis Bronze

    spark = creer_spark()
    ref = referentiel(spark)
    bronze = spark.read.parquet(str(BRONZE / "v2_journaliere"))
    colonnes_bronze = bronze.columns
    jour = F.to_date("date", "yyyy-MM-dd")

    # Etape C : mois touches par les nouveaux batchs
    mois = (bronze.where(F.col("_batch_id").isin(nouveaux))
                  .select(F.year(jour).alias("a"), F.month(jour).alias("m"))
                  .where(F.col("a").isNotNull()).distinct())
    # Batchs concernes = les nouveaux + ceux qui ont des lignes dans ces mois
    dans_les_mois = bronze.join(F.broadcast(mois),
                                (F.year(jour) == mois.a) & (F.month(jour) == mois.m),
                                "left_semi")
    lot = sorted(set(nouveaux)
                 | {r[0] for r in dans_les_mois.select("_batch_id").distinct().collect()})

    # Toutes les lignes (tous les mois) des batchs concernes, preparees
    tous = preparer(bronze.where(F.col("_batch_id").isin(lot)), ref).cache()
    # Les lignes a recalculer dans le Silver : celles des mois touches
    a_recalculer = tous.join(F.broadcast(mois),
                             (F.year("d") == mois.a) & (F.month("d") == mois.m), "left_semi")

    # Etape D : quarantaine = probleme sur TOUTE la ligne, sur les batchs ENTIERS
    quarantaine = tous.where(F.col("motif").isNotNull())

    # Etape F : dedoublonnage, une ligne par (station, jour), la plus recente gagne
    w = Window.partitionBy("station_id", "d").orderBy(
        F.col("_ingere_le").desc(), F.col("_batch_id").desc(), F.col("_ligne").asc())
    gardees = (a_recalculer.where(F.col("motif").isNull())
                           .withColumn("_rang", F.row_number().over(w)).where("_rang = 1"))

    # Etape E : controles de qualite par mesure, puis Q2 (tn > tx -> les deux INVALIDE)
    silver = gardees
    for m in MESURES_QC:
        silver = silver.withColumn(f"qc_{m}", qc(m))
    silver = (silver
        .withColumn("_q2", (F.col("qc_tx") == "OK") & (F.col("qc_tn") == "OK")
                           & (F.col("tn").cast("double") > F.col("tx").cast("double")))
        .withColumn("qc_tx", F.when(F.col("_q2"), "INVALIDE").otherwise(F.col("qc_tx")))
        .withColumn("qc_tn", F.when(F.col("_q2"), "INVALIDE").otherwise(F.col("qc_tn"))))

    # Colonnes finales (format LARGE, types corrects)
    silver = silver.select(
        "station_id", F.col("d").alias("date"),
        F.col("tx").cast("double").alias("tx"), "qc_tx",
        F.col("tn").cast("double").alias("tn"), "qc_tn",
        F.col("tm").cast("double").alias("tm"),
        F.col("rr").cast("double").alias("rr"), "qc_rr",
        F.col("n_obs").cast("int").alias("n_obs"),
        F.col("n_obs_rejetees").cast("int").alias("n_obs_rejetees"),
        "mfid", "nom_station", "departement", "latitude", "longitude", "altitude",
        "_batch_id", "_ligne", "_source", F.current_timestamp().alias("_traite_le"),
        F.year("d").alias("annee"), F.month("d").alias("mois"))

    # Etape G : ecriture du Silver (seulement les mois touches sont remplaces)
    (silver.repartition("annee", "mois")
           .sortWithinPartitions("station_id", "date")
           .write.mode("overwrite").partitionBy("annee", "mois")
           .parquet(str(SILVER / "observations_jour")))
    # Quarantaine : on remplace ENTIEREMENT celle des batchs concernes
    for b in lot:
        shutil.rmtree(SILVER / "quarantaine" / f"_batch_id={b}", ignore_errors=True)
    (quarantaine.select(*colonnes_bronze, "motif", F.current_timestamp().alias("_traite_le"))
                .write.mode("overwrite").partitionBy("_batch_id")
                .parquet(str(SILVER / "quarantaine")))

    # Etape H : bilan par batch ENTIER, en suivant chaque ligne du Bronze
    #   silver  : c'est cette ligne exacte (batch + numero) qui est dans le Silver
    #   doublon : le Silver contient ce jour, mais venant d'une autre ligne
    #   perdue  : ce jour n'est pas dans le Silver (ne doit jamais arriver)
    silver_ecrit = (spark.read.option("mergeSchema", "true")
                         .parquet(str(SILVER / "observations_jour")))
    garde = silver_ecrit.select(F.col("station_id").alias("k_station"),
                                F.col("date").alias("k_date"),
                                F.col("_batch_id").alias("g_batch"),
                                F.col("_ligne").alias("g_ligne"))
    valides = tous.where(F.col("motif").isNull()).select(
        "_batch_id", "_ligne", F.col("station_id").alias("k_station"),
        F.col("d").alias("k_date"))
    devenir = (valides.join(garde, ["k_station", "k_date"], "left")
        .withColumn("devenir",
                    F.when(F.col("g_batch").isNull(), "perdue")
                     .when((F.col("g_batch") == F.col("_batch_id"))
                           & (F.col("g_ligne") == F.col("_ligne")), "silver")
                     .otherwise("doublon")))
    n_dev = {(r["_batch_id"], r["devenir"]): r["count"]
             for r in devenir.groupBy("_batch_id", "devenir").count().collect()}
    n_bronze, n_quar = compter(tous), compter(quarantaine)
    n_qc = {}
    ecrit_lot = silver_ecrit.where(F.col("_batch_id").isin(lot))
    for m in MESURES_QC:
        for r in ecrit_lot.groupBy("_batch_id", f"qc_{m}").count().collect():
            n_qc[(r["_batch_id"], m, r[f"qc_{m}"].lower())] = r["count"]

    rapport = {} if tout else lire_rapport()
    bilan = {"PUBLIE": 0, "ECHEC": 0}
    for b in lot:
        nb, nq = n_bronze.get(b, 0), n_quar.get(b, 0)
        ns, nd = n_dev.get((b, "silver"), 0), n_dev.get((b, "doublon"), 0)
        npe = n_dev.get((b, "perdue"), 0)
        statut = "PUBLIE" if (nb == ns + nd + nq and npe == 0) else "ECHEC"
        ligne = {"batch_id": b, "lignes_bronze": nb, "lignes_silver": ns,
                 "doublons": nd, "quarantaine": nq, "statut": statut, "maj_le": maintenant()}
        for m in MESURES_QC:
            for e in ETATS_QC:
                ligne[f"{m}_{e}"] = n_qc.get((b, m, e), 0)
        rapport[b] = ligne
        bilan[statut] += 1
        print(f"{statut:<7} {b}  bronze={nb} silver={ns} doublons={nd} "
              f"quarantaine={nq} perdues={npe}")
    ecrire_rapport(rapport)

    spark.stop()
    print(f"\nBilan : {bilan['PUBLIE']} publie(s), {bilan['ECHEC']} echec(s)")
    sys.exit(1 if bilan["ECHEC"] else 0)


if __name__ == "__main__":
    main()