"""
load_bronze.py - Ingestion par batchs, partie 2 : zone brute -> table Bronze (PySpark).

Lit les fichiers JSON des batchs notes RECU dans le batch log, les "deplie"
(une ligne par jour de mesure, une ligne par station), garde TOUS les champs
en texte, ajoute les colonnes techniques, ecrit en Parquet partitionne par
_batch_id, verifie les comptes, puis passe les batchs a CHARGE.

Usage :
  python3 climate-project/ingestion_batch/load_bronze.py          # batchs RECU seulement
  python3 climate-project/ingestion_batch/load_bronze.py --tout   # recharge tout depuis la zone brute
"""
import sys

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import ArrayType, MapType, StructType

from extract_api import CONFIG, PROJET, ecrire_log, lire_log, maintenant

BRONZE = PROJET / CONFIG["chemins"]["bronze"]
SOURCE = CONFIG["source"]


def creer_spark():
    """Demarre Spark sur cette machine (tous les coeurs)."""
    spark = (SparkSession.builder
             .appName("load_bronze")
             .master("local[*]")
             # Ne remplacer QUE les dossiers _batch_id ecrits (S2 p.17)
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def lire_brut(spark, batchs):
    """Lit les fichiers JSON des batchs, en gardant toutes les valeurs en texte."""
    chemins = [str(PROJET / b["fichiers"] / "page_*.json") for b in batchs]
    return (spark.read
            .option("multiLine", True)           # un fichier = un document JSON
            .option("primitivesAsString", True)  # nombres et booleens lus comme TEXTE
            .json(chemins)
            .withColumn("_fichier", F.col("_metadata.file_path"))
            .withColumn("_batch_id",
                        F.regexp_extract("_fichier", r"batch_id(?:=|%3D)([^/]+)/", 1)))


def mesures_journalieres(brut):
    """Une ligne par jour. _ligne = position du jour dans le fichier (1, 2, 3...)."""
    lignes = brut.select("_fichier", "_batch_id",
                         F.posexplode("mesures").alias("_pos", "m"))
    return lignes.select("m.*", "_fichier", "_batch_id",
                         (F.col("_pos") + 1).alias("_ligne"))


def stations(brut):
    """Une ligne par station. Les parties imbriquees restent en texte JSON."""
    lignes = brut.select("_fichier", "_batch_id",
                         F.posexplode("features").alias("_pos", "f"))
    champs = lignes.schema["f"].dataType["properties"].dataType.fields
    colonnes = []
    for c in champs:
        col = F.col(f"f.properties.{c.name}")
        if isinstance(c.dataType, (StructType, ArrayType, MapType)):
            col = F.to_json(col)                 # ex. parametres_mesures, acces
        colonnes.append(col.alias(c.name))
    return lignes.select(*colonnes,
                         F.to_json("f.geometry").alias("geometry"),
                         "_fichier", "_batch_id",
                         (F.col("_pos") + 1).alias("_ligne"))


def ajouter_colonnes_techniques(df, endpoint, checksums):
    """Ajoute les etiquettes de provenance (S2 p.13)."""
    return (df.join(F.broadcast(checksums), "_batch_id", "left")
              .withColumn("_source", F.lit(SOURCE))
              .withColumn("_endpoint", F.lit(endpoint))
              .withColumn("_ingere_le", F.current_timestamp()))


def main():
    tout = "--tout" in sys.argv
    statuts = ("RECU", "CHARGE") if tout else ("RECU",)
    log = lire_log()
    a_charger = [b for b in log.values() if b["statut"] in statuts]
    if not a_charger:
        print("Rien a charger : aucun batch au statut RECU.")
        return

    spark = creer_spark()
    bilan = {"CHARGE": 0, "ECHEC": 0}

    for endpoint, deplier in (("v2_journaliere", mesures_journalieres),
                              ("ref_stations", stations),
                              ("v2_records", mesures_journalieres)):  # meme format : une liste "mesures"
        batchs = [b for b in a_charger if b["endpoint"] == endpoint]
        if not batchs:
            continue
        cible = str(BRONZE / endpoint)

        # 1. Lire, deplier, etiqueter
        checksums = spark.createDataFrame(
            [(b["batch_id"], b["checksum"]) for b in batchs], ["_batch_id", "_checksum"])
        table = ajouter_colonnes_techniques(deplier(lire_brut(spark, batchs)), endpoint, checksums)

        # 2. Ecrire en Parquet, un dossier par batch (remplace, n'ajoute pas)
        table.write.mode("overwrite").partitionBy("_batch_id").parquet(cible)

        # 3. Verifier : relire ce qui a ete ecrit et compter par batch
        ids = [b["batch_id"] for b in batchs]
        ecrit = {r["_batch_id"]: r["count"] for r in
                 spark.read.parquet(cible).where(F.col("_batch_id").isin(ids))
                      .groupBy("_batch_id").count().collect()}

        # 4. Mettre a jour le cahier : CHARGE si les comptes sont justes
        for b in batchs:
            attendu, obtenu = int(b["nb_lignes"]), ecrit.get(b["batch_id"], 0)
            if obtenu == attendu:
                b["statut"], b["message"] = "CHARGE", ""
                bilan["CHARGE"] += 1
                print(f"CHARGE  {b['batch_id']}  ({obtenu} lignes)")
            else:
                b["statut"] = "ECHEC"
                b["message"] = f"bronze: {obtenu} lignes ecrites, {attendu} attendues"
                bilan["ECHEC"] += 1
                print(f"ECHEC   {b['batch_id']}  ({b['message']})")
            b["maj_le"] = maintenant()
        ecrire_log(log)

    spark.stop()
    print(f"\nBilan : {bilan['CHARGE']} charge(s), {bilan['ECHEC']} echec(s)")
    sys.exit(1 if bilan["ECHEC"] else 0)


if __name__ == "__main__":
    main()