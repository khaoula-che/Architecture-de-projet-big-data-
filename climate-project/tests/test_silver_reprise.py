"""
test_silver_reprise.py - Verifie la reprise apres panne du Silver (S4, partie B).

Dans un dossier temporaire (les vraies donnees ne sont PAS touchees) :
  1. preparation : lundi (batch 1 traite), puis livraison du mardi (batch 2)
  2. reference : traitement du mardi SANS panne (on mesure sa duree)
  3. 5 pannes : le traitement du mardi est tue brutalement (SIGKILL, comme une
     coupure de courant) a 40 %, 55 %, 70 %, 80 % et 90 % de sa duree,
     puis relance normalement.
Chaque relance doit donner EXACTEMENT le meme resultat que la reference
(Silver, quarantaine et rapport de qualite).

Usage (depuis la racine du depot) :
  python3 climate-project/tests/test_silver_reprise.py
"""
import csv
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

ICI = Path(__file__).resolve().parent
sys.path.insert(0, str(ICI))
from test_silver_incremental import (B1, B2, T1, T2, SCHEMA_REF, ecrire_bronze,  # noqa: E402
                                     ecrire_log, lignes_batch1, lignes_batch2)

PROJET = ICI.parent
MOMENTS = (0.40, 0.55, 0.70, 0.80, 0.90)


def commande(racine):
    return [sys.executable, str(racine / "silver" / "build_silver.py")]


def lancer(racine, afficher=False):
    r = subprocess.run(commande(racine), capture_output=True, text=True)
    if afficher or "Bilan" not in r.stdout:
        print(r.stdout.strip())
    if "Bilan" not in r.stdout:
        print(r.stderr[-3000:])


def lancer_et_couper(racine, delai):
    """Lance build_silver.py puis le tue brutalement (Python ET Java) apres `delai` s."""
    p = subprocess.Popen(commande(racine), stdout=subprocess.DEVNULL,
                         stderr=subprocess.DEVNULL, start_new_session=True)
    time.sleep(delai)
    try:
        os.killpg(p.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass                      # deja termine : la "panne" arrive trop tard
    p.wait()


def apres_panne(data):
    with open(data / "_meta" / "rapport_qualite_silver.csv", encoding="utf-8") as f:
        statuts = {l["batch_id"]: l["statut"] for l in csv.DictReader(f)}
    return "trop tard (B2 deja PUBLIE)" if statuts.get(B2) == "PUBLIE" else "B2 pas encore PUBLIE"


def restes(data):
    """Des dossiers d'ecriture Spark interrompue trainent-ils ?"""
    return any(p.name.startswith((".spark-staging", "_temporary"))
               for p in (data / "silver").rglob("*"))


def etat(spark, data):
    silver = sorted(tuple(r) for r in spark.read.parquet(str(data / "silver" / "observations_jour"))
                    .select("station_id", F.col("date").cast("string"), "tx", "qc_tx",
                            "_batch_id", "_ligne").collect())
    quar = sorted(tuple(r) for r in spark.read.parquet(str(data / "silver" / "quarantaine"))
                  .select("_batch_id", "date", "motif").collect())
    with open(data / "_meta" / "rapport_qualite_silver.csv", encoding="utf-8") as f:
        rapport = sorted((l["batch_id"], l["statut"], l["lignes_silver"], l["doublons"],
                          l["quarantaine"]) for l in csv.DictReader(f))
    return silver, quar, rapport


def verifier(nom, condition):
    try:
        ok = bool(condition())
    except Exception:
        ok = False
    print(f"{'REUSSI' if ok else 'ECHEC '}  {nom}")
    return ok


def main():
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        modele = tmp / "modele" / "climate-project"
        for d in ("ingestion_batch", "silver"):
            shutil.copytree(PROJET / d, modele / d, ignore=shutil.ignore_patterns("__pycache__"))
        data = modele / "data"
        spark = SparkSession.builder.master("local[*]").appName("test_reprise").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        print("--- Preparation : lundi traite, puis livraison du mardi ---")
        ref = [("uid-07486", "07486", "Grenoble-St Geoirs", "38", "45.36", "5.33", "384",
                "ref_stations_test")]
        (spark.createDataFrame(ref, SCHEMA_REF).write.partitionBy("_batch_id")
              .parquet(str(data / "bronze" / "ref_stations")))
        ecrire_bronze(spark, data, B1, lignes_batch1(), T1, "overwrite")
        ecrire_log(data, [(B1, len(lignes_batch1()))])
        lancer(modele)
        ecrire_bronze(spark, data, B2, lignes_batch2(), T2, "append")
        ecrire_log(data, [(B1, len(lignes_batch1())), (B2, len(lignes_batch2()))])

        print("\n--- Reference : traitement du mardi SANS panne ---")
        racine = tmp / "reference" / "climate-project"
        shutil.copytree(modele, racine)
        t0 = time.time()
        lancer(racine, afficher=True)
        duree = time.time() - t0
        reference = etat(spark, racine / "data")
        print(f"Duree du traitement : {duree:.1f} s")

        print("\n--- 5 pannes, puis relance ---")
        pannes = []
        for i, m in enumerate(MOMENTS, 1):
            racine = tmp / f"panne{i}" / "climate-project"
            shutil.copytree(modele, racine)
            lancer_et_couper(racine, duree * m)
            avant = apres_panne(racine / "data")
            traces = "oui" if restes(racine / "data") else "non"
            lancer(racine)
            identique = etat(spark, racine / "data") == reference
            print(f"Panne {i} a {duree * m:4.1f} s : {avant:<27} restes d'ecriture : {traces:<3}"
                  f" -> apres relance : {'IDENTIQUE' if identique else 'DIFFERENT'}")
            pannes.append((avant, identique))
        spark.stop()

    print("\n--- Verification ---")
    tests = [("1  Reference : les 2 batchs sont PUBLIE",
              lambda: len(reference[2]) == 2 and all(r[1] == "PUBLIE" for r in reference[2]))]
    for i, (_, identique) in enumerate(pannes, 1):
        tests.append((f"{i + 1}  Panne {i} : apres relance, resultat identique a la reference",
                      lambda ok=identique: ok))
    tests.append(("7  Au moins une panne a eu lieu en plein traitement",
                  lambda: any(a.startswith("B2 pas encore") for a, _ in pannes)))
    resultats = [verifier(nom, cond) for nom, cond in tests]
    print(f"\n{sum(resultats)}/{len(resultats)} tests reussis")
    sys.exit(0 if all(resultats) else 1)


if __name__ == "__main__":
    main()