"""
test_silver_incremental.py - Verifie les donnees en retard et les corrections (S4).

Dans un dossier temporaire (les vraies donnees ne sont PAS touchees) :
  1. batch 1 (lundi) : Grenoble du 01/07 au 28/08/2003 + 2 lignes en quarantaine
     -> build_silver.py
  2. batch 2 (mardi, en retard) : corrige le 12/08 et le 13/08, ajoute 29-31/08
     -> build_silver.py (incremental : seul aout doit etre recalcule)
  3. build_silver.py --tout (recalcul complet)
Puis verifie : correction appliquee, jours en retard arrives, aucun doublon,
juillet non reecrit, quarantaine intacte, bilans PUBLIE, ancienne valeur gardee
dans le Bronze, et incremental = recalcul complet.

Usage (depuis la racine du depot) :
  python3 climate-project/tests/test_silver_incremental.py
"""
import csv
import shutil
import subprocess
import sys
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

PROJET = Path(__file__).resolve().parent.parent
B1 = "v2j_07486_2003-07-01_2003-09-01"   # livraison du lundi
B2 = "v2j_07486_2003-08-12_2003-09-01"   # livraison du mardi (en retard)
T1 = datetime(2026, 11, 23, 8, 0, tzinfo=timezone.utc)
T2 = T1 + timedelta(days=1)

SCHEMA_BRONZE = ("ic_id string, mfid string, date string, tx string, tn string, tm string, "
                 "tntxm string, rr string, n_obs string, n_obs_rejetees string, "
                 "statut_extremes string, _fichier string, _ligne int, _checksum string, "
                 "_source string, _endpoint string, _ingere_le timestamp, _batch_id string")
SCHEMA_REF = ("station_uid string, ic_id string, libelle string, departement string, "
              "latitude string, longitude string, altitude string, _batch_id string")
COLONNES_LOG = ["batch_id", "endpoint", "station", "debut", "fin", "fichiers",
                "checksum", "nb_lignes", "statut", "message", "maj_le"]


def jours(debut, fin):
    """Tous les jours de debut (inclus) a fin (exclue)."""
    d = debut
    while d < fin:
        yield d
        d += timedelta(days=1)


def lignes_batch1():
    lignes = []
    for d in jours(date(2003, 7, 1), date(2003, 8, 29)):
        tx = {"2003-08-12": "38.6", "2003-08-13": "38.8"}.get(d.isoformat(), "30.0")
        lignes.append(("07486", d.isoformat(), tx))
    lignes += [("99999", "2003-07-10", "31.0"),    # quarantaine (juillet)
               ("99999", "2003-08-05", "32.0")]    # quarantaine (aout)
    return lignes


def lignes_batch2():
    return [("07486", "2003-08-12", "39.0"),       # correction
            ("07486", "2003-08-13", "40.0"),       # correction
            ("07486", "2003-08-29", "31.0"),       # en retard
            ("07486", "2003-08-30", "32.0"),
            ("07486", "2003-08-31", "33.0")]


def ecrire_bronze(spark, data, batch, lignes, t, mode):
    rows = [(s, "38384001", d, tx, "15.0", None, None, "0.0", "48", "0", "quotidien",
             "test", i, "test", "climato", "v2_journaliere", t, batch)
            for i, (s, d, tx) in enumerate(lignes, 1)]
    (spark.createDataFrame(rows, SCHEMA_BRONZE)
          .write.mode(mode).partitionBy("_batch_id")
          .parquet(str(data / "bronze" / "v2_journaliere")))


def ecrire_log(data, batchs):
    (data / "_meta").mkdir(parents=True, exist_ok=True)
    with open(data / "_meta" / "batch_log.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLONNES_LOG)
        w.writeheader()
        for b, n in batchs:
            _, s, debut, fin = b.split("_")
            w.writerow({"batch_id": b, "endpoint": "v2_journaliere", "station": s,
                        "debut": debut, "fin": fin, "fichiers": "test", "checksum": "test",
                        "nb_lignes": n, "statut": "CHARGE", "message": "", "maj_le": ""})


def lancer(racine, *args):
    r = subprocess.run([sys.executable, str(racine / "silver" / "build_silver.py"), *args],
                       capture_output=True, text=True)
    print(r.stdout.strip())
    if "Bilan" not in r.stdout:
        print(r.stderr[-3000:])


def photo(spark, data):
    df = spark.read.parquet(str(data / "silver" / "observations_jour"))
    return sorted(tuple(r) for r in df.select(
        "station_id", F.col("date").cast("string"), "tx", "qc_tx", "tn", "rr",
        "_batch_id", "_ligne").collect())


def fichiers_juillet(data):
    d = data / "silver" / "observations_jour" / "annee=2003" / "mois=7"
    return sorted(p.name for p in d.glob("*.parquet"))


def lire_rapport(data):
    with open(data / "_meta" / "rapport_qualite_silver.csv", encoding="utf-8") as f:
        return {l["batch_id"]: l for l in csv.DictReader(f)}


def verifier(nom, condition):
    try:
        ok = bool(condition())
    except Exception:
        ok = False
    print(f"{'REUSSI' if ok else 'ECHEC '}  {nom}")
    return ok


def main():
    with tempfile.TemporaryDirectory() as tmp:
        racine = Path(tmp) / "climate-project"
        for d in ("ingestion_batch", "silver"):
            shutil.copytree(PROJET / d, racine / d,
                            ignore=shutil.ignore_patterns("__pycache__"))
        data = racine / "data"
        spark = SparkSession.builder.master("local[*]").appName("test_incr").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        ref = [("uid-07486", "07486", "Grenoble-St Geoirs", "38", "45.36", "5.33", "384",
                "ref_stations_test")]
        (spark.createDataFrame(ref, SCHEMA_REF).write.partitionBy("_batch_id")
              .parquet(str(data / "bronze" / "ref_stations")))

        print("--- Lundi : batch 1 ---")
        ecrire_bronze(spark, data, B1, lignes_batch1(), T1, "overwrite")
        ecrire_log(data, [(B1, len(lignes_batch1()))])
        lancer(racine)
        juillet_avant = fichiers_juillet(data)

        print("\n--- Mardi : batch 2 en retard (incremental) ---")
        ecrire_bronze(spark, data, B2, lignes_batch2(), T2, "append")
        ecrire_log(data, [(B1, len(lignes_batch1())), (B2, len(lignes_batch2()))])
        lancer(racine)
        incr = photo(spark, data)
        juillet_apres = fichiers_juillet(data)
        rapport = lire_rapport(data)
        quar_b1 = {r["date"] for r in spark.read.parquet(str(data / "silver" / "quarantaine"))
                   .where(F.col("_batch_id") == B1).collect()}
        ancien = (spark.read.parquet(str(data / "bronze" / "v2_journaliere"))
                  .where((F.col("_batch_id") == B1) & (F.col("date") == "2003-08-12"))
                  .first()["tx"])

        print("\n--- Recalcul complet (--tout) ---")
        lancer(racine, "--tout")
        tout = photo(spark, data)
        spark.stop()

    par_date = {r[1]: r for r in incr}
    print("\n--- Verification ---")
    tests = [
        ("1  Correction : le 12/08 vaut 39,0 (batch 2)",
         lambda: (par_date["2003-08-12"][2], par_date["2003-08-12"][6]) == (39.0, B2)),
        ("2  Correction : le 13/08 vaut 40,0 (batch 2)",
         lambda: par_date["2003-08-13"][2] == 40.0),
        ("3  Retard : les 29, 30 et 31/08 sont arrives",
         lambda: all(d in par_date for d in ("2003-08-29", "2003-08-30", "2003-08-31"))),
        ("4  Aucun doublon : 62 jours, une seule ligne chacun",
         lambda: len(incr) == 62 and len(par_date) == 62),
        ("5  Juillet n'a pas ete reecrit (seul aout est recalcule)",
         lambda: juillet_avant and juillet_avant == juillet_apres),
        ("6  Quarantaine du batch 1 intacte (10/07 et 05/08)",
         lambda: quar_b1 == {"2003-07-10", "2003-08-05"}),
        ("7  Bilan batch 1 : 61 = 57 Silver + 2 doublons + 2 quarantaine, PUBLIE",
         lambda: tuple(rapport[B1][k] for k in
                       ("lignes_bronze", "lignes_silver", "doublons", "quarantaine", "statut"))
                 == ("61", "57", "2", "2", "PUBLIE")),
        ("8  Bilan batch 2 : 5 = 5 Silver + 0 + 0, PUBLIE",
         lambda: tuple(rapport[B2][k] for k in
                       ("lignes_bronze", "lignes_silver", "doublons", "quarantaine", "statut"))
                 == ("5", "5", "0", "0", "PUBLIE")),
        ("9  Ancienne valeur gardee dans le Bronze (38,6 le 12/08, batch 1)",
         lambda: ancien == "38.6"),
        ("10 Incremental = recalcul complet (--tout)",
         lambda: incr == tout),
    ]
    resultats = [verifier(nom, cond) for nom, cond in tests]
    print(f"\n{sum(resultats)}/{len(resultats)} tests reussis")
    sys.exit(0 if all(resultats) else 1)


if __name__ == "__main__":
    main()