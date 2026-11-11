"""
test_silver_qualite.py - Verifie que les regles de qualite du Silver fonctionnent.

Fabrique un petit Bronze de TEST (10 lignes piegees) dans un dossier temporaire,
lance build_silver.py dessus (copie isolee : les vraies donnees ne sont PAS touchees),
puis verifie chaque regle : Q1, Q2, Q3, Q6, absence, conversion, pluie suspecte,
dedoublonnage et reconciliation.

Usage (depuis la racine du depot) :
  python3 climate-project/tests/test_silver_qualite.py
"""
import csv
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession

PROJET = Path(__file__).resolve().parent.parent          # dossier climate-project/
BATCH = "v2j_07486_2003-08-01_2003-09-01"                # periode du batch : aout 2003
REF_BATCH = "ref_stations_test"

# Les 10 lignes piegees : (ic_id, date, tx, tn, rr)
LIGNES = [
    ("07486", "2003-08-12", "38.6", "17.4", "0.0"),    # 1  aucun piege
    ("07486", "2003-08-13", "61.0", "18.0", "0.0"),    # 2  Q1 : tx impossible
    ("07486", "2003-08-14", "30.0", "35.0", "1.0"),    # 3  Q2 : tn > tx
    ("07486", "2003-08-15", "33.0", "15.0", None),     # 4  pluie absente
    ("07486", "2003-08-16", "mq",   "16.0", "2.0"),    # 5  tx non convertible
    ("07486", "2003-08-17", "34.0", "16.0", "650.0"),  # 6  pluie suspecte
    ("07486", "2003-08-17", "34.0", "16.0", "650.0"),  # 7  doublon de la ligne 6
    ("99999", "2003-08-18", "35.0", "17.0", "0.0"),    # 8  Q6 : station inconnue
    ("07486", "2003-13-45", "35.0", "17.0", "0.0"),    # 9  Q3 : date illisible
    ("07486", "2003-07-15", "35.0", "17.0", "0.0"),    # 10 Q3 : hors periode du batch
]

SCHEMA_BRONZE = ("ic_id string, mfid string, date string, tx string, tn string, tm string, "
                 "tntxm string, rr string, n_obs string, n_obs_rejetees string, "
                 "statut_extremes string, _fichier string, _ligne int, _checksum string, "
                 "_source string, _endpoint string, _ingere_le timestamp, _batch_id string")
SCHEMA_REF = ("station_uid string, ic_id string, libelle string, departement string, "
              "latitude string, longitude string, altitude string, _batch_id string")
COLONNES_LOG = ["batch_id", "endpoint", "station", "debut", "fin", "fichiers",
                "checksum", "nb_lignes", "statut", "message", "maj_le"]


def spark_local():
    s = SparkSession.builder.master("local[*]").appName("test_silver").getOrCreate()
    s.sparkContext.setLogLevel("ERROR")
    return s


def preparer(racine):
    """Copie les programmes et fabrique un faux Bronze + faux referentiel + batch log."""
    for d in ("ingestion_batch", "silver"):
        shutil.copytree(PROJET / d, racine / d, ignore=shutil.ignore_patterns("__pycache__"))
    data = racine / "data"
    spark = spark_local()
    maintenant = datetime.now(timezone.utc)
    lignes = [(s, "38384001", d, tx, tn, None, None, rr, "48", "0", "quotidien",
               "test", i, "test", "climato", "v2_journaliere", maintenant, BATCH)
              for i, (s, d, tx, tn, rr) in enumerate(LIGNES, 1)]
    (spark.createDataFrame(lignes, SCHEMA_BRONZE)
          .write.partitionBy("_batch_id").parquet(str(data / "bronze" / "v2_journaliere")))
    ref = [("uid-07486", "07486", "Grenoble-St Geoirs", "38", "45.36", "5.33", "384", REF_BATCH)]
    (spark.createDataFrame(ref, SCHEMA_REF)
          .write.partitionBy("_batch_id").parquet(str(data / "bronze" / "ref_stations")))
    spark.stop()
    (data / "_meta").mkdir(parents=True, exist_ok=True)
    with open(data / "_meta" / "batch_log.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLONNES_LOG)
        w.writeheader()
        w.writerow({"batch_id": BATCH, "endpoint": "v2_journaliere", "station": "07486",
                    "debut": "2003-08-01", "fin": "2003-09-01", "fichiers": "test",
                    "checksum": "test", "nb_lignes": len(LIGNES), "statut": "CHARGE",
                    "message": "", "maj_le": ""})


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
        preparer(racine)

        # Lancer le VRAI build_silver.py, mais sur la copie de test
        res = subprocess.run([sys.executable, str(racine / "silver" / "build_silver.py")],
                             capture_output=True, text=True)
        print("--- Sortie de build_silver.py ---")
        print(res.stdout.strip())
        if "Bilan" not in res.stdout:
            print(res.stderr[-3000:])

        spark = spark_local()
        data = racine / "data"
        silver = spark.read.parquet(str(data / "silver" / "observations_jour")).collect()
        par_date = {r["date"].isoformat(): r for r in silver}
        quar = {r["_ligne"]: r["motif"] for r in
                spark.read.parquet(str(data / "silver" / "quarantaine")).collect()}
        with open(data / "_meta" / "rapport_qualite_silver.csv", encoding="utf-8") as f:
            rapport = next(csv.DictReader(f))
        spark.stop()

        print("\n--- Verification des regles ---")
        d = par_date.get
        tests = [
            ("1  ligne normale -> tout OK",
             lambda: d("2003-08-12")["tx"] == 38.6 and d("2003-08-12")["qc_tx"] == "OK"),
            ("2  Q1 tx=61 -> qc_tx INVALIDE, tn reste OK",
             lambda: d("2003-08-13")["qc_tx"] == "INVALIDE" and d("2003-08-13")["qc_tn"] == "OK"),
            ("3  Q2 tn>tx -> qc_tx et qc_tn INVALIDE",
             lambda: d("2003-08-14")["qc_tx"] == "INVALIDE" and d("2003-08-14")["qc_tn"] == "INVALIDE"),
            ("4  pluie vide -> rr vide (pas 0) et ABSENT",
             lambda: d("2003-08-15")["rr"] is None and d("2003-08-15")["qc_rr"] == "ABSENT"),
            ("5  tx='mq' -> tx vide et INVALIDE",
             lambda: d("2003-08-16")["tx"] is None and d("2003-08-16")["qc_tx"] == "INVALIDE"),
            ("6  pluie 650 mm -> SUSPECT",
             lambda: d("2003-08-17")["qc_rr"] == "SUSPECT"),
            ("7  doublon du 17/08 -> une seule ligne en Silver",
             lambda: sum(1 for r in silver if r["date"].isoformat() == "2003-08-17") == 1),
            ("8  station 99999 -> quarantaine Q6",
             lambda: "Q6" in quar[8]),
            ("9  date 2003-13-45 -> quarantaine Q3 (illisible)",
             lambda: "illisible" in quar[9]),
            ("10 date 15/07 hors periode -> quarantaine Q3",
             lambda: "hors periode" in quar[10]),
            ("Bilan : 10 = 6 Silver + 1 doublon + 3 quarantaine, PUBLIE",
             lambda: (rapport["lignes_bronze"], rapport["lignes_silver"], rapport["doublons"],
                      rapport["quarantaine"], rapport["statut"]) == ("10", "6", "1", "3", "PUBLIE")),
        ]
        resultats = [verifier(nom, cond) for nom, cond in tests]

    print(f"\n{sum(resultats)}/{len(resultats)} tests reussis")
    sys.exit(0 if all(resultats) else 1)


if __name__ == "__main__":
    main()