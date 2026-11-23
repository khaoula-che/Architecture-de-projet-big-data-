"""
test_gold_episodes.py - Verifie la detection des episodes du Gold.

Fabrique un petit Silver EN MEMOIRE (series de jours piegees, une fausse station
par serie), appelle directement detecter() de build_gold.py, puis verifie chaque
regle : duree minimale, tolerance d'un jour manquant (D21), coupure, bords,
froid, pluie (1 jour, plusieurs jours, cumul), valeur SUSPECT (D22).
Aucun fichier n'est ecrit : les vraies donnees ne sont pas touchees.

Usage (depuis la racine du depot) :
  python3 climate-project/tests/test_gold_episodes.py
"""
import sys
from datetime import date, timedelta
from pathlib import Path

from pyspark.sql import SparkSession

PROJET = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJET / "gold"))
from build_gold import EP, TYPES, detecter  # noqa: E402

DEBUT = date(2003, 7, 1)
NORMAL = {"tx": 25.0, "tn": 15.0, "rr": 0.0}
SCHEMA = ("station_id string, nom_station string, departement string, latitude double, "
          "longitude double, date date, tx double, qc_tx string, tn double, qc_tn string, "
          "rr double, qc_rr string")


def jour(i):
    return DEBUT + timedelta(days=i)


def serie(station, mesure, valeurs, qc_speciaux=None):
    """Une fausse station : 'mesure' prend les valeurs donnees (None = jour manquant)."""
    lignes = []
    for i, v in enumerate(valeurs):
        val, qc = dict(NORMAL), {m: "OK" for m in NORMAL}
        val[mesure] = None if v is None else float(v)
        if v is None:
            qc[mesure] = "ABSENT"
        elif qc_speciaux and v in qc_speciaux:
            qc[mesure] = qc_speciaux[v]
        lignes.append((station, f"Test {station}", "00", 45.0, 5.0, jour(i),
                       val["tx"], qc["tx"], val["tn"], qc["tn"], val["rr"], qc["rr"]))
    return lignes


SERIES = (
    serie("S1", "tx", [25, 36, 36, 36, 36, 36, 25])           # 5 jours chauds
    + serie("S2", "tx", [25, 36, 36, 25])                     # 2 jours seulement
    + serie("S3", "tx", [25, 36, 36, None, 37, 36, 25])       # 1 manquant au milieu
    + serie("S4", "tx", [25, 36, None, None, 36, 37, 36, 25]) # 2 manquants -> coupure
    + serie("S9", "tx", [25, None, 36, 36, 36, None, 25])     # manquants au bord
    + serie("S5", "tn", [10, -6, -7, -6, 10])                 # 3 nuits froides
    + serie("S6", "rr", [0, 60, 0])                           # pluie 1 jour
    + serie("S7", "rr", [0, 60, 70, 0])                       # pluie 2 jours
    + serie("S8", "rr", [0, 600, 0], {600: "SUSPECT"})        # pluie suspecte
)


def verifier(nom, condition):
    try:
        ok = bool(condition())
    except Exception:
        ok = False
    print(f"{'REUSSI' if ok else 'ECHEC '}  {nom}")
    return ok


def main():
    spark = SparkSession.builder.master("local[*]").appName("test_gold").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    silver = spark.createDataFrame(SERIES, SCHEMA)
    tol = EP["jours_manquants_toleres"]
    res = []
    for t in TYPES:
        res += detecter(silver, t, EP[t], tol).collect()
    spark.stop()

    def ep(station, type_ep):
        return [r for r in res if r["station_id"] == station and r["type"] == type_ep]

    def un(station, type_ep):
        e = ep(station, type_ep)
        assert len(e) == 1
        return e[0]

    print("\n--- Verification de la detection des episodes ---")
    tests = [
        ("S1 5 jours a 36 °C -> 1 canicule de 5 jours",
         lambda: un("S1", "canicule")["duree_jours"] == 5),
        ("S2 2 jours a 36 °C -> aucune canicule",
         lambda: len(ep("S2", "canicule")) == 0),
        ("S3 🔥🔥❓🔥🔥 -> 1 canicule de 5 jours, 4 au-dessus du seuil, 1 manquant",
         lambda: (un("S3", "canicule")["duree_jours"], un("S3", "canicule")["nb_jours_seuil"],
                  un("S3", "canicule")["nb_jours_manquants"],
                  un("S3", "canicule")["intensite"]) == (5, 4, 1, 37.0)),
        ("S4 🔥❓❓🔥🔥🔥 -> coupure : 1 canicule de 3 jours (du 05/07 au 07/07)",
         lambda: (un("S4", "canicule")["date_debut"], un("S4", "canicule")["date_fin"])
                 == (jour(4), jour(6))),
        ("S9 ❓🔥🔥🔥❓ -> les jours manquants au bord ne sont pas inclus",
         lambda: (un("S9", "canicule")["date_debut"], un("S9", "canicule")["date_fin"],
                  un("S9", "canicule")["nb_jours_manquants"]) == (jour(2), jour(4), 0)),
        ("S5 3 nuits a -6/-7/-6 °C -> 1 vague de froid, intensite -7",
         lambda: (un("S5", "vague_froid")["duree_jours"],
                  un("S5", "vague_froid")["intensite"]) == (3, -7.0)),
        ("S6 1 jour a 60 mm -> 1 episode de fortes pluies",
         lambda: (un("S6", "fortes_pluies")["duree_jours"],
                  un("S6", "fortes_pluies")["cumul_rr"]) == (1, 60.0)),
        ("S7 60 puis 70 mm -> 1 episode de 2 jours, cumul 130 mm",
         lambda: (un("S7", "fortes_pluies")["duree_jours"], un("S7", "fortes_pluies")["cumul_rr"],
                  un("S7", "fortes_pluies")["intensite"]) == (2, 130.0, 70.0)),
        ("S8 600 mm (SUSPECT) -> 1 episode signale contient_suspect",
         lambda: un("S8", "fortes_pluies")["contient_suspect"] is True),
        ("Total : 4 canicules, 1 vague de froid, 3 episodes de pluie (rien de parasite)",
         lambda: (len([r for r in res if r["type"] == "canicule"]),
                  len([r for r in res if r["type"] == "vague_froid"]),
                  len([r for r in res if r["type"] == "fortes_pluies"])) == (4, 1, 3)),
    ]
    resultats = [verifier(nom, cond) for nom, cond in tests]
    print(f"\n{sum(resultats)}/{len(resultats)} tests reussis")
    sys.exit(0 if all(resultats) else 1)


if __name__ == "__main__":
    main()