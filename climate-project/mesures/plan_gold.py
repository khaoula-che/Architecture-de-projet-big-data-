"""
plan_gold.py - Lit l'execution plan du Gold (S3 : « execution plan annote »).

Pour chaque produit du Gold (episodes, records, validation), demande a Spark son plan
(explain("formatted")) SANS rien ecrire, puis compte les operations qui coutent :
  Exchange           = un SHUFFLE (echange de donnees entre travailleurs)
  BroadcastExchange  = une petite table copiee sur chaque travailleur (pas de shuffle)
  Window / Sort      = calculs par fenetre, tris
  Scan parquet       = lectures de fichiers
Les plans complets sont archives dans mesures/resultats/plan_gold.txt.

Usage (depuis la racine du depot) :
  python3 climate-project/mesures/plan_gold.py
"""
import re
import sys
from collections import Counter
from pathlib import Path

ICI = Path(__file__).resolve().parent
sys.path.insert(0, str(ICI.parent / "gold"))
from build_gold import (EP, SILVER, TYPES, PUBLIES, creer_spark, detecter,  # noqa: E402
                        records, validation)

SORTIE = ICI / "resultats" / "plan_gold.txt"
SUIVIS = ["Exchange", "BroadcastExchange", "Window", "Sort", "SortMergeJoin",
          "BroadcastHashJoin", "HashAggregate", "Scan parquet", "InMemoryTableScan"]


def plan(df):
    """Texte du plan « formatted », sans executer le calcul."""
    return df._jdf.queryExecution().executedPlan().toString(), \
        df._sc._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), "formatted")


def compter(texte):
    """Compte les operations de la liste numerotee « (n) Nom ... » du plan formatted."""
    c = Counter()
    for nom in re.findall(r"^\(\d+\) (.+?)(?:\s\[|\s\(|$)", texte, re.M):
        nom = nom.strip()
        for s in SUIVIS:
            if nom == s or nom.startswith(s + " "):
                c[s] += 1
    return c


def main():
    spark = creer_spark()
    spark.sparkContext.setLogLevel("ERROR")
    silver = (spark.read.parquet(str(SILVER / "observations_jour"))
                   .select("station_id", "nom_station", "departement", "latitude", "longitude",
                           "date", "tx", "qc_tx", "tn", "qc_tn", "rr", "qc_rr")
                   .cache())
    tol = EP["jours_manquants_toleres"]
    episodes = None
    for t in PUBLIES:
        e = detecter(silver, t, EP[t], tol)
        episodes = e if episodes is None else episodes.unionByName(e)
    recs = records(silver)
    produits = {"episodes (catalogue officiel)": episodes,
                "canicule seule (1 type)": detecter(silver, "canicule", EP["canicule"], tol),
                "records_stations": recs,
                "validation_records": validation(spark, recs, silver)}

    conf = spark.conf
    reglages = {k: conf.get(k) for k in ("spark.master", "spark.sql.shuffle.partitions",
                                         "spark.sql.adaptive.enabled",
                                         "spark.sql.adaptive.coalescePartitions.enabled",
                                         "spark.sql.autoBroadcastJoinThreshold")}
    SORTIE.parent.mkdir(parents=True, exist_ok=True)
    with open(SORTIE, "w", encoding="utf-8") as f:
        f.write("Reglages Spark : " + str(reglages) + "\n\n")
        print("=== Reglages Spark ===")
        for k, v in reglages.items():
            print(f"  {k:<48} {v}")
        print("\n=== Operations par produit (lecture du plan, rien n'est calcule) ===")
        etiquettes = ["Exchange", "Broadcast", "Window", "Sort", "SortMergeJ", "BroadcastJ"]
        print(f"  {'produit':<32}" + "".join(f"{e:>12}" for e in etiquettes))
        for nom, df in produits.items():
            _, texte = plan(df)
            c = compter(texte)
            print(f"  {nom:<32}" + "".join(f"{c.get(s, 0):>12}" for s in SUIVIS[:6]))
            f.write("=" * 100 + f"\n{nom}\n" + "=" * 100 + "\n" + texte + "\n\n")
    spark.stop()
    print(f"\nTypes publies : {PUBLIES} (sur {TYPES})")
    print("(Exchange = shuffle ; Broadcast = petite table copiee, sans shuffle ;"
          " SortMergeJ / BroadcastJ = jointures)")
    print(f"Plans complets : {SORTIE.relative_to(ICI.parent.parent)}")


if __name__ == "__main__":
    main()
