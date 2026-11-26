"""
mesurer_gold.py - Mesure du Gold avec 1, 2, 4 puis 8 coeurs (protocole S3 du cours).

Protocole (PDF S3, « Protocole de mesure ») :
  1. Figer    : meme Silver, meme code (commit), meme configuration Spark
  2. Varier   : une seule chose, le nombre de coeurs (local[1], local[2], local[4], local[8])
  3. Repeter  : 3 executions par reglage, mediane et ecart rapportes
  4. Isoler   : fermer les autres applications lourdes pendant la mesure
  5. Compter  : tout le traitement, du lancement a l'ecriture du Gold
  6. Archiver : resultats bruts + synthese en CSV, journaux Spark conserves

Pour chaque execution : duree (horloge), shuffle lu et ecrit, spill, et le resultat
du Gold (nombre d'episodes, nombre de KO) pour verifier qu'il ne change pas.

Usage (depuis la racine du depot) :
  python3 climate-project/mesures/mesurer_gold.py              # 1, 2, 4, 8 coeurs x 3
  python3 climate-project/mesures/mesurer_gold.py --coeurs 1 4 --repetitions 2
"""
import argparse
import csv
import json
import os
import re
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ICI = Path(__file__).resolve().parent
PROJET = ICI.parent
GOLD = PROJET / "gold" / "build_gold.py"
EVENTLOGS = PROJET / "data" / "_meta" / "eventlogs"      # journaux Spark (non versionnes)
BRUT = ICI / "resultats" / "mesures_gold_brut.csv"         # versionne avec le code
SYNTHESE = ICI / "resultats" / "mesures_gold_synthese.csv"


def commit():
    try:
        return subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True,
                              text=True, cwd=PROJET).stdout.strip()
    except Exception:
        return ""


def lignes_silver():
    """Nombre de lignes lues (pour le debit), compte une seule fois."""
    code = ("from pyspark.sql import SparkSession;"
            "s=SparkSession.builder.master('local[*]').getOrCreate();"
            "s.sparkContext.setLogLevel('ERROR');"
            f"print('N=',s.read.parquet(r'{PROJET / 'data' / 'silver' / 'observations_jour'}').count())")
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True).stdout
    m = re.search(r"N= (\d+)", out)
    return int(m.group(1)) if m else 0


def lire_eventlog(chemin):
    """Additionne les metriques de toutes les taches d'un journal d'execution Spark."""
    m = {"shuffle_lu_octets": 0, "shuffle_ecrit_octets": 0, "spill_memoire_octets": 0,
         "spill_disque_octets": 0, "nb_taches": 0, "nb_stages": 0}
    debuts, fins = [], []
    # Spark 4 range le journal dans un dossier "eventlog_v2_<id>" (fichiers events_*)
    chemin = Path(chemin)
    fichiers = sorted(chemin.glob("events_*")) if chemin.is_dir() else [chemin]
    for fichier in fichiers:
        for ligne in open(fichier, encoding="utf-8"):
            ev = json.loads(ligne)
            nom = ev.get("Event")
            if nom == "SparkListenerJobStart":
                debuts.append(ev["Submission Time"])
            elif nom == "SparkListenerJobEnd":
                fins.append(ev["Completion Time"])
            elif nom == "SparkListenerStageCompleted":
                m["nb_stages"] += 1
            elif nom == "SparkListenerTaskEnd":
                t = ev.get("Task Metrics") or {}
                lu = t.get("Shuffle Read Metrics") or {}
                ecrit = t.get("Shuffle Write Metrics") or {}
                m["shuffle_lu_octets"] += lu.get("Remote Bytes Read", 0) + lu.get("Local Bytes Read", 0)
                m["shuffle_ecrit_octets"] += ecrit.get("Shuffle Bytes Written", 0)
                m["spill_memoire_octets"] += t.get("Memory Bytes Spilled", 0)
                m["spill_disque_octets"] += t.get("Disk Bytes Spilled", 0)
                m["nb_taches"] += 1
    # duree des calculs Spark seuls (du 1er job au dernier), sans le demarrage de Java
    m["duree_jobs_s"] = round((max(fins) - min(debuts)) / 1000, 2) if debuts and fins else ""
    return m


def une_execution(coeurs, rep):
    EVENTLOGS.mkdir(parents=True, exist_ok=True)
    avant = set(EVENTLOGS.iterdir())
    env = dict(os.environ, SPARK_MASTER=f"local[{coeurs}]", SPARK_EVENTLOG_DIR=str(EVENTLOGS))
    t0 = time.perf_counter()
    r = subprocess.run([sys.executable, str(GOLD)], capture_output=True, text=True, env=env)
    duree = time.perf_counter() - t0
    nouveaux = [p for p in EVENTLOGS.iterdir() if p not in avant and not p.name.endswith(".inprogress")]
    metriques = lire_eventlog(nouveaux[0]) if nouveaux else {}
    # Resultat du Gold, pour verifier qu'il est identique a chaque execution
    resultat = " ".join(f"{t}={n}" for t, n in re.findall(r"^\s+(\w+)\s+(\d+) episode", r.stdout, re.M))
    ko = re.search(r"\((\d+) KO\)", r.stdout)
    return {"date": datetime.now(timezone.utc).isoformat(timespec="seconds"), "commit": commit(),
            "coeurs": coeurs, "repetition": rep, "duree_s": round(duree, 2),
            "code_retour": r.returncode, "resultat": resultat, "ko": ko.group(1) if ko else "?",
            "eventlog": nouveaux[0].name if nouveaux else "", **metriques}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--coeurs", type=int, nargs="+", default=[1, 2, 4, 8])
    p.add_argument("--repetitions", type=int, default=3)
    a = p.parse_args()

    n_lignes = lignes_silver()
    print(f"Silver lu : {n_lignes} lignes - commit {commit()}\n")
    print("Execution de chauffe (non comptee)...")
    une_execution(a.coeurs[0], 0)

    brut = []
    for c in a.coeurs:
        for rep in range(1, a.repetitions + 1):
            m = une_execution(c, rep)
            brut.append(m)
            print(f"  {c} coeur(s), essai {rep} : {m['duree_s']:6.2f} s  "
                  f"(calcul {m.get('duree_jobs_s', '?')} s)  "
                  f"shuffle={m.get('shuffle_lu_octets', 0) / 1e6:6.2f} Mo  "
                  f"spill={m.get('spill_disque_octets', 0)}  [{m['resultat']}, KO={m['ko']}]")

    BRUT.parent.mkdir(parents=True, exist_ok=True)
    with open(BRUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(brut[0].keys()))
        w.writeheader()
        w.writerows(brut)

    # Synthese : mediane, ecart, debit, speedup, efficacite, cout
    synth = []
    for c in a.coeurs:
        d = [m["duree_s"] for m in brut if m["coeurs"] == c]
        synth.append({"coeurs": c, "mediane_s": round(statistics.median(d), 2),
                      "ecart_s": round(max(d) - min(d), 2),
                      "mediane_jobs_s": round(statistics.median(
                          [m["duree_jobs_s"] for m in brut if m["coeurs"] == c and m.get("duree_jobs_s") != ""] or [0]), 2),
                      "debit_lignes_s": round(n_lignes / statistics.median(d)) if n_lignes else "",
                      "shuffle_lu_mo": round(statistics.median(
                          [m.get("shuffle_lu_octets", 0) for m in brut if m["coeurs"] == c]) / 1e6, 2),
                      "spill_disque_octets": max(m.get("spill_disque_octets", 0)
                                                 for m in brut if m["coeurs"] == c),
                      "vcpu_s": round(c * statistics.median(d), 1)})
    t1 = synth[0]["mediane_s"]
    for s in synth:
        s["speedup"] = round(t1 / s["mediane_s"], 2)
        s["efficacite_pct"] = round(100 * s["speedup"] / s["coeurs"])
    with open(SYNTHESE, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(synth[0].keys()))
        w.writeheader()
        w.writerows(synth)

    print("\n=== Synthese (mediane de", a.repetitions, "essais) ===")
    print(f"{'coeurs':>6} {'mediane':>8} {'ecart':>6} {'calcul':>7} {'debit/s':>9} {'shuffle':>8} "
          f"{'spill':>6} {'vCPU.s':>7} {'speedup':>8} {'efficacite':>10}")
    for s in synth:
        print(f"{s['coeurs']:>6} {s['mediane_s']:>7}s {s['ecart_s']:>5}s {s['mediane_jobs_s']:>6}s {s['debit_lignes_s']:>9} "
              f"{s['shuffle_lu_mo']:>6}Mo {s['spill_disque_octets']:>6} {s['vcpu_s']:>7} "
              f"{s['speedup']:>7}x {s['efficacite_pct']:>9}%")
    resultats = {m["resultat"] + " KO=" + m["ko"] for m in brut}
    print(f"\nResultat du Gold identique a chaque execution : {'OUI' if len(resultats) == 1 else 'NON'}"
          f" ({' | '.join(sorted(resultats))})")
    print(f"Fichiers : {BRUT.relative_to(PROJET.parent)} et {SYNTHESE.relative_to(PROJET.parent)}")


if __name__ == "__main__":
    main()
