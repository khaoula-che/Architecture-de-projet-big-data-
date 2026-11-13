"""
extract_api.py - Ingestion par batchs, partie 1 : zone brute (raw).

Interroge l'API climato, enregistre chaque reponse TELLE QUELLE dans data/raw/,
calcule son empreinte SHA-256 et note chaque batch dans le batch log.
Relancer le script ne retelecharge pas un batch deja recu et intact.
"""
import csv
import hashlib
import json
import os
import shutil
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timezone
from pathlib import Path

import yaml

# ---------- Configuration ----------
PROJET = Path(__file__).resolve().parent.parent          # dossier climate-project/
CONFIG = yaml.safe_load(open(PROJET / "ingestion_batch" / "config.yaml", encoding="utf-8"))
API = CONFIG["api"]
RAW = PROJET / CONFIG["chemins"]["raw"]
LOG = PROJET / CONFIG["chemins"]["batch_log"]
COLONNES_LOG = ["batch_id", "endpoint", "station", "debut", "fin", "fichiers",
                "checksum", "nb_lignes", "statut", "message", "maj_le"]


def maintenant():
    """Date et heure actuelles en UTC."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------- Le cahier des batchs (batch log) ----------
def lire_log():
    """Charge le cahier : un dictionnaire {batch_id: ligne}."""
    if not LOG.exists():
        return {}
    with open(LOG, newline="", encoding="utf-8") as f:
        return {ligne["batch_id"]: ligne for ligne in csv.DictReader(f)}


def ecrire_log(log):
    """Reecrit le cahier en entier, via un fichier temporaire (jamais a moitie ecrit)."""
    LOG.parent.mkdir(parents=True, exist_ok=True)
    tmp = LOG.with_suffix(".tmp")
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLONNES_LOG)
        w.writeheader()
        for ligne in sorted(log.values(), key=lambda x: x["batch_id"]):
            w.writerow(ligne)
    os.replace(tmp, LOG)


# ---------- Appel a l'API ----------
def appeler(chemin, params):
    """Pose une question a l'API et renvoie la reponse brute (octets), sans la transformer."""
    url = API["base_url"] + chemin
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "climate-project-ingestion/0.1"})
    for essai in range(1, API["tentatives"] + 1):
        try:
            with urllib.request.urlopen(req, timeout=API["timeout_s"]) as rep:
                return rep.read()
        except urllib.error.HTTPError as e:
            if e.code == 503 and essai < API["tentatives"]:
                ra = e.headers.get("Retry-After")
                time.sleep(int(ra) if ra and ra.isdigit() else API["pause_s"])
                continue
            raise
        except urllib.error.URLError:
            if essai < API["tentatives"]:
                time.sleep(API["pause_s"])
                continue
            raise


def telecharger_batch(chemin, params, pagine):
    """Renvoie la liste des pages brutes. Suit la pagination (tronque / suite.from)."""
    pages, params = [], dict(params)
    while True:
        brut = appeler(chemin, params)
        pages.append(brut)
        if not pagine:
            break
        rep = json.loads(brut)
        if not rep.get("tronque"):
            break
        params["from"] = rep["suite"]["from"]
    return pages


def empreinte(pages):
    """Empreinte SHA-256 de l'ensemble des pages d'un batch."""
    h = hashlib.sha256()
    for p in pages:
        h.update(p)
    return h.hexdigest()


def compter_lignes(endpoint, pages):
    """Nombre de lignes de donnees recues (stations ou mesures)."""
    cle = "features" if endpoint == "ref_stations" else "mesures"
    return sum(len(json.loads(p)[cle]) for p in pages)


def dossier_batch(endpoint, batch_id):
    return RAW / endpoint / f"batch_id={batch_id}"


def deja_recu(log, batch_id, endpoint):
    """Vrai si le batch est note RECU ou CHARGE ET que ses fichiers bruts sont intacts."""
    ligne = log.get(batch_id)
    if not ligne or ligne["statut"] not in ("RECU", "CHARGE"):
        return False
    fichiers = sorted(dossier_batch(endpoint, batch_id).glob("page_*.json"))
    if not fichiers:
        return False
    return empreinte([f.read_bytes() for f in fichiers]) == ligne["checksum"]


# ---------- Traitement d'un batch ----------
def traiter(log, batch_id, endpoint, chemin, params, station="", debut="", fin="", pagine=True):
    if deja_recu(log, batch_id, endpoint):
        print(f"IGNORE  {batch_id}  (deja recu, empreinte identique)")
        return "IGNORE"
    try:
        pages = telecharger_batch(chemin, params, pagine)
        nb = compter_lignes(endpoint, pages)
        cs = empreinte(pages)
        # Ecrire a cote (.tmp), puis publier d'un seul coup
        d = dossier_batch(endpoint, batch_id)
        tmp = d.with_name(d.name + ".tmp")
        if tmp.exists():
            shutil.rmtree(tmp)
        tmp.mkdir(parents=True)
        for i, p in enumerate(pages, 1):
            (tmp / f"page_{i:03d}.json").write_bytes(p)
        if d.exists():
            shutil.rmtree(d)
        os.replace(tmp, d)
        # Noter dans le cahier SEULEMENT apres l'ecriture reussie
        log[batch_id] = {"batch_id": batch_id, "endpoint": endpoint, "station": station,
                         "debut": debut, "fin": fin, "fichiers": str(d.relative_to(PROJET)),
                         "checksum": cs, "nb_lignes": nb, "statut": "RECU",
                         "message": "", "maj_le": maintenant()}
        ecrire_log(log)
        print(f"RECU    {batch_id}  ({nb} lignes, {len(pages)} page(s))")
        return "RECU"
    except Exception as e:
        log[batch_id] = {**dict.fromkeys(COLONNES_LOG, ""), "batch_id": batch_id,
                         "endpoint": endpoint, "station": station, "debut": debut, "fin": fin,
                         "statut": "ECHEC", "message": str(e)[:200], "maj_le": maintenant()}
        ecrire_log(log)
        print(f"ECHEC   {batch_id}  ({e})")
        return "ECHEC"


# ---------- Programme principal ----------
def main():
    log = lire_log()
    bilan = {"RECU": 0, "IGNORE": 0, "ECHEC": 0}

    # 1. Referentiel des stations : un batch par jour d'extraction
    jour = date.today().isoformat()
    r = traiter(log, f"ref_stations_{jour}", "ref_stations", "/ref/stations", {}, pagine=False)
    bilan[r] += 1

    # 2. Mesures journalieres : un batch par station et par periode
    for p in CONFIG["periodes"]:
        for s in CONFIG["stations"]:
            batch_id = f"v2j_{s}_{p['debut']}_{p['fin']}"
            params = {"station": s, "from": p["debut"], "to": p["fin"]}
            r = traiter(log, batch_id, "v2_journaliere", "/v2/journaliere", params,
                        station=s, debut=p["debut"], fin=p["fin"])
            bilan[r] += 1
            if r == "RECU":
                time.sleep(0.5)        # rester raisonnable avec l'API
    
    # 3. Records de l'API (/v2/records) : un batch par station et par jour d'extraction.
    #    Reference de validation pour le Gold (records mensuels sur toute l'histoire).
    for s in CONFIG["stations"]:
        batch_id = f"v2r_{s}_{jour}"
        r = traiter(log, batch_id, "v2_records", "/v2/records", {"station": s},
                    station=s, pagine=False)
        bilan[r] += 1
        if r == "RECU":
            time.sleep(0.5)

    print(f"\nBilan : {bilan['RECU']} recu(s), {bilan['IGNORE']} ignore(s), {bilan['ECHEC']} echec(s)")
    sys.exit(1 if bilan["ECHEC"] else 0)


if __name__ == "__main__":
    main()