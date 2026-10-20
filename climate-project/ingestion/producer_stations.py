import ast
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from confluent_kafka import Producer


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "data" / "source" / "station.csv"
TOPIC = "climate.stations.raw"


def parse_feature(value):
    """Lire une station encodée en JSON ou en dictionnaire Python."""
    try:
        feature = json.loads(value)
    except json.JSONDecodeError:
        feature = ast.literal_eval(value)

    if not isinstance(feature, dict):
        raise ValueError("La station doit être un dictionnaire.")

    if not isinstance(feature.get("properties"), dict):
        raise ValueError("La station ne contient pas de properties valides.")

    return feature


def main():
    if not SOURCE.is_file():
        raise FileNotFoundError(f"Fichier introuvable : {SOURCE}")

    producer = Producer({
        "bootstrap.servers": "127.0.0.1:9092",
        "enable.idempotence": True,
        "acks": "all",
    })

    run_id = str(uuid4())
    delivery_errors = []
    sent = 0
    delivered = 0

    def delivery_report(error, message):
        nonlocal delivered

        if error is not None:
            delivery_errors.append(str(error))
        else:
            delivered += 1

    try:
        with SOURCE.open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as source_file:

            # Détecter le séparateur uniquement depuis l'en-tête.
            header = source_file.readline()
            source_file.seek(0)

            if not header.strip():
                raise ValueError("Le fichier CSV est vide.")

            separator = max(
                (";", ",", "\t"),
                key=lambda candidate: header.count(candidate),
            )

            print("Séparateur CSV :", repr(separator))

            reader = csv.DictReader(
                source_file,
                delimiter=separator,
                quotechar='"',
                doublequote=True,
                strict=True,
            )

            if "features" not in (reader.fieldnames or []):
                raise ValueError(
                    "Colonne 'features' absente. "
                    f"Colonnes détectées : {reader.fieldnames}"
                )

            for record_number, row in enumerate(reader, start=1):
                # Une clé None indique des champs supplémentaires.
                if None in row:
                    raise ValueError(
                        f"Enregistrement {record_number} : trop de colonnes. "
                        "Vérifie le séparateur et les guillemets du CSV."
                    )

                if any(value is None for value in row.values()):
                    raise ValueError(
                        f"Enregistrement {record_number} : "
                        "certaines colonnes sont manquantes."
                    )

                raw_feature = row.get("features")

                if not raw_feature or not raw_feature.strip():
                    raise ValueError(
                        f"Enregistrement {record_number} : features vide."
                    )

                try:
                    feature = parse_feature(raw_feature)
                except (ValueError, SyntaxError) as error:
                    raise ValueError(
                        f"Enregistrement {record_number} : "
                        "features illisible.\n"
                        f"Début : {raw_feature[:200]!r}\n"
                        f"Fin : {raw_feature[-100:]!r}"
                    ) from error

                ic_id = feature["properties"].get("ic_id")

                if not isinstance(ic_id, str) or not ic_id.strip():
                    raise ValueError(
                        f"Enregistrement {record_number} : "
                        f"ic_id invalide : {ic_id!r}"
                    )

                # Conserver la ligne source sans nettoyage métier.
                event = {
                    "event_id": str(uuid4()),
                    "run_id": run_id,
                    "ingested_at": datetime.now(
                        timezone.utc
                    ).isoformat(),
                    "source_file": SOURCE.name,
                    "source_record": record_number,
                    "dataset": "stations",
                    "raw_data": row,
                }

                value = json.dumps(
                    event,
                    ensure_ascii=False,
                    allow_nan=False,
                ).encode("utf-8")

                while True:
                    if delivery_errors:
                        raise RuntimeError(
                            f"Erreur de livraison Kafka : "
                            f"{delivery_errors[0]}"
                        )

                    try:
                        producer.produce(
                            topic=TOPIC,
                            key=ic_id.encode("utf-8"),
                            value=value,
                            on_delivery=delivery_report,
                        )
                        break
                    except BufferError:
                        producer.poll(0.5)

                sent += 1
                producer.poll(0)

                if sent % 1000 == 0:
                    print(f"{sent} messages mis en file d'envoi...")

    finally:
        # Attendre les confirmations, même après une erreur de lecture.
        remaining = producer.flush(timeout=60)
        print(f"Messages mis en file : {sent}")
        print(f"Messages confirmés par Kafka : {delivered}")
        print(f"Messages encore en attente : {remaining}")

        if delivery_errors:
            print("Erreurs Kafka :", delivery_errors[:5])

    if remaining or delivery_errors:
        raise RuntimeError(
            "L'envoi est incomplet. Consulte les compteurs ci-dessus."
        )

    print(f"Ingestion vers Kafka terminée. Exécution : {run_id}")


if __name__ == "__main__":
    main()